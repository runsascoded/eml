import { useState, useEffect, useRef, useCallback } from 'react'
import { useParams, Link, useSearchParams, useNavigate } from 'react-router-dom'
import { useHotkeys, ShortcutsModal } from '@rdub/use-hotkeys'
import type { FSFolder, FSEmail } from '../types'
import './EmailBrowser.scss'

function formatSize(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`
  return `${(bytes / (1024 * 1024)).toFixed(1)} MB`
}

function formatDate(dateStr: string): string {
  if (!dateStr) return '-'
  try {
    const d = new Date(dateStr)
    return d.toLocaleDateString([], { year: 'numeric', month: 'short', day: 'numeric' })
  } catch {
    return dateStr.slice(0, 16)
  }
}

const HOTKEYS = {
  'j': 'nav:down',
  'k': 'nav:up',
  'enter': 'nav:open',
  'o': 'nav:open',
  '/': 'nav:search',
  'g i': 'nav:inbox',
  'n': 'nav:nextPage',
  'p': 'nav:prevPage',
}

const HOTKEY_DESCRIPTIONS = {
  'nav:down': 'Next email',
  'nav:up': 'Previous email',
  'nav:open': 'Open email',
  'nav:search': 'Focus search',
  'nav:inbox': 'Go to Inbox',
  'nav:nextPage': 'Next page',
  'nav:prevPage': 'Previous page',
}

export function EmailBrowser() {
  const { account, '*': folderPath } = useParams<{ account?: string; '*'?: string }>()
  const folder = folderPath || undefined  // Convert empty string to undefined
  const [searchParams, setSearchParams] = useSearchParams()
  const navigate = useNavigate()
  const [folders, setFolders] = useState<FSFolder[]>([])
  const [emails, setEmails] = useState<FSEmail[]>([])
  const [total, setTotal] = useState(0)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [selectedIndex, setSelectedIndex] = useState(0)
  const searchInputRef = useRef<HTMLInputElement>(null)
  const [searchQuery, setSearchQuery] = useState('')

  const page = parseInt(searchParams.get('page') || '1', 10)
  const limit = 50
  const offset = (page - 1) * limit

  // Fetch folders list
  useEffect(() => {
    fetch('/api/fs-folders')
      .then(res => res.json())
      .then(data => setFolders(data.folders || []))
      .catch(err => console.error('Failed to load folders:', err))
  }, [])

  // Fetch emails when account/folder changes
  useEffect(() => {
    if (!account || !folder) {
      setEmails([])
      setTotal(0)
      return
    }

    setLoading(true)
    setError(null)

    fetch(`/api/fs-emails/${encodeURIComponent(account)}/${folder}?limit=${limit}&offset=${offset}`)
      .then(res => res.json())
      .then(data => {
        if (data.error) {
          setError(data.error)
          setEmails([])
          setTotal(0)
        } else {
          setEmails(data.emails || [])
          setTotal(data.total || 0)
        }
        setLoading(false)
      })
      .catch(err => {
        setError(err.message)
        setLoading(false)
      })
  }, [account, folder, offset])

  const totalPages = Math.ceil(total / limit)

  const handlePageChange = (newPage: number) => {
    setSearchParams({ page: String(newPage) })
  }

  // Reset selection when emails change
  useEffect(() => {
    setSelectedIndex(0)
  }, [emails])

  // Hotkey handlers
  const openSelected = useCallback(() => {
    if (emails.length > 0 && selectedIndex < emails.length) {
      navigate(`/email/${emails[selectedIndex].path}`)
    }
  }, [emails, selectedIndex, navigate])

  useHotkeys(HOTKEYS, {
    'nav:down': () => setSelectedIndex(i => Math.min(i + 1, emails.length - 1)),
    'nav:up': () => setSelectedIndex(i => Math.max(i - 1, 0)),
    'nav:open': openSelected,
    'nav:search': () => searchInputRef.current?.focus(),
    'nav:inbox': () => navigate('/folder/y/Inbox'),
    'nav:nextPage': () => page < totalPages && handlePageChange(page + 1),
    'nav:prevPage': () => page > 1 && handlePageChange(page - 1),
  })

  const handleSearch = (e: React.FormEvent) => {
    e.preventDefault()
    if (searchQuery.trim()) {
      navigate(`/thread/search?q=${encodeURIComponent(searchQuery.trim())}`)
    }
  }

  // Group folders by account
  const foldersByAccount: Record<string, FSFolder[]> = {}
  for (const f of folders) {
    if (!foldersByAccount[f.account]) {
      foldersByAccount[f.account] = []
    }
    foldersByAccount[f.account].push(f)
  }

  return (
    <div className="email-browser">
      <nav className="breadcrumb">
        <Link to="/">Inbox</Link>
        {account && folder && account !== 'y' && (
          <>
            <span>/</span>
            <span>{account}</span>
          </>
        )}
        {folder && folder !== 'Inbox' && (
          <>
            <span>/</span>
            <span>{folder}</span>
          </>
        )}
        <Link to="/admin" className="admin-link">Admin</Link>
      </nav>

      <header className="browser-header">
        <form className="search-form" onSubmit={handleSearch}>
          <input
            ref={searchInputRef}
            type="text"
            placeholder="Search emails... (press /)"
            value={searchQuery}
            onChange={e => setSearchQuery(e.target.value)}
            className="search-input"
          />
          <button type="submit" className="search-button">Search</button>
        </form>
        {account && folder && (
          <p className="folder-info">
            <span className="folder-name">{folder}</span>
            {total > 0 && <span className="count">{total.toLocaleString()} emails</span>}
          </p>
        )}
      </header>

      <div className="browser-layout">
        <aside className="folder-sidebar">
          <h3>Folders</h3>
          {Object.entries(foldersByAccount).map(([acct, fldrs]) => (
            <div key={acct} className="folder-group">
              <h4>{acct}</h4>
              <ul>
                {fldrs.map(f => {
                  const isActive = f.account === account && f.folder === folder
                  return (
                    <li key={`${f.account}-${f.folder}`}>
                      <Link
                        to={`/folder/${encodeURIComponent(f.account)}/${f.folder}`}
                        className={isActive ? 'active' : ''}
                      >
                        {f.folder}
                        <span className="count">{f.eml_count.toLocaleString()}</span>
                      </Link>
                    </li>
                  )
                })}
              </ul>
            </div>
          ))}
        </aside>

        <main className="email-list">
          {!account || !folder ? (
            <div className="placeholder">
              <p>Select a folder from the sidebar to browse emails</p>
            </div>
          ) : loading ? (
            <div className="loading">Loading...</div>
          ) : error ? (
            <div className="error">{error}</div>
          ) : emails.length === 0 ? (
            <div className="empty">No emails in this folder</div>
          ) : (
            <>
              <table className="emails-table">
                <thead>
                  <tr>
                    <th>Subject</th>
                    <th>From</th>
                    <th>Date</th>
                    <th>Size</th>
                  </tr>
                </thead>
                <tbody>
                  {emails.map((email, index) => (
                    <tr
                      key={email.path}
                      className={index === selectedIndex ? 'selected' : ''}
                      onClick={() => setSelectedIndex(index)}
                      onDoubleClick={() => navigate(`/email/${email.path}`)}
                    >
                      <td className="subject">
                        <Link to={`/email/${email.path}`}>{email.subject || '(no subject)'}</Link>
                      </td>
                      <td className="from">{email.from}</td>
                      <td className="date">{formatDate(email.date)}</td>
                      <td className="size">{formatSize(email.size)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>

              {totalPages > 1 && (
                <div className="pagination">
                  <button
                    disabled={page <= 1}
                    onClick={() => handlePageChange(page - 1)}
                  >
                    Previous
                  </button>
                  <span className="page-info">
                    Page {page} of {totalPages}
                  </span>
                  <button
                    disabled={page >= totalPages}
                    onClick={() => handlePageChange(page + 1)}
                  >
                    Next
                  </button>
                </div>
              )}
            </>
          )}
        </main>
      </div>
      <ShortcutsModal keymap={HOTKEYS} descriptions={HOTKEY_DESCRIPTIONS} />
    </div>
  )
}
