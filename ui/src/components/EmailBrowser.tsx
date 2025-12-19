import { useState, useEffect } from 'react'
import { useParams, Link, useSearchParams } from 'react-router-dom'
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

export function EmailBrowser() {
  const { account, folder } = useParams<{ account?: string; folder?: string }>()
  const [searchParams, setSearchParams] = useSearchParams()
  const [folders, setFolders] = useState<FSFolder[]>([])
  const [emails, setEmails] = useState<FSEmail[]>([])
  const [total, setTotal] = useState(0)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)

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

    fetch(`/api/fs-emails/${encodeURIComponent(account)}/${encodeURIComponent(folder)}?limit=${limit}&offset=${offset}`)
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
        <Link to="/">Dashboard</Link>
        <span>/</span>
        <span>Browse</span>
        {account && (
          <>
            <span>/</span>
            <span>{account}</span>
          </>
        )}
        {folder && (
          <>
            <span>/</span>
            <span>{folder}</span>
          </>
        )}
      </nav>

      <header className="browser-header">
        <h1>Email Browser</h1>
        {account && folder && (
          <p className="folder-info">
            <code>{account}/{folder}</code>
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
                        to={`/browse/${encodeURIComponent(f.account)}/${encodeURIComponent(f.folder)}`}
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
                  {emails.map(email => (
                    <tr key={email.path}>
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
    </div>
  )
}
