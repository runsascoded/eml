import { useState, useCallback, type FormEvent } from 'react'
import { Link } from 'react-router-dom'
import type { SearchResult, SearchResponse } from '../types'
import './Search.scss'

export function Search() {
  const [query, setQuery] = useState('')
  const [results, setResults] = useState<SearchResult[]>([])
  const [count, setCount] = useState<number | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [loading, setLoading] = useState(false)

  const handleSearch = useCallback(async (e?: FormEvent) => {
    e?.preventDefault()
    if (!query.trim()) return

    setLoading(true)
    setError(null)
    try {
      const res = await fetch(`/api/search?q=${encodeURIComponent(query)}&limit=50`)
      const data: SearchResponse = await res.json()
      if (data.error) {
        setError(data.error)
        setResults([])
        setCount(null)
      } else {
        setResults(data.results)
        setCount(data.count)
      }
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Search failed')
      setResults([])
      setCount(null)
    } finally {
      setLoading(false)
    }
  }, [query])

  return (
    <div className="search-container">
      <form className="search-form" onSubmit={handleSearch}>
        <input
          type="text"
          className="search-input"
          placeholder="Search emails (FTS5: AND, OR, NOT, &quot;phrases&quot;, from_addr:john)"
          value={query}
          onChange={(e) => setQuery(e.target.value)}
        />
        <button type="submit" className="search-button" disabled={loading || !query.trim()}>
          {loading ? 'Searching...' : 'Search'}
        </button>
      </form>

      {error && <div className="search-error">{error}</div>}

      {count !== null && (
        <div className="search-count">
          {count} result{count !== 1 ? 's' : ''}
        </div>
      )}

      {results.length > 0 && (
        <div className="search-results">
          {results.map((r, i) => {
            const subject = r.subject || '(no subject)'
            const msgDate = r.msg_date ? r.msg_date.slice(0, 16).replace('T', ' ') : ''
            const from = r.from_addr || ''
            const fromDisplay = from.length > 40 ? from.slice(0, 40) + '...' : from

            return (
              <div key={`${r.account}-${r.folder}-${r.uid}-${i}`} className="search-result">
                <div className="result-header">
                  <code className="result-path">{r.account}/{r.folder}</code>
                  {msgDate && <span className="result-date">{msgDate}</span>}
                </div>
                <div className="result-subject">
                  {r.local_path ? (
                    <Link to={`/email/${r.local_path}`} className="subject-link">
                      {subject}
                    </Link>
                  ) : (
                    <span className="subject-text">{subject}</span>
                  )}
                </div>
                {fromDisplay && <div className="result-from">From: {fromDisplay}</div>}
              </div>
            )
          })}
        </div>
      )}
    </div>
  )
}
