import { useState, useCallback, useEffect, useRef, type FormEvent } from 'react'
import { Link } from 'react-router-dom'
import type { SearchResult, SearchResponse } from '../types'
import './Search.scss'

const PAGE_SIZE = 50
const DEBOUNCE_MS = 300

export function Search() {
  const [query, setQuery] = useState('')
  const [results, setResults] = useState<SearchResult[]>([])
  const [total, setTotal] = useState<number | null>(null)
  const [offset, setOffset] = useState(0)
  const [error, setError] = useState<string | null>(null)
  const [loading, setLoading] = useState(false)
  const debounceRef = useRef<NodeJS.Timeout | null>(null)

  const doSearch = useCallback(async (searchQuery: string, searchOffset: number) => {
    if (!searchQuery.trim()) {
      setResults([])
      setTotal(null)
      setOffset(0)
      return
    }

    setLoading(true)
    setError(null)
    try {
      const res = await fetch(
        `/api/search?q=${encodeURIComponent(searchQuery)}&limit=${PAGE_SIZE}&offset=${searchOffset}`
      )
      const data: SearchResponse = await res.json()
      if (data.error) {
        setError(data.error)
        setResults([])
        setTotal(null)
      } else {
        setResults(data.results)
        setTotal(data.total)
        setOffset(searchOffset)
      }
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Search failed')
      setResults([])
      setTotal(null)
    } finally {
      setLoading(false)
    }
  }, [])

  // Debounced search-as-you-type
  useEffect(() => {
    if (debounceRef.current) {
      clearTimeout(debounceRef.current)
    }
    debounceRef.current = setTimeout(() => {
      doSearch(query, 0)
    }, DEBOUNCE_MS)
    return () => {
      if (debounceRef.current) clearTimeout(debounceRef.current)
    }
  }, [query, doSearch])

  const handleSubmit = (e: FormEvent) => {
    e.preventDefault()
    if (debounceRef.current) clearTimeout(debounceRef.current)
    doSearch(query, 0)
  }

  const handleClear = () => {
    setQuery('')
    setResults([])
    setTotal(null)
    setOffset(0)
    setError(null)
  }

  const handlePrev = () => {
    if (offset > 0) {
      doSearch(query, Math.max(0, offset - PAGE_SIZE))
    }
  }

  const handleNext = () => {
    if (total !== null && offset + PAGE_SIZE < total) {
      doSearch(query, offset + PAGE_SIZE)
    }
  }

  const startIdx = offset + 1
  const endIdx = Math.min(offset + results.length, total ?? 0)
  const hasPrev = offset > 0
  const hasNext = total !== null && offset + PAGE_SIZE < total

  return (
    <div className="search-container">
      <form className="search-form" onSubmit={handleSubmit}>
        <input
          type="text"
          className="search-input"
          placeholder="Search emails (FTS5: AND, OR, NOT, &quot;phrases&quot;, from_addr:john)"
          value={query}
          onChange={(e) => setQuery(e.target.value)}
        />
        {query && (
          <button type="button" className="clear-button" onClick={handleClear} title="Clear search">
            &times;
          </button>
        )}
        <button type="submit" className="search-button" disabled={loading || !query.trim()}>
          {loading ? 'Searching...' : 'Search'}
        </button>
      </form>

      {error && <div className="search-error">{error}</div>}

      {total !== null && (
        <div className="search-header">
          <div className="search-count">
            {total === 0 ? 'No results' : `Showing ${startIdx}-${endIdx} of ${total.toLocaleString()}`}
          </div>
          {total > PAGE_SIZE && (
            <div className="pagination">
              <button onClick={handlePrev} disabled={!hasPrev || loading} className="page-button">
                Prev
              </button>
              <button onClick={handleNext} disabled={!hasNext || loading} className="page-button">
                Next
              </button>
            </div>
          )}
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
