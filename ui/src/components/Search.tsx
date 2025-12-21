import { useState, useCallback, useEffect, useRef, type FormEvent } from 'react'
import { Link } from 'react-router-dom'
import { useUrlParam, defStringParam, paginationParam } from '@rdub/use-url-params'
import type { SearchResult, SearchResponse } from '../types'
import './Search.scss'

const DEFAULT_PAGE_SIZE = 50
const PAGE_SIZE_OPTIONS = [20, 50, 100, 200] as const
const DEBOUNCE_MS = 300

export function Search() {
  const [query, setQuery] = useUrlParam('q', defStringParam(''))
  const [pagination, setPagination] = useUrlParam('p', paginationParam(DEFAULT_PAGE_SIZE, PAGE_SIZE_OPTIONS))
  const { offset, pageSize } = pagination
  const [results, setResults] = useState<SearchResult[]>([])
  const [total, setTotal] = useState<number | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [loading, setLoading] = useState(false)
  const debounceRef = useRef<ReturnType<typeof setTimeout> | null>(null)
  const initialSearchDone = useRef(false)

  const doSearch = useCallback(async (searchQuery: string, searchOffset: number, searchPageSize: number) => {
    if (!searchQuery.trim()) {
      setResults([])
      setTotal(null)
      return
    }

    setLoading(true)
    setError(null)
    try {
      const res = await fetch(
        `/api/search?q=${encodeURIComponent(searchQuery)}&limit=${searchPageSize}&offset=${searchOffset}`
      )
      const data: SearchResponse = await res.json()
      if (data.error) {
        setError(data.error)
        setResults([])
        setTotal(null)
      } else {
        setResults(data.results)
        setTotal(data.total)
      }
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Search failed')
      setResults([])
      setTotal(null)
    } finally {
      setLoading(false)
    }
  }, [])

  // Initial search from URL params on mount
  useEffect(() => {
    if (!initialSearchDone.current && query) {
      doSearch(query, offset, pageSize)
      initialSearchDone.current = true
    }
  }, [query, offset, pageSize, doSearch])

  // Debounced search-as-you-type (only after initial search)
  useEffect(() => {
    if (!initialSearchDone.current) return

    if (debounceRef.current) {
      clearTimeout(debounceRef.current)
    }
    debounceRef.current = setTimeout(() => {
      setPagination({ offset: 0, pageSize }) // Reset to page 0 when query changes
      doSearch(query, 0, pageSize)
    }, DEBOUNCE_MS)
    return () => {
      if (debounceRef.current) clearTimeout(debounceRef.current)
    }
  }, [query, pageSize, doSearch, setPagination])

  // Re-search when pagination changes
  useEffect(() => {
    if (initialSearchDone.current && query) {
      doSearch(query, offset, pageSize)
    }
  }, [offset, pageSize, query, doSearch])

  const handleSubmit = (e: FormEvent) => {
    e.preventDefault()
    if (debounceRef.current) clearTimeout(debounceRef.current)
    setPagination({ offset: 0, pageSize })
    doSearch(query, 0, pageSize)
  }

  const handleClear = () => {
    setQuery('')
    setPagination({ offset: 0, pageSize: DEFAULT_PAGE_SIZE })
    setResults([])
    setTotal(null)
    setError(null)
  }

  const handlePageSizeChange = (newSize: number) => {
    setPagination({ offset: 0, pageSize: newSize })
  }

  const handlePrev = () => {
    if (offset > 0) {
      setPagination({ offset: Math.max(0, offset - pageSize), pageSize })
    }
  }

  const handleNext = () => {
    if (total !== null && offset + pageSize < total) {
      setPagination({ offset: offset + pageSize, pageSize })
    }
  }

  const startIdx = offset + 1
  const endIdx = Math.min(offset + results.length, total ?? 0)
  const hasPrev = offset > 0
  const hasNext = total !== null && offset + pageSize < total

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
          <div className="pagination-controls">
            <select
              className="page-size-select"
              value={pageSize}
              onChange={(e) => handlePageSizeChange(Number(e.target.value))}
              disabled={loading}
            >
              {PAGE_SIZE_OPTIONS.map(size => (
                <option key={size} value={size}>{size} per page</option>
              ))}
            </select>
            {total > pageSize && (
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
