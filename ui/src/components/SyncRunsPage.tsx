import { useState } from 'react'
import { Link } from 'react-router-dom'
import { SyncRunsList } from './SyncRunsList'
import { useSyncRunsPaginated } from '../hooks/useApi'
import './SyncRunsPage.scss'

const PAGE_SIZE = 20

export function SyncRunsPage() {
  const [page, setPage] = useState(0)
  const offset = page * PAGE_SIZE
  const { runs, total, loading } = useSyncRunsPaginated(PAGE_SIZE, offset)

  const totalPages = Math.ceil(total / PAGE_SIZE)
  const hasNext = page < totalPages - 1
  const hasPrev = page > 0

  return (
    <div className="sync-runs-page">
      <div className="header-row">
        <h1>Sync Runs</h1>
        <Link to="/admin" className="back-link">Back to Admin</Link>
      </div>

      <div className="pagination-info">
        Showing {offset + 1} - {Math.min(offset + PAGE_SIZE, total)} of {total.toLocaleString()} runs
        {loading && <span className="loading"> (loading...)</span>}
      </div>

      <SyncRunsList runs={runs} />

      <div className="pagination">
        <button
          onClick={() => setPage(0)}
          disabled={!hasPrev}
          className="pagination-btn"
        >
          First
        </button>
        <button
          onClick={() => setPage(p => p - 1)}
          disabled={!hasPrev}
          className="pagination-btn"
        >
          Prev
        </button>
        <span className="page-info">
          Page {page + 1} of {totalPages || 1}
        </span>
        <button
          onClick={() => setPage(p => p + 1)}
          disabled={!hasNext}
          className="pagination-btn"
        >
          Next
        </button>
        <button
          onClick={() => setPage(totalPages - 1)}
          disabled={!hasNext}
          className="pagination-btn"
        >
          Last
        </button>
      </div>
    </div>
  )
}
