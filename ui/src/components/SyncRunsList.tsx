import { Link } from 'react-router-dom'
import type { SyncRun } from '../types'
import './SyncRunsList.scss'

interface Props {
  runs: SyncRun[]
  compact?: boolean
}

function formatDuration(startedAt: string, endedAt: string | null): string {
  const start = new Date(startedAt)
  const end = endedAt ? new Date(endedAt) : new Date()
  const diff = Math.floor((end.getTime() - start.getTime()) / 1000)

  if (diff < 60) return `${diff}s`
  if (diff < 3600) return `${Math.floor(diff / 60)}m ${diff % 60}s`
  return `${Math.floor(diff / 3600)}h ${Math.floor((diff % 3600) / 60)}m`
}

function formatTime(iso: string): string {
  const d = new Date(iso)
  const now = new Date()
  const isToday = d.toDateString() === now.toDateString()

  if (isToday) {
    return d.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })
  }
  return d.toLocaleDateString([], { month: 'short', day: 'numeric' }) +
    ' ' + d.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })
}

function StatusBadge({ status }: { status: string }) {
  const className = `status-badge status-${status}`
  return <span className={className}>{status}</span>
}

export function SyncRunsList({ runs, compact = false }: Props) {
  if (runs.length === 0) {
    return <div className="sync-runs-list empty">No sync runs recorded yet</div>
  }

  return (
    <div className={`sync-runs-list ${compact ? 'compact' : ''}`}>
      {runs.map((run) => (
        <Link key={run.id} to={`/sync/${run.id}`} className="sync-run-item">
          <div className="run-header">
            <span className="operation">{run.operation}</span>
            <span className="folder">{run.account}/{run.folder}</span>
            <StatusBadge status={run.status} />
          </div>
          <div className="run-stats">
            <span className="stat new">{run.fetched} new</span>
            <span className="stat skipped">{run.skipped} skipped</span>
            {run.failed > 0 && <span className="stat failed">{run.failed} failed</span>}
            <span className="stat total">/ {run.total} total</span>
          </div>
          <div className="run-time">
            <span className="time">{formatTime(run.started_at)}</span>
            <span className="duration">{formatDuration(run.started_at, run.ended_at)}</span>
          </div>
        </Link>
      ))}
    </div>
  )
}
