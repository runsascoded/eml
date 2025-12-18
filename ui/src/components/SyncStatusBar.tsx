import type { SyncStatus } from '../types'
import './SyncStatusBar.scss'

interface Props {
  sync: SyncStatus
}

export function SyncStatusBar({ sync }: Props) {
  if (!sync.running) {
    return (
      <div className="sync-status sync-idle">
        ○ No sync running
      </div>
    )
  }

  const completed = sync.completed ?? 0
  const skipped = sync.skipped ?? 0
  const failed = sync.failed ?? 0
  const fetched = completed - skipped - failed
  const pct = sync.total ? Math.round(completed / sync.total * 100) : 0

  const details: string[] = []
  if (fetched > 0) details.push(`${fetched.toLocaleString()} fetched`)
  if (skipped > 0) details.push(`${skipped.toLocaleString()} skipped`)
  if (failed > 0) details.push(`${failed.toLocaleString()} failed`)
  const detailsStr = details.length ? ` [${details.join(', ')}]` : ''

  return (
    <div className="sync-status sync-running">
      <strong>● {sync.operation} in progress:</strong>{' '}
      <code>{sync.account}/{sync.folder}</code>{' '}
      {completed.toLocaleString()} / {(sync.total ?? 0).toLocaleString()} ({pct}%){detailsStr}
      {sync.current_subject && (
        <div className="current-subject">Current: {sync.current_subject}</div>
      )}
      <div className="progress-bar">
        <div className="progress-fill" style={{ width: `${pct}%` }} />
      </div>
    </div>
  )
}
