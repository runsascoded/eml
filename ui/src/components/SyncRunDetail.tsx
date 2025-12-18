import { useParams, Link } from 'react-router-dom'
import { useSyncRunDetail } from '../hooks/useApi'
import './SyncRunDetail.scss'

function formatDateTime(iso: string): string {
  return new Date(iso).toLocaleString()
}

function formatDuration(startedAt: string, endedAt: string | null): string {
  const start = new Date(startedAt)
  const end = endedAt ? new Date(endedAt) : new Date()
  const diff = Math.floor((end.getTime() - start.getTime()) / 1000)

  if (diff < 60) return `${diff} seconds`
  if (diff < 3600) return `${Math.floor(diff / 60)}m ${diff % 60}s`
  return `${Math.floor(diff / 3600)}h ${Math.floor((diff % 3600) / 60)}m`
}

export function SyncRunDetail() {
  const { runId } = useParams<{ runId: string }>()
  const { run, messages, loading } = useSyncRunDetail(runId ? parseInt(runId) : null)

  if (loading && !run) {
    return <div className="sync-run-detail loading">Loading...</div>
  }

  if (!run) {
    return (
      <div className="sync-run-detail not-found">
        <h1>Sync Run Not Found</h1>
        <Link to="/">Back to Dashboard</Link>
      </div>
    )
  }

  return (
    <div className="sync-run-detail">
      <nav className="breadcrumb">
        <Link to="/">Dashboard</Link>
        <span>/</span>
        <span>Sync Run #{run.id}</span>
      </nav>

      <header className="run-header">
        <h1>
          <span className="operation">{run.operation}</span>
          <code className="folder">{run.account}/{run.folder}</code>
          <span className={`status-badge status-${run.status}`}>{run.status}</span>
        </h1>
      </header>

      <section className="run-info">
        <div className="info-grid">
          <div className="info-item">
            <label>Started</label>
            <span>{formatDateTime(run.started_at)}</span>
          </div>
          <div className="info-item">
            <label>Duration</label>
            <span>{formatDuration(run.started_at, run.ended_at)}</span>
          </div>
          <div className="info-item">
            <label>Total Messages</label>
            <span>{run.total.toLocaleString()}</span>
          </div>
        </div>

        <div className="stats-bar">
          <div className="stat new">
            <span className="value">{run.fetched}</span>
            <span className="label">New</span>
          </div>
          <div className="stat skipped">
            <span className="value">{run.skipped}</span>
            <span className="label">Skipped</span>
          </div>
          <div className="stat failed">
            <span className="value">{run.failed}</span>
            <span className="label">Failed</span>
          </div>
        </div>

        {run.error_message && (
          <div className="error-message">
            <strong>Error:</strong> {run.error_message}
          </div>
        )}
      </section>

      <section className="messages-section">
        <h2>Messages ({messages.length})</h2>
        {messages.length === 0 ? (
          <p className="empty">No messages recorded for this sync run</p>
        ) : (
          <table className="messages-table">
            <thead>
              <tr>
                <th>UID</th>
                <th>Status</th>
                <th>Path / Error</th>
                <th>Time</th>
              </tr>
            </thead>
            <tbody>
              {messages.map((m) => (
                <tr key={m.uid} className={`status-${m.status || 'unknown'}`}>
                  <td className="uid">{m.uid}</td>
                  <td className="status">
                    <span className={`status-pill ${m.status || 'unknown'}`}>
                      {m.status || 'unknown'}
                    </span>
                  </td>
                  <td className="path">
                    {m.status === 'failed' && m.error_message ? (
                      <span className="error-text">{m.error_message}</span>
                    ) : m.local_path ? (
                      <Link to={`/email/${m.local_path}`}>{m.local_path}</Link>
                    ) : (
                      <span className="no-path">-</span>
                    )}
                  </td>
                  <td className="time">
                    {new Date(m.pulled_at).toLocaleTimeString()}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </section>
    </div>
  )
}
