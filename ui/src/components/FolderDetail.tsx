import { useParams, Link } from 'react-router-dom'
import { useFolderDetail, useFolders } from '../hooks/useApi'
import { SyncRunsList } from './SyncRunsList'
import './FolderDetail.scss'

function formatDate(iso: string): string {
  return new Date(iso).toLocaleString()
}

export function FolderDetail() {
  const { account, folder } = useParams<{ account: string; folder: string }>()
  const { data, loading } = useFolderDetail(account || null, folder || null)
  const { folders } = useFolders()

  if (loading && !data) {
    return <div className="folder-detail loading">Loading...</div>
  }

  if (!data) {
    return (
      <div className="folder-detail not-found">
        <h1>Folder Not Found</h1>
        <Link to="/">Back to Dashboard</Link>
      </div>
    )
  }

  return (
    <div className="folder-detail">
      <nav className="breadcrumb">
        <Link to="/">Dashboard</Link>
        <span>/</span>
        <span>{data.account}</span>
        <span>/</span>
        <span>{data.folder}</span>
      </nav>

      <header className="folder-header">
        <h1>
          <span className="account">{data.account}</span>
          <span className="separator">/</span>
          <span className="folder-name">{data.folder}</span>
        </h1>
      </header>

      <aside className="folder-sidebar">
        <h3>Folders</h3>
        <ul className="folder-list">
          {folders.map((f) => {
            const isActive = f.account === data.account && f.folder === data.folder
            return (
              <li key={`${f.account}-${f.folder}`}>
                <Link
                  to={`/folder/${f.account}/${encodeURIComponent(f.folder)}`}
                  className={isActive ? 'active' : ''}
                >
                  {f.folder}
                  <span className="count">{f.count.toLocaleString()}</span>
                </Link>
              </li>
            )
          })}
        </ul>
      </aside>

      <main className="folder-content">
        <section className="stats-section">
          <div className="stat">
            <span className="value">{data.server_uids.toLocaleString()}</span>
            <span className="label">Server UIDs</span>
          </div>
          <div className="stat">
            <span className="value">{data.pulled_uids.toLocaleString()}</span>
            <span className="label">Pulled</span>
          </div>
          {data.uidvalidity && (
            <div className="stat">
              <span className="value mono">{data.uidvalidity}</span>
              <span className="label">UIDVALIDITY</span>
            </div>
          )}
        </section>

        <section className="sync-runs-section">
          <h2>Recent Sync Runs</h2>
          {data.sync_runs.length === 0 ? (
            <p className="empty">No sync runs for this folder</p>
          ) : (
            <SyncRunsList runs={data.sync_runs} compact />
          )}
        </section>

        <section className="messages-section">
          <h2>Recent Messages ({data.messages.length})</h2>
          {data.messages.length === 0 ? (
            <p className="empty">No messages recorded for this folder</p>
          ) : (
            <table className="messages-table">
              <thead>
                <tr>
                  <th>UID</th>
                  <th>Subject</th>
                  <th>Date</th>
                  <th>Status</th>
                  <th>Pulled At</th>
                </tr>
              </thead>
              <tbody>
                {data.messages.map((m) => (
                  <tr key={m.uid} className={m.status === 'new' ? 'new' : 'skipped'}>
                    <td className="uid">{m.uid}</td>
                    <td className="subject">
                      {m.path ? (
                        <Link to={`/email/${m.path}`}>{m.subject || '(no subject)'}</Link>
                      ) : (
                        <span className="no-link">{m.subject || '(no subject)'}</span>
                      )}
                    </td>
                    <td className="date">{m.msg_date || '-'}</td>
                    <td className="status">
                      <span className={`status-pill ${m.status || 'skipped'}`}>
                        {m.status || 'skipped'}
                      </span>
                    </td>
                    <td className="pulled-at">{formatDate(m.pulled_at)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
        </section>
      </main>
    </div>
  )
}
