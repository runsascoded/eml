import type { UIDStatus } from '../types'
import './UIDSummary.scss'

interface Props {
  status: UIDStatus | null
}

export function UIDSummary({ status }: Props) {
  if (!status) {
    return <div className="uid-summary">Loading...</div>
  }

  if (status.error) {
    return <div className="uid-summary error">{status.error}</div>
  }

  const hasServerData = status.server_uids > 0

  return (
    <div className="uid-summary">
      {hasServerData && (
        <div className="stat-row">
          <span className="stat">{status.server_uids.toLocaleString()}</span>
          <span className="stat-label">on server</span>
        </div>
      )}
      <div className="stat-row">
        <span className="stat pulled">{status.pulled_uids.toLocaleString()}</span>
        <span className="stat-label">pulled</span>
      </div>
      {hasServerData && (
        <div className="stat-row">
          <span className={`stat ${status.unpulled_uids > 0 ? 'unpulled' : ''}`}>
            {status.unpulled_uids.toLocaleString()}
          </span>
          <span className="stat-label">remaining</span>
        </div>
      )}
      {hasServerData && status.server_uids > 0 && (
        <div className="progress-bar">
          <div
            className="progress-fill"
            style={{ width: `${(status.pulled_uids / status.server_uids) * 100}%` }}
          />
        </div>
      )}
    </div>
  )
}
