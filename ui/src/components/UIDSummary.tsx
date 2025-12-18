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

  return (
    <div className="uid-summary">
      <div>
        <span className="stat">{status.server_uids.toLocaleString()}</span>{' '}
        <span className="stat-label">server UIDs</span>
      </div>
      <div>
        <span className="stat">{status.pulled_uids.toLocaleString()}</span>{' '}
        <span className="stat-label">pulled</span>
      </div>
      <div>
        <span className="stat warning">{status.unpulled_uids.toLocaleString()}</span>{' '}
        <span className="stat-label">unpulled</span>
      </div>
      <div className="no-mid">
        {status.no_message_id.toLocaleString()} without Message-ID
      </div>
    </div>
  )
}
