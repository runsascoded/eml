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
        <span className="stat">{status.pulled_uids.toLocaleString()}</span>{' '}
        <span className="stat-label">messages pulled</span>
      </div>
    </div>
  )
}
