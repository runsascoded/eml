import { Link } from 'react-router-dom'
import type { PullActivity } from '../types'
import './RecentActivity.scss'

interface Props {
  pulls: PullActivity[]
}

export function RecentActivity({ pulls }: Props) {
  if (pulls.length === 0) {
    return <div className="recent-list empty">No recent activity</div>
  }

  return (
    <div className="recent-list">
      {pulls.map((p, i) => {
        const itemClass = p.is_new ? 'recent-item new' : 'recent-item skipped'
        const subject = p.subject || '(no subject)'
        const subjectTrunc = subject.length > 60 ? subject.slice(0, 60) + '...' : subject
        const pulledAt = p.pulled_at.replace('T', ' ').slice(0, 19)
        const msgDate = p.msg_date ? p.msg_date.slice(0, 16).replace('T', ' ') : ''

        return (
          <div key={`${p.uid}-${i}`} className={itemClass}>
            <span className="recent-time">{pulledAt}</span>
            <span className={`recent-badge ${p.is_new ? 'badge-new' : 'badge-skip'}`}>
              {p.is_new ? 'NEW' : 'SKIP'}
            </span>
            {msgDate && <span className="msg-date">{msgDate}</span>}
            <br />
            {p.path ? (
              <Link to={`/email/${p.path}`} className="subject-link">
                {subjectTrunc}
              </Link>
            ) : (
              <span className="subject-text">{subjectTrunc}</span>
            )}
          </div>
        )
      })}
    </div>
  )
}
