import { useState, useEffect } from 'react'
import { useParams, Link, useNavigate } from 'react-router-dom'
import type { EmailData, ThreadResponse } from '../types'
import { useTheme } from '../theme'
import './EmailViewer.scss'

export function EmailViewer() {
  const { '*': path } = useParams()
  const navigate = useNavigate()
  const { uiStyle } = useTheme()
  const [email, setEmail] = useState<EmailData | null>(null)
  const [thread, setThread] = useState<ThreadResponse | null>(null)
  const [expandedMessages, setExpandedMessages] = useState<Set<string>>(new Set())
  const [error, setError] = useState<string | null>(null)
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    if (!path) {
      setError('No email path specified')
      setLoading(false)
      return
    }

    fetch(`/api/email/${path}`)
      .then(res => res.json())
      .then(data => {
        if (data.error) {
          setError(data.error)
        } else {
          setEmail(data)
          // Fetch thread if we have a message_id
          if (data.headers?.message_id) {
            const messageId = encodeURIComponent(data.headers.message_id)
            fetch(`/api/thread/${messageId}`)
              .then(res => res.json())
              .then(threadData => {
                if (!threadData.error && threadData.count > 1 && threadData.thread_slug) {
                  // Find the index of the current message in the thread
                  const msgIndex = threadData.messages.findIndex(
                    (m: { local_path: string | null }) => m.local_path === path
                  )
                  // Redirect to thread view with message index param
                  const indexParam = msgIndex >= 0 ? `?m=${msgIndex}` : ''
                  navigate(`/thread/${threadData.thread_slug}${indexParam}`, { replace: true })
                } else if (!threadData.error && threadData.count > 0) {
                  setThread(threadData)
                }
              })
              .catch(() => {
                // Silently ignore thread fetch errors
              })
          }
        }
        setLoading(false)
      })
      .catch(err => {
        setError(err.message)
        setLoading(false)
      })
  }, [path, navigate])

  if (loading) {
    return <div className="email-viewer">Loading...</div>
  }

  if (error) {
    return (
      <div className="email-viewer">
        <Link to="/" className="back-link">← Back to Dashboard</Link>
        <div className="error">{error}</div>
      </div>
    )
  }

  if (!email) {
    return (
      <div className="email-viewer">
        <Link to="/" className="back-link">← Back to Dashboard</Link>
        <div className="error">Email not found</div>
      </div>
    )
  }

  const formatSize = (bytes: number) => {
    if (bytes < 1024) return `${bytes} B`
    if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`
    return `${(bytes / (1024 * 1024)).toFixed(1)} MB`
  }

  const isCurrentEmail = (msgPath: string | null) => {
    if (!msgPath || !path) return false
    return msgPath === path
  }

  const getInitials = (addr: string) => {
    if (!addr) return '?'
    const match = addr.match(/^([^<@]+)/)
    if (match) {
      const name = match[1].trim()
      const parts = name.split(/\s+/)
      if (parts.length >= 2) {
        return (parts[0][0] + parts[parts.length - 1][0]).toUpperCase()
      }
      return name.slice(0, 2).toUpperCase()
    }
    return addr.slice(0, 2).toUpperCase()
  }

  const formatRelativeDate = (dateStr: string | null) => {
    if (!dateStr) return ''
    const date = new Date(dateStr)
    const now = new Date()
    const diffMs = now.getTime() - date.getTime()
    const diffDays = Math.floor(diffMs / (1000 * 60 * 60 * 24))

    if (diffDays === 0) {
      return date.toLocaleTimeString([], { hour: 'numeric', minute: '2-digit' })
    } else if (diffDays === 1) {
      return 'Yesterday'
    } else if (diffDays < 7) {
      return date.toLocaleDateString([], { weekday: 'short' })
    } else {
      return date.toLocaleDateString([], { month: 'short', day: 'numeric' })
    }
  }

  const toggleMessageExpand = (msgId: string) => {
    setExpandedMessages(prev => {
      const next = new Set(prev)
      if (next.has(msgId)) {
        next.delete(msgId)
      } else {
        next.add(msgId)
      }
      return next
    })
  }

  const renderThreadPanel = () => {
    if (!thread || thread.count <= 1) return null

    // If we have a thread_slug, link to the thread view (prefer slug over full thread_id)
    const threadUrl = thread.thread_slug
      ? `/thread/${thread.thread_slug}`
      : null

    if (uiStyle === 'superhuman') {
      return (
        <div className="thread-panel superhuman-style">
          <div className="thread-header">
            {threadUrl ? (
              <Link to={threadUrl} className="thread-count">{thread.count} messages →</Link>
            ) : (
              <span className="thread-count">{thread.count} messages</span>
            )}
          </div>
          <div className="thread-list">
            {thread.messages.map((msg, i) => {
              const isCurrent = isCurrentEmail(msg.local_path)
              return (
                <div
                  key={msg.message_id || i}
                  className={`thread-item ${isCurrent ? 'current' : ''}`}
                >
                  <div className="avatar">{getInitials(msg.from_addr || '')}</div>
                  <div className="content">
                    {msg.local_path && !isCurrent ? (
                      <Link to={`/email/${msg.local_path}`}>
                        <div className="top-row">
                          <span className="sender">{msg.from_addr?.split('<')[0].trim() || 'Unknown'}</span>
                          <span className="date">{formatRelativeDate(msg.msg_date)}</span>
                        </div>
                        <div className="snippet">{msg.subject || '(no subject)'}</div>
                      </Link>
                    ) : (
                      <>
                        <div className="top-row">
                          <span className="sender">{msg.from_addr?.split('<')[0].trim() || 'Unknown'}</span>
                          <span className="date">{formatRelativeDate(msg.msg_date)}</span>
                        </div>
                        <div className="snippet">{msg.subject || '(no subject)'}</div>
                      </>
                    )}
                  </div>
                </div>
              )
            })}
          </div>
        </div>
      )
    }

    if (uiStyle === 'gmail') {
      return (
        <div className="thread-panel gmail-style">
          <div className="thread-subject-header">
            {threadUrl ? (
              <Link to={threadUrl}>{email?.headers.subject}</Link>
            ) : (
              email?.headers.subject
            )}
            <span className="message-count">{thread.count}</span>
          </div>
          <div className="thread-list">
            {thread.messages.map((msg, i) => {
              const isCurrent = isCurrentEmail(msg.local_path)
              const msgKey = msg.message_id || `${i}`
              const isExpanded = expandedMessages.has(msgKey) || isCurrent
              return (
                <div
                  key={msgKey}
                  className={`thread-item ${isCurrent ? 'current' : ''} ${isExpanded ? 'expanded' : 'collapsed'}`}
                >
                  <div
                    className="collapsed-row"
                    onClick={() => !isCurrent && msg.local_path && toggleMessageExpand(msgKey)}
                  >
                    <div className="avatar">{getInitials(msg.from_addr || '')}</div>
                    <span className="sender">{msg.from_addr?.split('<')[0].trim() || 'Unknown'}</span>
                    <span className="snippet">{msg.subject || '(no subject)'}</span>
                    <span className="date">{formatRelativeDate(msg.msg_date)}</span>
                  </div>
                  {isExpanded && !isCurrent && msg.local_path && (
                    <div className="expanded-content">
                      <Link to={`/email/${msg.local_path}`} className="view-link">
                        View full message →
                      </Link>
                    </div>
                  )}
                </div>
              )
            })}
          </div>
        </div>
      )
    }

    // Default style
    return (
      <div className="thread-panel default-style">
        <h3>
          {threadUrl ? (
            <Link to={threadUrl}>Thread ({thread.count} messages) →</Link>
          ) : (
            <>Thread ({thread.count} messages)</>
          )}
        </h3>
        <div className="thread-list">
          {thread.messages.map((msg, i) => (
            <div
              key={msg.message_id || i}
              className={`thread-item ${isCurrentEmail(msg.local_path) ? 'current' : ''}`}
            >
              {msg.local_path && !isCurrentEmail(msg.local_path) ? (
                <Link to={`/email/${msg.local_path}`}>
                  <span className="thread-from">{msg.from_addr || 'Unknown'}</span>
                  <span className="thread-date">{msg.msg_date || ''}</span>
                  <span className="thread-subject">{msg.subject || '(no subject)'}</span>
                </Link>
              ) : (
                <>
                  <span className="thread-from">{msg.from_addr || 'Unknown'}</span>
                  <span className="thread-date">{msg.msg_date || ''}</span>
                  <span className="thread-subject">{msg.subject || '(no subject)'}</span>
                </>
              )}
            </div>
          ))}
        </div>
      </div>
    )
  }

  return (
    <div className={`email-viewer ui-style-${uiStyle}`}>
      <Link to="/" className="back-link">← Back to Dashboard</Link>

      {renderThreadPanel()}

      <div className="header">
        <div className="subject">{email.headers.subject}</div>
        <div className="meta">
          <div><strong>From:</strong> {email.headers.from}</div>
          <div><strong>To:</strong> {email.headers.to}</div>
          {email.headers.cc && <div><strong>Cc:</strong> {email.headers.cc}</div>}
          <div><strong>Date:</strong> {email.headers.date}</div>
        </div>
      </div>

      <div className="body">
        {email.body_html ? (
          <div dangerouslySetInnerHTML={{ __html: email.body_html }} />
        ) : (
          <pre>{email.body_plain}</pre>
        )}
      </div>

      {email.attachments.length > 0 && (
        <div className="attachments">
          <h3>Attachments</h3>
          {email.attachments.map((att, i) => (
            <div key={i} className="attachment">
              {att.filename} ({att.content_type}, {formatSize(att.size)})
            </div>
          ))}
        </div>
      )}

      <div className="path">File: {email.path}</div>
    </div>
  )
}
