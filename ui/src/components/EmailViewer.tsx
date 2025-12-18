import { useState, useEffect } from 'react'
import { useParams, Link } from 'react-router-dom'
import type { EmailData } from '../types'
import './EmailViewer.scss'

export function EmailViewer() {
  const { '*': path } = useParams()
  const [email, setEmail] = useState<EmailData | null>(null)
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
        }
        setLoading(false)
      })
      .catch(err => {
        setError(err.message)
        setLoading(false)
      })
  }, [path])

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

  return (
    <div className="email-viewer">
      <Link to="/" className="back-link">← Back to Dashboard</Link>

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
