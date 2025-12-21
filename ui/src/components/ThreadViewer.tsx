import { useState, useCallback, useMemo } from 'react'
import { useParams, Link } from 'react-router-dom'
import { useQuery } from '@tanstack/react-query'
import { useUrlParam, type Param } from '@rdub/use-url-params'
import Tooltip from '@mui/material/Tooltip'
import type { ThreadResponse, ThreadMessage, EmailData } from '../types'
import { useTheme } from '../theme'
import './ThreadViewer.scss'

// API fetch functions
async function fetchThread(threadId: string): Promise<ThreadResponse> {
  const res = await fetch(`/api/thread-by-slug/${threadId}`)
  const data = await res.json()
  if (data.error) throw new Error(data.error)
  return data
}

async function fetchEmailContent(localPath: string): Promise<EmailData> {
  const res = await fetch(`/api/email/${localPath}`)
  const data = await res.json()
  if (data.error) throw new Error(data.error)
  return data
}

// URL param for expanded message indices (space-separated, encodes as + in URL)
// Special value: -1 means "last message" (computed after thread loads)
// Empty string "" means "none expanded" (all collapsed)
const makeExpandedParam = (messageCount: number): Param<number[]> => ({
  encode: (indices) => {
    if (indices.length === 0) return '' // Empty string = none expanded
    if (indices.length === 1 && indices[0] === -1) return undefined // Default state (last msg)
    if (indices.length === 1 && indices[0] === messageCount - 1) return undefined
    return indices.join(' ') // Space encodes as + in URL
  },
  decode: (encoded) => {
    if (encoded === undefined) return [-1] // No param = default to last message
    if (encoded === '') return [] // Empty string = none expanded
    return encoded.split(' ').map(s => parseInt(s, 10)).filter(n => !isNaN(n))
  },
})

// Component to render email body with collapsible quoted text
function EmailBodyWithQuotes({ html, plain }: { html?: string; plain?: string }) {
  const [showQuoted, setShowQuoted] = useState(false)

  if (html) {
    const quotedPatterns = [
      /(<div class="gmail_quote">[\s\S]*$)/i,
      /(<blockquote[\s\S]*$)/i,
      /(<div[^>]*>On [^<]*wrote:[\s\S]*$)/i,
    ]

    let mainContent = html
    let quotedContent = ''

    for (const pattern of quotedPatterns) {
      const match = html.match(pattern)
      if (match && match.index !== undefined) {
        mainContent = html.slice(0, match.index)
        quotedContent = match[1]
        break
      }
    }

    if (quotedContent) {
      return (
        <div className="email-body-with-quotes">
          <div dangerouslySetInnerHTML={{ __html: mainContent }} />
          <div className="quoted-section">
            <button
              className="toggle-quoted"
              onClick={() => setShowQuoted(!showQuoted)}
            >
              {showQuoted ? '▼ Hide quoted text' : '▶ Show quoted text'}
            </button>
            {showQuoted && (
              <div
                className="quoted-content"
                dangerouslySetInnerHTML={{ __html: quotedContent }}
              />
            )}
          </div>
        </div>
      )
    }

    return <div dangerouslySetInnerHTML={{ __html: html }} />
  }

  if (plain) {
    const lines = plain.split('\n')
    const mainLines: string[] = []
    const quotedLines: string[] = []
    let inQuoted = false

    for (const line of lines) {
      if (!inQuoted && /^On .* wrote:$/.test(line.trim())) {
        inQuoted = true
        quotedLines.push(line)
      } else if (line.startsWith('>')) {
        inQuoted = true
        quotedLines.push(line)
      } else if (inQuoted) {
        quotedLines.push(line)
      } else {
        mainLines.push(line)
      }
    }

    if (quotedLines.length > 0) {
      return (
        <div className="email-body-with-quotes">
          <pre>{mainLines.join('\n')}</pre>
          <div className="quoted-section">
            <button
              className="toggle-quoted"
              onClick={() => setShowQuoted(!showQuoted)}
            >
              {showQuoted ? '▼ Hide quoted text' : '▶ Show quoted text'}
            </button>
            {showQuoted && (
              <pre className="quoted-content">{quotedLines.join('\n')}</pre>
            )}
          </div>
        </div>
      )
    }

    return <pre>{plain}</pre>
  }

  return null
}

// Parse email address string into name and email parts
interface ParsedAddress {
  name: string
  email: string
  full: string
}

function parseAddresses(addressStr: string | null | undefined): ParsedAddress[] {
  if (!addressStr) return []
  const addresses: ParsedAddress[] = []
  const regex = /(?:"([^"]+)"|([^<,]+))?\s*<?([^>,\s]+@[^>,\s]+)>?/g
  let match
  while ((match = regex.exec(addressStr)) !== null) {
    const quotedName = match[1]
    const unquotedName = match[2]
    const email = match[3]
    const name = (quotedName || unquotedName || '').trim()
    addresses.push({
      name: name || email.split('@')[0],
      email,
      full: name ? `${name} <${email}>` : email,
    })
  }
  return addresses
}

// Single address chip with hover tooltip
function AddressChip({ addr, showEmail, isLast }: {
  addr: ParsedAddress
  showEmail: boolean
  isLast: boolean
}) {
  const [copied, setCopied] = useState(false)

  const handleCopy = async (e: React.MouseEvent) => {
    e.stopPropagation()
    e.preventDefault()
    await navigator.clipboard.writeText(addr.email)
    setCopied(true)
    setTimeout(() => setCopied(false), 1500)
  }

  const tooltipSx = {
    bgcolor: 'var(--bg-secondary)',
    color: 'var(--text-primary)',
    border: '1px solid var(--border-color)',
    boxShadow: '0 4px 12px rgba(0,0,0,0.3)',
    fontSize: '0.85rem',
    padding: '6px 10px',
    maxWidth: 'none',
    '& .MuiTooltip-arrow': {
      color: 'var(--bg-secondary)',
      '&::before': {
        border: '1px solid var(--border-color)',
      },
    },
  }

  return (
    <Tooltip
      title={
        <span
          className="address-tooltip"
          onClick={(e) => e.stopPropagation()}
          onMouseDown={(e) => e.stopPropagation()}
        >
          <span className="tooltip-name">{addr.name}</span>
          <span className="tooltip-email">&lt;{addr.email}&gt;</span>
          <button
            className="tooltip-copy"
            onClick={handleCopy}
            onMouseDown={(e) => e.stopPropagation()}
          >
            {copied ? '✓' : '⧉'}
          </button>
        </span>
      }
      arrow
      placement="bottom"
      enterDelay={200}
      leaveDelay={100}
      slotProps={{
        tooltip: { sx: tooltipSx },
        popper: {
          modifiers: [{ name: 'preventOverflow', options: { boundary: 'viewport' } }],
          sx: { pointerEvents: 'auto' },
        },
      }}
    >
      <span
        className={showEmail ? 'address-item' : 'address-chip'}
        onClick={(e) => e.stopPropagation()}
      >
        <span className="name">{addr.name}</span>
        {showEmail && <span className="email">&lt;{addr.email}&gt;</span>}
        {!isLast && (showEmail ? ',' : ', ')}
      </span>
    </Tooltip>
  )
}

// Superhuman-style recipients display with tooltips and copy
function RecipientsRow({ label, addresses, expanded, onToggle }: {
  label: string
  addresses: ParsedAddress[]
  expanded: boolean
  onToggle: () => void
}) {
  if (addresses.length === 0) return null

  if (expanded) {
    return (
      <div className="recipients-row expanded" onClick={onToggle}>
        <span className="label">{label}</span>
        <div className="addresses-expanded">
          {addresses.map((addr, i) => (
            <AddressChip
              key={i}
              addr={addr}
              showEmail={true}
              isLast={i === addresses.length - 1}
            />
          ))}
        </div>
      </div>
    )
  }

  return (
    <div className="recipients-row collapsed" onClick={onToggle}>
      <span className="label">{label}</span>
      <div className="addresses-collapsed">
        {addresses.map((addr, i) => (
          <AddressChip
            key={i}
            addr={addr}
            showEmail={false}
            isLast={i === addresses.length - 1}
          />
        ))}
      </div>
    </div>
  )
}

// Message meta with Superhuman-style recipients
function MessageMeta({ from, to, cc, date, uiStyle }: {
  from: string | null | undefined
  to: string | null | undefined
  cc: string | null | undefined
  date: string
  uiStyle: string
}) {
  const [expanded, setExpanded] = useState(false)

  const fromAddrs = parseAddresses(from)
  const toAddrs = parseAddresses(to)
  const ccAddrs = parseAddresses(cc)

  if (uiStyle === 'superhuman') {
    return (
      <div className="message-meta superhuman-meta">
        <RecipientsRow label="From" addresses={fromAddrs} expanded={expanded} onToggle={() => setExpanded(!expanded)} />
        <RecipientsRow label="To" addresses={toAddrs} expanded={expanded} onToggle={() => setExpanded(!expanded)} />
        {ccAddrs.length > 0 && (
          <RecipientsRow label="Cc" addresses={ccAddrs} expanded={expanded} onToggle={() => setExpanded(!expanded)} />
        )}
        <div className="date-row">
          <span className="date-value">{date}</span>
          {!expanded && (toAddrs.length + ccAddrs.length > 3) && (
            <button className="expand-btn" onClick={() => setExpanded(true)}>×</button>
          )}
        </div>
      </div>
    )
  }

  return (
    <div className="message-meta">
      <div><strong>From:</strong> {from}</div>
      <div><strong>To:</strong> {to}</div>
      {cc && <div><strong>Cc:</strong> {cc}</div>}
      <div><strong>Date:</strong> {date}</div>
    </div>
  )
}

// Individual message card component with its own content query
function MessageCard({
  msg,
  idx,
  isExpanded,
  onToggle,
  uiStyle,
}: {
  msg: ThreadMessage
  idx: number
  isExpanded: boolean
  onToggle: () => void
  uiStyle: string
}) {
  // Fetch email content when expanded and has local_path
  const { data: emailData, isLoading, error } = useQuery({
    queryKey: ['email', msg.local_path],
    queryFn: () => fetchEmailContent(msg.local_path!),
    enabled: isExpanded && !!msg.local_path,
    staleTime: Infinity, // Email content never changes
  })

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

  const formatDate = (dateStr: string | null) => {
    if (!dateStr) return ''
    const date = new Date(dateStr)
    return date.toLocaleString([], {
      month: 'short',
      day: 'numeric',
      year: 'numeric',
      hour: 'numeric',
      minute: '2-digit',
    })
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

  const getSenderName = (addr: string | null) => {
    if (!addr) return 'Unknown'
    const match = addr.match(/^([^<]+)</)
    if (match) return match[1].trim()
    return addr.split('@')[0]
  }

  const formatFileSize = (bytes: number) => {
    if (bytes < 1024) return `${bytes} B`
    if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`
    return `${(bytes / (1024 * 1024)).toFixed(1)} MB`
  }

  return (
    <div
      data-msg-idx={idx}
      className={`message-card ${isExpanded ? 'expanded' : 'collapsed'}`}
    >
      <div className="message-header" onClick={onToggle}>
        <div className="avatar">{getInitials(msg.from_addr || '')}</div>
        <div className="sender-info">
          <span className="sender-name">{getSenderName(msg.from_addr)}</span>
          {!isExpanded && (
            <span className="snippet">
              {msg.subject || '(no subject)'}
            </span>
          )}
        </div>
        {!isExpanded && (msg.attachment_count ?? 0) > 0 && (
          <span className="attachment-indicator" title={`${msg.attachment_count} attachment${msg.attachment_count! > 1 ? 's' : ''}`}>
            📎
          </span>
        )}
        <span className="date">{formatRelativeDate(msg.msg_date)}</span>
      </div>

      {isExpanded && (
        <div className="message-content">
          {isLoading && <div className="loading">Loading...</div>}
          {error && <div className="error">{error instanceof Error ? error.message : 'Failed to load'}</div>}
          {emailData && (
            <>
              <MessageMeta
                from={emailData.headers.from}
                to={emailData.headers.to}
                cc={emailData.headers.cc}
                date={formatDate(msg.msg_date)}
                uiStyle={uiStyle}
              />
              <div className="message-body">
                <EmailBodyWithQuotes
                  html={emailData.body_html}
                  plain={emailData.body_plain}
                />
              </div>
              {emailData.attachments.length > 0 && (
                <div className="attachments">
                  <div className="attachments-header">
                    <span className="attachments-icon">📎</span>
                    <span className="attachments-count">
                      {emailData.attachments.length} attachment{emailData.attachments.length > 1 ? 's' : ''}
                    </span>
                  </div>
                  <div className="attachments-list">
                    {emailData.attachments.map((att, i) => {
                      const isImage = att.content_type.startsWith('image/')
                      const downloadUrl = `/api/attachment/${msg.local_path}/${encodeURIComponent(att.filename)}`
                      return (
                        <a
                          key={i}
                          href={downloadUrl}
                          target="_blank"
                          rel="noopener noreferrer"
                          className={`attachment ${isImage ? 'attachment-image' : ''}`}
                          onClick={(e) => e.stopPropagation()}
                        >
                          {isImage ? (
                            <img
                              src={downloadUrl}
                              alt={att.filename}
                              className="attachment-thumbnail"
                            />
                          ) : (
                            <span className="attachment-icon">📄</span>
                          )}
                          <span className="attachment-info">
                            <span className="attachment-name">{att.filename}</span>
                            <span className="attachment-size">{formatFileSize(att.size)}</span>
                          </span>
                        </a>
                      )
                    })}
                  </div>
                </div>
              )}
            </>
          )}
        </div>
      )}
    </div>
  )
}

export function ThreadViewer() {
  const { '*': threadId } = useParams()
  const { uiStyle } = useTheme()

  // Fetch thread data
  const { data: thread, isLoading, error } = useQuery({
    queryKey: ['thread', threadId],
    queryFn: () => fetchThread(threadId!),
    enabled: !!threadId,
  })

  const messages = thread?.messages ?? []

  // URL param for expanded messages
  const expandedParam = useMemo(() => makeExpandedParam(messages.length), [messages.length])
  const [expandedIndices, setExpandedIndices] = useUrlParam('m', expandedParam)

  // Compute actual expanded set, resolving -1 sentinel to last message
  // Parse URL directly to handle initial load before hook state is fully synced
  const expandedMessages = useMemo(() => {
    if (messages.length === 0) return new Set<number>()

    const urlParams = new URLSearchParams(window.location.search)
    const mParam = urlParams.get('m')

    let indices: number[]
    if (mParam === null) {
      indices = [messages.length - 1]
    } else if (mParam === '') {
      indices = []
    } else {
      indices = mParam.split(' ').map(s => parseInt(s, 10)).filter(n => !isNaN(n))
    }

    return new Set(indices.filter(i => i >= 0 && i < messages.length))
  }, [messages.length, expandedIndices])

  const toggleMessage = useCallback((idx: number) => {
    const currentExpanded = new Set(
      expandedIndices.map(i => i === -1 ? messages.length - 1 : i).filter(i => i >= 0)
    )

    if (currentExpanded.has(idx)) {
      currentExpanded.delete(idx)
    } else {
      currentExpanded.add(idx)
    }

    setExpandedIndices(Array.from(currentExpanded).sort((a, b) => a - b))
  }, [expandedIndices, messages.length, setExpandedIndices])

  if (isLoading) {
    return <div className="thread-viewer">Loading...</div>
  }

  if (error) {
    return (
      <div className="thread-viewer">
        <Link to="/" className="back-link">← Back to Dashboard</Link>
        <div className="error">{error instanceof Error ? error.message : 'Failed to load thread'}</div>
      </div>
    )
  }

  if (!thread || messages.length === 0) {
    return (
      <div className="thread-viewer">
        <Link to="/" className="back-link">← Back to Dashboard</Link>
        <div className="error">Thread not found</div>
      </div>
    )
  }

  const threadSubject = messages[0]?.subject || '(no subject)'

  return (
    <div className={`thread-viewer ui-style-${uiStyle}`}>
      <Link to="/" className="back-link">← Back to Dashboard</Link>

      <div className="thread-header">
        <h1 className="thread-subject">{threadSubject}</h1>
        <span className="message-count">{thread.count} messages</span>
      </div>

      <div className="thread-messages">
        {messages.map((msg, idx) => (
          <MessageCard
            key={msg.message_id || idx}
            msg={msg}
            idx={idx}
            isExpanded={expandedMessages.has(idx)}
            onToggle={() => toggleMessage(idx)}
            uiStyle={uiStyle}
          />
        ))}
      </div>
    </div>
  )
}
