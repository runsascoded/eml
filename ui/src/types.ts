export interface Folder {
  account: string
  folder: string
  count: number
}

export interface PullActivity {
  uid: number
  folder: string
  path: string | null
  pulled_at: string
  status: 'new' | 'skipped' | 'failed' | null
  subject: string | null
  msg_date: string | null
}

export interface UIDStatus {
  account: string
  folder: string
  uidvalidity: number
  server_uids: number
  pulled_uids: number
  unpulled_uids: number
  no_message_id: number
  timestamp: string
  error?: string
}

export interface SyncStatus {
  running: boolean
  operation?: string
  account?: string
  folder?: string
  total?: number
  completed?: number
  skipped?: number
  failed?: number
  current_subject?: string
  started?: string
  pid?: number
  error?: string
}

export interface HistogramEntry {
  hour: string
  new: number
  deduped: number
}

export interface HistogramData {
  hours: number
  data: HistogramEntry[]
}

export interface SyncRun {
  id: number
  operation: string
  account: string
  folder: string
  started_at: string
  ended_at: string | null
  status: string
  total: number
  fetched: number
  skipped: number
  failed: number
  error_message: string | null
}

export interface SyncRunMessage {
  uid: number
  folder: string
  message_id: string | null
  local_path: string | null
  pulled_at: string
  status: string | null
  content_hash: string | null
  error_message: string | null
}

export interface FolderDetail {
  account: string
  folder: string
  uidvalidity: number | null
  server_uids: number
  pulled_uids: number
  messages: PullActivity[]
  sync_runs: SyncRun[]
}

export interface SearchResult {
  account: string
  folder: string
  uid: number
  message_id: string | null
  subject: string | null
  local_path: string | null
  msg_date: string | null
  from_addr: string | null
  to_addr: string | null
}

export interface SearchResponse {
  query: string
  total: number
  count: number
  offset: number
  limit: number
  results: SearchResult[]
  error?: string
}

export interface FSFolder {
  account: string
  folder: string
  path: string
  eml_count: number
}

export interface FSEmail {
  path: string
  subject: string
  from: string
  to: string
  date: string
  size: number
}

export interface FSEmailsResponse {
  account: string
  folder: string
  total: number
  offset: number
  limit: number
  emails: FSEmail[]
}

export interface FSThread {
  path: string
  subject: string
  from: string
  to: string
  date: string
  size: number
  thread_id: string | null
  thread_slug: string | null
  msg_count: number
  participants: string
}

export interface FSThreadsResponse {
  account: string
  folder: string
  total: number
  offset: number
  limit: number
  threads: FSThread[]
}

export interface EmailAttachment {
  filename: string
  content_type: string
  size: number
}

export interface EmailData {
  path: string
  headers: {
    from: string
    to: string
    cc: string
    date: string
    subject: string
    message_id: string
    in_reply_to: string
    references: string
  }
  body_html: string
  body_plain: string
  attachments: EmailAttachment[]
}

export interface ThreadMessage {
  uid: number
  subject: string | null
  message_id: string | null
  thread_id: string | null
  thread_slug: string | null
  local_path: string | null
  msg_date: string | null
  in_reply_to: string | null
  references: string | null
  from_addr: string | null
  to_addr: string | null
  attachment_count?: number
}

export interface ThreadResponse {
  message_id?: string
  thread_id: string | null
  thread_slug: string | null
  count: number
  messages: ThreadMessage[]
}
