import { useState, useEffect, useCallback } from 'react'
import type { Folder, FolderDetail, PullActivity, UIDStatus, SyncStatus, HistogramData, SyncRun, SyncRunMessage, FolderStatsResponse } from '../types'

export function useFolders() {
  const [folders, setFolders] = useState<Folder[]>([])

  const refresh = useCallback(async () => {
    const res = await fetch('/api/folders')
    const data = await res.json()
    setFolders(data.folders || [])
  }, [])

  useEffect(() => {
    refresh()
    const interval = setInterval(refresh, 60000)
    return () => clearInterval(interval)
  }, [refresh])

  return { folders, refresh }
}

export function useStatus(account: string | null, folder: string | null) {
  const [status, setStatus] = useState<UIDStatus | null>(null)

  const refresh = useCallback(async () => {
    if (!account) return
    const params = new URLSearchParams()
    params.set('account', account)
    if (folder) params.set('folder', folder)
    const res = await fetch(`/api/status?${params}`)
    const data = await res.json()
    setStatus(data)
  }, [account, folder])

  useEffect(() => {
    refresh()
  }, [refresh])

  return { status, refresh }
}

export function useRecent(account: string | null, folder: string | null) {
  const [pulls, setPulls] = useState<PullActivity[]>([])

  const refresh = useCallback(async () => {
    if (!account) return
    const params = new URLSearchParams()
    params.set('account', account)
    if (folder) params.set('folder', folder)
    const res = await fetch(`/api/recent?${params}`)
    const data = await res.json()
    setPulls(data.pulls || [])
  }, [account, folder])

  useEffect(() => {
    refresh()
  }, [refresh])

  return { pulls, refresh }
}

export function useHistogram(account: string | null, folder: string | null) {
  const [data, setData] = useState<HistogramData | null>(null)

  const refresh = useCallback(async () => {
    if (!account) return
    const params = new URLSearchParams()
    params.set('account', account)
    if (folder) params.set('folder', folder)
    const res = await fetch(`/api/histogram?${params}`)
    const d = await res.json()
    setData(d)
  }, [account, folder])

  useEffect(() => {
    refresh()
    const interval = setInterval(refresh, 30000)
    return () => clearInterval(interval)
  }, [refresh])

  return { data, refresh }
}

export function useSyncStatus() {
  const [sync, setSync] = useState<SyncStatus>({ running: false })

  const refresh = useCallback(async () => {
    const res = await fetch('/api/sync-status')
    const data = await res.json()
    setSync(data)
  }, [])

  useEffect(() => {
    refresh()
  }, [refresh])

  return { sync, refresh }
}

export function useSSE(
  onStatus: (status: UIDStatus) => void,
  onRecent: (pulls: PullActivity[]) => void,
  onSync: (sync: SyncStatus) => void,
) {
  useEffect(() => {
    const evtSource = new EventSource('/api/stream')

    evtSource.addEventListener('status', (e) => {
      onStatus(JSON.parse(e.data))
    })

    evtSource.addEventListener('recent', (e) => {
      const data = JSON.parse(e.data)
      onRecent(data.pulls || [])
    })

    evtSource.addEventListener('sync', (e) => {
      onSync(JSON.parse(e.data))
    })

    evtSource.onerror = () => {
      console.log('SSE connection lost, reconnecting...')
      evtSource.close()
    }

    return () => evtSource.close()
  }, [onStatus, onRecent, onSync])
}

export function useSyncRuns(limit = 10) {
  const [runs, setRuns] = useState<SyncRun[]>([])

  const refresh = useCallback(async () => {
    const res = await fetch(`/api/sync-runs?limit=${limit}`)
    const data = await res.json()
    setRuns(data.runs || [])
  }, [limit])

  useEffect(() => {
    refresh()
    const interval = setInterval(refresh, 10000)
    return () => clearInterval(interval)
  }, [refresh])

  return { runs, refresh }
}

export function useSyncRunsPaginated(limit = 20, offset = 0) {
  const [runs, setRuns] = useState<SyncRun[]>([])
  const [total, setTotal] = useState(0)
  const [loading, setLoading] = useState(false)

  const refresh = useCallback(async () => {
    setLoading(true)
    const res = await fetch(`/api/sync-runs?limit=${limit}&offset=${offset}`)
    const data = await res.json()
    setRuns(data.runs || [])
    setTotal(data.total || 0)
    setLoading(false)
  }, [limit, offset])

  useEffect(() => {
    refresh()
  }, [refresh])

  return { runs, total, loading, refresh }
}

export function useSyncRunDetail(runId: number | null, liveUpdate = true) {
  const [run, setRun] = useState<SyncRun | null>(null)
  const [messages, setMessages] = useState<SyncRunMessage[]>([])
  const [loading, setLoading] = useState(false)

  const refresh = useCallback(async () => {
    if (!runId) return
    setLoading(true)
    const res = await fetch(`/api/sync-runs/${runId}`)
    const data = await res.json()
    setRun(data.run || null)
    setMessages(data.messages || [])
    setLoading(false)
  }, [runId])

  useEffect(() => {
    refresh()
  }, [refresh])

  // Subscribe to SSE for live updates when the sync run is in progress
  useEffect(() => {
    if (!runId || !liveUpdate || (run && run.status !== 'running')) return

    const evtSource = new EventSource('/api/stream')

    // Listen for sync updates (run status changes)
    evtSource.addEventListener('sync', () => {
      refresh()
    })

    // Listen for message pulls (new messages added)
    evtSource.addEventListener('recent', () => {
      refresh()
    })

    evtSource.onerror = () => {
      console.log('SSE connection lost, reconnecting...')
      evtSource.close()
    }

    return () => evtSource.close()
  }, [runId, liveUpdate, run?.status, refresh])

  return { run, messages, loading, refresh }
}

export function useFolderDetail(account: string | null, folder: string | null) {
  const [data, setData] = useState<FolderDetail | null>(null)
  const [loading, setLoading] = useState(false)

  const refresh = useCallback(async () => {
    if (!account || !folder) return
    setLoading(true)
    const res = await fetch(`/api/folder/${account}/${encodeURIComponent(folder)}`)
    const d = await res.json()
    setData(d)
    setLoading(false)
  }, [account, folder])

  useEffect(() => {
    refresh()
  }, [refresh])

  return { data, loading, refresh }
}

export function useFolderStats(account: string | null) {
  const [data, setData] = useState<FolderStatsResponse | null>(null)

  const refresh = useCallback(async () => {
    if (!account) return
    const res = await fetch(`/api/folder-stats?account=${account}`)
    const d = await res.json()
    setData(d)
  }, [account])

  useEffect(() => {
    refresh()
  }, [refresh])

  return { data, refresh }
}
