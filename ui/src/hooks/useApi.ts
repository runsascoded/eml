import { useState, useEffect, useCallback } from 'react'
import type { Folder, FolderDetail, PullActivity, UIDStatus, SyncStatus, HistogramData, SyncRun, SyncRunMessage } from '../types'

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

export function useStatus(account: string, folder: string) {
  const [status, setStatus] = useState<UIDStatus | null>(null)

  const refresh = useCallback(async () => {
    const res = await fetch(`/api/status?account=${account}&folder=${folder}`)
    const data = await res.json()
    setStatus(data)
  }, [account, folder])

  useEffect(() => {
    refresh()
  }, [refresh])

  return { status, refresh }
}

export function useRecent(account: string, folder: string) {
  const [pulls, setPulls] = useState<PullActivity[]>([])

  const refresh = useCallback(async () => {
    const res = await fetch(`/api/recent?account=${account}&folder=${folder}`)
    const data = await res.json()
    setPulls(data.pulls || [])
  }, [account, folder])

  useEffect(() => {
    refresh()
  }, [refresh])

  return { pulls, refresh }
}

export function useHistogram(account: string, folder: string) {
  const [data, setData] = useState<HistogramData | null>(null)

  const refresh = useCallback(async () => {
    const res = await fetch(`/api/histogram?account=${account}&folder=${folder}`)
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

export function useSyncRunDetail(runId: number | null) {
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
