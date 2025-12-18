import { useState, useCallback } from 'react'
import { FolderNav } from './FolderNav'
import { SyncStatusBar } from './SyncStatusBar'
import { UIDSummary } from './UIDSummary'
import { Histogram } from './Histogram'
import { RecentActivity } from './RecentActivity'
import { SyncRunsList } from './SyncRunsList'
import { useFolders, useStatus, useRecent, useHistogram, useSyncStatus, useSyncRuns, useSSE } from '../hooks/useApi'
import type { UIDStatus, PullActivity, SyncStatus } from '../types'

export function Dashboard() {
  const [account, setAccount] = useState('y')
  const [folder, setFolder] = useState('Inbox')
  const [lastUpdate, setLastUpdate] = useState<Date | null>(null)

  const { folders } = useFolders()
  const { status, refresh: refreshStatus } = useStatus(account, folder)
  const { pulls, refresh: refreshRecent } = useRecent(account, folder)
  const { data: histogramData } = useHistogram(account, folder)
  const { sync, refresh: refreshSync } = useSyncStatus()
  const { runs } = useSyncRuns(5)

  const handleFolderSelect = useCallback((acc: string, fld: string) => {
    setAccount(acc)
    setFolder(fld)
  }, [])

  const handleStatusUpdate = useCallback((s: UIDStatus) => {
    if (s.account === account && s.folder === folder) {
      refreshStatus()
      setLastUpdate(new Date())
    }
  }, [account, folder, refreshStatus])

  const handleRecentUpdate = useCallback((_pulls: PullActivity[]) => {
    refreshRecent()
  }, [refreshRecent])

  const handleSyncUpdate = useCallback((_sync: SyncStatus) => {
    refreshSync()
  }, [refreshSync])

  useSSE(handleStatusUpdate, handleRecentUpdate, handleSyncUpdate)

  return (
    <>
      <h1>
        <span className="live-indicator" />
        EML Pull Status
      </h1>
      <FolderNav
        folders={folders}
        currentAccount={account}
        currentFolder={folder}
        onSelect={handleFolderSelect}
        linkMode
      />
      <SyncStatusBar sync={sync} />
      <div className="grid">
        <div className="card">
          <h2>Recent Sync Runs</h2>
          <SyncRunsList runs={runs} compact />
        </div>
        <div className="card">
          <h2>UID Summary</h2>
          <UIDSummary status={status} />
        </div>
        <div className="card">
          <h2>Activity by Hour (last 24h)</h2>
          <Histogram data={histogramData} />
        </div>
        <div className="card">
          <h2>Recent Activity</h2>
          <RecentActivity pulls={pulls} />
        </div>
      </div>
      {lastUpdate && (
        <p className="last-update">
          Last updated: {lastUpdate.toLocaleTimeString()}
        </p>
      )}
    </>
  )
}
