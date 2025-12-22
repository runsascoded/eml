import { useState, useCallback, useEffect } from 'react'
import { Link } from 'react-router-dom'
import { FolderNav } from './FolderNav'
import { SyncStatusBar } from './SyncStatusBar'
import { UIDSummary } from './UIDSummary'
import { FolderStats } from './FolderStats'
import { Histogram } from './Histogram'
import { RecentActivity } from './RecentActivity'
import { SyncRunsList } from './SyncRunsList'
import { Search } from './Search'
import { useFolders, useStatus, useRecent, useHistogram, useSyncStatus, useSyncRuns, useSSE, useFolderStats } from '../hooks/useApi'
import type { UIDStatus, PullActivity, SyncStatus } from '../types'

export function Dashboard() {
  // Account is derived from the folders list (first account found)
  const [account, setAccount] = useState<string | null>(null)
  const [folder, setFolder] = useState<string | null>(null)  // null = All folders
  const [lastUpdate, setLastUpdate] = useState<Date | null>(null)

  const { folders } = useFolders()

  // Set account from first folder when folders load
  useEffect(() => {
    if (folders.length > 0 && !account) {
      setAccount(folders[0].account)
    }
  }, [folders, account])
  const { status, refresh: refreshStatus } = useStatus(account, folder)
  const { pulls, refresh: refreshRecent } = useRecent(account, folder)
  const { data: histogramData } = useHistogram(account, folder)
  const { sync, refresh: refreshSync } = useSyncStatus()
  const { runs } = useSyncRuns(5)
  const { data: folderStats } = useFolderStats(account)

  const handleFolderSelect = useCallback((acc: string, fld: string | null) => {
    setAccount(acc)
    setFolder(fld)
  }, [])

  const handleStatusUpdate = useCallback((s: UIDStatus) => {
    // Refresh if viewing all folders or if matching specific folder
    if (s.account === account && (folder === null || s.folder === folder)) {
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
      <div className="header-row">
        <h1>
          <span className="live-indicator" />
          EML Admin
        </h1>
        <Link to="/" className="browse-link">Back to Inbox</Link>
      </div>
      <FolderNav
        folders={folders}
        currentAccount={account}
        currentFolder={folder}
        onSelect={handleFolderSelect}
        showAll
        dropdownMode
      />
      <SyncStatusBar sync={sync} />
      <div className="card">
        <h2>Search</h2>
        <Search />
      </div>
      <div className="grid">
        <div className="card">
          <h2>Recent Sync Runs <Link to="/admin/syncs" className="view-all-link">View all</Link></h2>
          <SyncRunsList runs={runs} compact />
        </div>
        <div className="card">
          <h2>UID Summary</h2>
          {folder === null ? (
            folderStats?.folders ? (
              <FolderStats folders={folderStats.folders} />
            ) : (
              <p className="muted">Loading folder stats...</p>
            )
          ) : (
            <UIDSummary status={status} />
          )}
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
