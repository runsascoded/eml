import { Link } from 'react-router-dom'
import type { Folder } from '../types'
import './FolderNav.scss'

interface Props {
  folders: Folder[]
  currentAccount: string | null  // null until loaded
  currentFolder: string | null  // null means "All"
  onSelect?: (account: string, folder: string | null) => void
  linkMode?: boolean
  showAll?: boolean  // Show "All" option
}

export function FolderNav({ folders, currentAccount, currentFolder, onSelect, linkMode = false, showAll = false }: Props) {
  if (folders.length === 0) {
    return <div className="folder-nav"><span className="no-folders">No folders found</span></div>
  }

  // Calculate total count across all folders for the account
  const totalCount = folders
    .filter(f => f.account === currentAccount)
    .reduce((sum, f) => sum + f.count, 0)

  return (
    <div className="folder-nav">
      {showAll && currentAccount && (
        <button
          className={`folder-btn ${currentFolder === null ? 'active' : ''}`}
          onClick={() => onSelect?.(currentAccount, null)}
        >
          All
          <span className="folder-count">{totalCount.toLocaleString()}</span>
        </button>
      )}
      {folders.map((f) => {
        const isActive = f.folder === currentFolder && f.account === currentAccount
        if (linkMode) {
          return (
            <Link
              key={f.folder}
              to={`/folder/${encodeURIComponent(f.folder)}`}
              className={`folder-btn ${isActive ? 'active' : ''}`}
            >
              {f.folder}
              <span className="folder-count">{f.count.toLocaleString()}</span>
            </Link>
          )
        }
        return (
          <button
            key={`${f.account}-${f.folder}`}
            className={`folder-btn ${isActive ? 'active' : ''}`}
            onClick={() => onSelect?.(f.account, f.folder)}
          >
            {f.folder}
            <span className="folder-count">{f.count.toLocaleString()}</span>
          </button>
        )
      })}
    </div>
  )
}
