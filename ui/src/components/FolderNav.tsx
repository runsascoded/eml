import { Link } from 'react-router-dom'
import type { Folder } from '../types'
import './FolderNav.scss'

interface Props {
  folders: Folder[]
  currentAccount: string
  currentFolder: string
  onSelect?: (account: string, folder: string) => void
  linkMode?: boolean
}

export function FolderNav({ folders, currentAccount, currentFolder, onSelect, linkMode = false }: Props) {
  if (folders.length === 0) {
    return <div className="folder-nav"><span className="no-folders">No folders found</span></div>
  }

  return (
    <div className="folder-nav">
      {folders.map((f) => {
        const isActive = f.folder === currentFolder && f.account === currentAccount
        if (linkMode) {
          return (
            <Link
              key={`${f.account}-${f.folder}`}
              to={`/folder/${f.account}/${encodeURIComponent(f.folder)}`}
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
