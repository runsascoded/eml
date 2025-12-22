import type { FolderStat } from '../types'

interface FolderStatsProps {
  folders: FolderStat[]
}

export function FolderStats({ folders }: FolderStatsProps) {
  if (folders.length === 0) {
    return <p className="muted">No folder data available</p>
  }

  const total = folders.reduce((sum, f) => sum + f.pulled, 0)
  const totalServer = folders.reduce((sum, f) => sum + f.server, 0)

  return (
    <div className="folder-stats">
      <table className="folder-stats-table">
        <thead>
          <tr>
            <th>Folder</th>
            <th className="num">Pulled</th>
            <th className="num">Server</th>
          </tr>
        </thead>
        <tbody>
          {folders.map(f => (
            <tr key={f.folder}>
              <td>{f.folder}</td>
              <td className="num">{f.pulled.toLocaleString()}</td>
              <td className="num">{f.server > 0 ? f.server.toLocaleString() : '-'}</td>
            </tr>
          ))}
        </tbody>
        <tfoot>
          <tr>
            <td><strong>Total</strong></td>
            <td className="num"><strong>{total.toLocaleString()}</strong></td>
            <td className="num"><strong>{totalServer > 0 ? totalServer.toLocaleString() : '-'}</strong></td>
          </tr>
        </tfoot>
      </table>
    </div>
  )
}
