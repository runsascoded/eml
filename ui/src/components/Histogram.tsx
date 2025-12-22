import { Tooltip } from '@mui/material'
import type { HistogramData } from '../types'
import './Histogram.scss'

interface Props {
  data: HistogramData | null
}

export function Histogram({ data }: Props) {
  if (!data) {
    return <div className="histogram">Loading...</div>
  }

  const maxCount = Math.max(...data.data.map((d) => d.new + d.deduped + (d.failed || 0)), 1)
  const hasFailures = data.data.some((d) => (d.failed || 0) > 0)

  return (
    <div className="histogram">
      <div className="bar-legend">
        <div className="legend-item">
          <div className="legend-color new" />
          New
        </div>
        <div className="legend-item">
          <div className="legend-color deduped" />
          Deduped
        </div>
        {hasFailures && (
          <div className="legend-item">
            <div className="legend-color failed" />
            Failed
          </div>
        )}
      </div>
      <div className="bar-chart">
        {data.data.map((d) => {
          const failed = d.failed || 0
          const total = d.new + d.deduped + failed
          const newPct = (d.new / maxCount) * 100
          const dedupedPct = (d.deduped / maxCount) * 100
          const failedPct = (failed / maxCount) * 100
          const tooltipParts = [`${d.new.toLocaleString()} new`, `${d.deduped.toLocaleString()} deduped`]
          if (failed > 0) {
            tooltipParts.push(`${failed.toLocaleString()} failed`)
          }
          const tooltipText = tooltipParts.join(', ')
          return (
            <div key={d.hour} className="bar-row">
              <span className="bar-label">{d.hour}</span>
              <span className="bar-value">{total.toLocaleString()}</span>
              <Tooltip title={tooltipText} followCursor>
                <div className="bar-container">
                  {d.new > 0 && (
                    <div
                      className="bar bar-new"
                      style={{ width: `${newPct}%` }}
                    />
                  )}
                  {d.deduped > 0 && (
                    <div
                      className="bar bar-deduped"
                      style={{ width: `${dedupedPct}%` }}
                    />
                  )}
                  {failed > 0 && (
                    <div
                      className="bar bar-failed"
                      style={{ width: `${failedPct}%` }}
                    />
                  )}
                </div>
              </Tooltip>
            </div>
          )
        })}
      </div>
    </div>
  )
}
