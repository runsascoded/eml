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

  const maxCount = Math.max(...data.data.map((d) => d.new + d.deduped), 1)

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
      </div>
      <div className="bar-chart">
        {data.data.map((d) => {
          const total = d.new + d.deduped
          const newPct = (d.new / maxCount) * 100
          const dedupedPct = (d.deduped / maxCount) * 100
          const tooltipText = `${d.new.toLocaleString()} new, ${d.deduped.toLocaleString()} deduped`
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
                </div>
              </Tooltip>
            </div>
          )
        })}
      </div>
    </div>
  )
}
