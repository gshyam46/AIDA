'use client'

import {useState} from 'react'
import {Catalog, formatValue, isRelational, QueryResponse} from '../lib/api'
import RelationalChart from './RelationalChart'

export default function QueryChart({result, catalog, type, onSelect, compact = false}: {
  result: QueryResponse; catalog?: Catalog | null; type?: string; onSelect?: (dimension: string, value: string) => void; compact?: boolean
}) {
  const [active, setActive] = useState<number | null>(null)
  if (isRelational(result.plan)) return <RelationalChart result={result} catalog={catalog} type={type} onSelect={onSelect} compact={compact}/>
  const dimension = result.plan?.dimension
  const metric = result.plan?.metric || 'value'
  const format = (value: number, short = false) => formatValue(value, metric, short, catalog)
  const rows = result.data
  if (!rows.length) return <div className="empty-chart">No matching data. Try a wider date range or remove a filter.</div>
  if (!dimension) return <div className="single-stat" data-testid="query-chart"><span>{metric.replace(/_/g, ' ')}</span><strong data-testid="metric-value">{rows[0].value == null ? '—' : format(Number(rows[0].value))}</strong><p>{rows[0].value == null ? 'No matching values. Try a wider date range or remove a filter.' : 'Calculated directly from matching records.'}</p></div>
  const min = Math.min(...rows.map(row => Number(row.value)), 0)
  const max = Math.max(...rows.map(row => Number(row.value)), 0) || (min === 0 ? 1 : 0)
  const span = max - min
  const labels = rows.map(row => String(row[dimension] ?? ''))
  const selected = active === null ? null : rows[active]
  const canDrill = !!onSelect
  const chartType = type || result.chart?.type || 'bar'
  const fmtLabel = (label: string) => /^\d{4}-\d{2}$/.test(label)
    ? new Date(`${label}-02T00:00:00`).toLocaleDateString('en-US', {month: 'short', year: '2-digit'}) : label
  const pick = (index: number) => {
    setActive(index)
    if (canDrill) onSelect!(dimension, labels[index])
  }
  if (chartType !== 'line' || rows.some(row => row.value == null)) return <div className={`bar-chart ${compact ? 'compact' : ''}`} data-testid="query-chart">
    <div className="chart-scale"><span>{format(min, true)}</span><span>{format(min + span / 2, true)}</span><span>{format(max, true)}</span></div>
    {rows.map((row, index) => <button key={labels[index]} type="button" className="bar-row" data-testid="chart-bar"
      aria-label={`${labels[index]}: ${row.value == null ? 'No matching values' : format(Number(row.value))}${canDrill ? ` — filter to ${labels[index]}` : ''}`}
      onClick={() => pick(index)} onMouseEnter={() => setActive(index)} onFocus={() => setActive(index)}>
      <span className="bar-label">{fmtLabel(labels[index])}</span>
      <span className="bar-track"><span className={`bar-fill bar-${index % 4}`} style={{position: 'absolute', left: `${(Math.min(Number(row.value), 0) - min) / span * 100}%`, width: `${Math.abs(Number(row.value)) / span * 100}%`}} /></span>
      <strong>{row.value == null ? '—' : format(Number(row.value), compact)}</strong>
    </button>)}
    <p className="chart-hint">{canDrill ? 'Select a bar to filter this group and explore another breakdown.' : `${rows.length} groups · values from the database`}</p>
  </div>
  const width = 760, height = 230, left = 62, right = 24, top = 20, bottom = 34
  const coords = rows.map((row, index) => ({
    x: left + index * (width - left - right) / Math.max(rows.length - 1, 1),
    y: top + (1 - (Number(row.value) - min) / span) * (height - top - bottom),
  }))
  const points = coords.map(p => `${p.x},${p.y}`).join(' ')
  const area = `M ${coords[0].x},${height - bottom} L ${coords.map(p => `${p.x},${p.y}`).join(' L ')} L ${coords[coords.length - 1].x},${height - bottom} Z`
  return <div className={`line-chart ${compact ? 'compact' : ''}`} data-testid="query-chart">
    <div className="chart-readout" aria-live="polite">{selected ? <><span>{labels[active!]}</span><strong>{format(Number(selected.value))}</strong></> : <span>Hover or select a point to see its value</span>}</div>
    <svg viewBox={`0 0 ${width} ${height}`} role="img" aria-label={`${metric.replace(/_/g, ' ')} by ${dimension}, ${rows.length} data points`}>
      {[0, .25, .5, .75, 1].map(ratio => {
        const y = top + (1 - ratio) * (height - top - bottom)
        return <g key={ratio}><line x1={left} x2={width-right} y1={y} y2={y} stroke="#DEDCCD" strokeDasharray="4 5"/><text x={left-12} y={y+4} textAnchor="end" fill="#6D705E" fontSize="11">{format(min + span*ratio, true)}</text></g>
      })}
      <path d={area} fill="#526b23" fillOpacity="0.08"/>
      <polyline points={points} fill="none" stroke="#526b23" strokeWidth="3" strokeLinecap="round" strokeLinejoin="round"/>
      {coords.map((point, index) => <g key={labels[index]}>
        <circle cx={point.x} cy={point.y} r={active === index ? 6 : 4} fill="#ffffff" stroke="#526b23" strokeWidth="2"/>
        <circle role="button" tabIndex={0} aria-label={`${labels[index]}: ${format(Number(rows[index].value))}`} cx={point.x} cy={point.y} r="13" fill="transparent" className="chart-point"
          onMouseEnter={() => setActive(index)} onFocus={() => setActive(index)} onClick={() => pick(index)} onKeyDown={event => {if (event.key === 'Enter' || event.key === ' ') {event.preventDefault(); pick(index)}}}>
          <title>{labels[index]}: {format(Number(rows[index].value))}</title>
        </circle>
        {(rows.length <= 12 || index % Math.ceil(rows.length / 12) === 0) && <text x={point.x} y={height-9} textAnchor="middle" fill="#6D705E" fontSize="11">{fmtLabel(labels[index])}</text>}
      </g>)}
    </svg>
    <p className="chart-hint">{canDrill ? 'Select a point to filter this group and explore another breakdown.' : 'Select any point for the exact value. Use Table to see every row.'}</p>
  </div>
}
