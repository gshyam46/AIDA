'use client'

import {useMemo, useState} from 'react'
import {ArrowDown, ArrowDownToLine, ArrowUp, ArrowUpDown, ChevronLeft, ChevronRight, Search, X} from 'lucide-react'

type Cell = string | number | null
type Row = Record<string, Cell>
const PAGE_SIZE = 25

function matchesNumber(value: Cell, filter: string) {
  const match = filter.trim().match(/^(>=|<=|>|<|=)?\s*(-?\d+(?:\.\d+)?)$/)
  if (!match) return true
  if (value == null) return false
  const actual = Number(value), bound = Number(match[2])
  switch (match[1]) {
    case '>': return actual > bound
    case '<': return actual < bound
    case '<=': return actual <= bound
    case '=': return actual === bound
    default: return actual >= bound
  }
}

export default function DataTable({rows, columns, label, format, fileName, onExported}: {
  rows: Row[]; columns: string[]; label: (key: string) => string; format: (key: string, value: Cell) => string; fileName: string; onExported?: (count: number) => void
}) {
  const [search, setSearch] = useState('')
  const [filters, setFilters] = useState<Record<string, string>>({})
  const [sort, setSort] = useState<{key: string; direction: 'asc' | 'desc'} | null>(null)
  const [page, setPage] = useState(0)
  const numeric = useMemo(() => Object.fromEntries(columns.map(key => [key, rows.some(row => typeof row[key] === 'number') && rows.every(row => row[key] == null || typeof row[key] === 'number')])), [rows, columns])
  const visible = useMemo(() => {
    const needle = search.trim().toLowerCase()
    const output = rows.filter(row => (!needle || columns.some(key => String(row[key] ?? '').toLowerCase().includes(needle) || format(key, row[key]).toLowerCase().includes(needle)))
      && Object.entries(filters).every(([key, value]) => !value.trim() || (numeric[key] ? matchesNumber(row[key], value) : String(row[key] ?? '').toLowerCase().includes(value.trim().toLowerCase()))))
    if (!sort) return output
    return [...output].sort((left, right) => {
      const a = left[sort.key], b = right[sort.key]
      if (a == null || b == null) return a == null && b == null ? 0 : a == null ? 1 : -1
      const order = typeof a === 'number' && typeof b === 'number' ? a - b : String(a).localeCompare(String(b), undefined, {numeric: true})
      return sort.direction === 'asc' ? order : -order
    })
  }, [rows, columns, search, filters, sort, numeric, format])
  const pages = Math.max(1, Math.ceil(visible.length / PAGE_SIZE)), current = Math.min(page, pages - 1)
  const shown = visible.slice(current * PAGE_SIZE, (current + 1) * PAGE_SIZE)
  const toggleSort = (key: string) => {
    setSort(previous => previous?.key !== key ? {key, direction: numeric[key] ? 'desc' : 'asc'} : previous.direction === (numeric[key] ? 'desc' : 'asc') ? {key, direction: numeric[key] ? 'asc' : 'desc'} : null)
    setPage(0)
  }
  const exportCSV = () => {
    const escape = (value: unknown) => {let cell = String(value ?? ''); if (/^[=+@\-\t\r]/.test(cell)) cell = `'${cell}`; return `"${cell.replace(/"/g, '""')}"`}
    const csv = [columns.map(key => escape(label(key))).join(','), ...visible.map(row => columns.map(key => escape(row[key])).join(','))].join('\r\n')
    const url = URL.createObjectURL(new Blob([csv], {type: 'text/csv;charset=utf-8;'}))
    const anchor = document.createElement('a'); anchor.href = url; anchor.download = fileName; anchor.click(); URL.revokeObjectURL(url)
    onExported?.(visible.length)
  }
  const activeFilters = Object.values(filters).some(value => value.trim()) || !!search.trim() || !!sort
  if (!rows.length) return <p className="empty-chart">No rows match these filters.</p>
  return <div className="data-table">
    <div className="data-table-tools">
      <label className="table-search"><Search size={14}/><input aria-label="Search table" placeholder="Search every column" value={search} onChange={event => {setSearch(event.target.value); setPage(0)}}/></label>
      {activeFilters && <button className="text-button" onClick={() => {setSearch(''); setFilters({}); setSort(null); setPage(0)}}><X size={13}/>Clear table view</button>}
      <span className="count">{visible.length === rows.length ? `${rows.length} rows` : `${visible.length} of ${rows.length} rows`}</span>
      <button className="text-button" onClick={exportCSV} disabled={!visible.length}><ArrowDownToLine size={13}/>Export CSV</button>
    </div>
    <div className="data-table-wrap">
      <table>
        <thead>
          <tr>{columns.map(key => <th key={key} aria-sort={sort?.key === key ? (sort.direction === 'asc' ? 'ascending' : 'descending') : 'none'}>
            <button onClick={() => toggleSort(key)} aria-label={`Sort by ${label(key)}`}>{label(key)}{sort?.key !== key ? <ArrowUpDown size={11}/> : sort.direction === 'asc' ? <ArrowUp size={11}/> : <ArrowDown size={11}/>}</button>
          </th>)}</tr>
          <tr className="filter-row">{columns.map(key => <th key={key}><input aria-label={`Filter ${label(key)}`} placeholder={numeric[key] ? '≥ 100, < 5, = 3' : 'Contains…'} value={filters[key] || ''} onChange={event => {setFilters(previous => ({...previous, [key]: event.target.value})); setPage(0)}}/></th>)}</tr>
        </thead>
        <tbody>{shown.map((row, index) => <tr key={`${current}-${index}`}>{columns.map(key => <td key={key}>{row[key] == null ? '—' : format(key, row[key])}</td>)}</tr>)}</tbody>
      </table>
      {!visible.length && <p className="empty-chart">No rows match the table search or column filters.</p>}
    </div>
    {pages > 1 && <div className="table-pager"><span>Page {current + 1} of {pages}</span><button aria-label="Previous page" disabled={current === 0} onClick={() => setPage(current - 1)}><ChevronLeft size={14}/></button><button aria-label="Next page" disabled={current >= pages - 1} onClick={() => setPage(current + 1)}><ChevronRight size={14}/></button></div>}
  </div>
}
