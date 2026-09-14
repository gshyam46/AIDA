'use client'

import {useEffect, useState} from 'react'
import {Database, LoaderCircle, Plus, Trash2, Upload} from 'lucide-react'
import {configureSource, getSource, labelFor, Source, SourceInspection, SourceMapping, uploadSource} from '../lib/api'
import DatabaseConnections from './DatabaseConnections'

type Measure = SourceMapping['metrics'][number]
const identifier = (value: string) => value.toLowerCase().replace(/[^a-z0-9_]+/g, '_').replace(/^([^a-z])/, 'field_$1')
const countMetric: Measure = {id: 'records', label: 'Records', description: 'Count of all rows in the selected table, after filters.', aggregate: 'COUNT', format: 'number'}

export default function SourceSetup({enabled, pendingSource, onConfigured}: {enabled: boolean; pendingSource?: Source; onConfigured: (id: string) => Promise<void>}) {
  const [inspection, setInspection] = useState<SourceInspection | null>(null)
  const [name, setName] = useState('')
  const [table, setTable] = useState('')
  const [metrics, setMetrics] = useState<Measure[]>([{...countMetric}])
  const [dimensions, setDimensions] = useState<Record<string, string>>({})
  const [dateColumn, setDateColumn] = useState('')
  const [asOf, setAsOf] = useState('')
  const [currency, setCurrency] = useState('USD')
  const [busy, setBusy] = useState(false)
  const [error, setError] = useState('')
  const [mappingMode, setMappingMode] = useState<'simple' | 'relational'>('simple')
  const [manifest, setManifest] = useState('')
  const open = (data: SourceInspection) => {
    setInspection(data); setName(data.name); setTable(data.tables[0]?.name || '')
    setMetrics([{...countMetric}]); setDimensions({}); setDateColumn(''); setAsOf(''); setMappingMode('simple'); setManifest('')
  }
  useEffect(() => {
    if (!pendingSource || !enabled) return
    let active = true
    setBusy(true); setError('')
    void getSource(pendingSource.id).then(data => {if (active) open(data)}).catch(err => {if (active) setError(err.message)}).finally(() => {if (active) setBusy(false)})
    return () => {active = false}
  }, [pendingSource?.id, enabled])
  const upload = async (file?: File) => {
    if (!file) return
    if (file.size > 20 * 1024 * 1024) {setError('Choose a SQLite database smaller than 20 MB.'); return}
    setBusy(true); setError('')
    try {open(await uploadSource(file))} catch (err) {setError(err instanceof Error ? err.message : 'Upload could not be completed.')}
    finally {setBusy(false)}
  }
  const columns = inspection?.tables.find(item => item.name === table)?.columns || []
  const approvedColumns = columns.filter(column => !column.sensitive && !column.primary_key)
  const numericColumns = approvedColumns.filter(column => column.numeric)
  const updateMetric = (index: number, patch: Partial<Measure>) => setMetrics(items => items.map((item, at) => at === index ? {...item, ...patch} : item))
  const save = async () => {
    if (!inspection) return
    setBusy(true); setError('')
    const mapping: SourceMapping = {
      name, table, metrics,
      dimensions: Object.entries(dimensions).map(([column, values]) => {const approved = values.split(',').map(value => value.trim()).filter(Boolean); return {id: identifier(column), label: labelFor(column), column, ...(approved.length ? {values: approved} : {})}}),
      ...(dateColumn ? {date_column: dateColumn, as_of: asOf} : {}),
      ...(metrics.some(metric => metric.format === 'currency') ? {currency} : {}),
    }
    try {
      let approved: SourceMapping | Record<string, unknown> = mapping
      if (mappingMode === 'relational') {
        const parsed = JSON.parse(manifest)
        if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed) || parsed.version !== 2) throw new Error('Provide a version 2 relational catalog with approved tables, keys, measures, and fields.')
        approved = {...parsed, name}
      }
      await configureSource(inspection.id, approved)
      await onConfigured(inspection.id)
      setInspection(null)
    } catch (err) {setError(err instanceof Error ? err.message : 'The catalog could not be saved.')}
    finally {setBusy(false)}
  }
  return <><DatabaseConnections enabled={enabled} onInspect={open} onOpen={onConfigured}/><section className="panel source-setup" aria-labelledby="source-setup-heading">
    <div className="setup-heading"><div><h2 id="source-setup-heading">Connect your SQLite database</h2><p>Choose a local file, then define the business fields AIDA is allowed to query. Schema inspection reads column definitions, without sampling rows.</p></div><Database size={23}/></div>
    {!enabled ? <p className="setup-notice">Database uploads are disabled on this public demo. Run AIDA locally to connect your own SQLite file.</p> : <>
      {!inspection && <label className={`button secondary upload-button ${busy ? 'is-disabled' : ''}`}><Upload size={15}/>{busy ? 'Inspecting database…' : 'Choose SQLite file'}<input type="file" accept=".db,.sqlite,.sqlite3" aria-label="Upload SQLite database" disabled={busy} onChange={event => {void upload(event.target.files?.[0]); event.target.value = ''}}/></label>}
      {inspection && <><div className="mapping-mode"><button className={mappingMode === 'simple' ? 'active' : ''} type="button" onClick={() => setMappingMode('simple')}>Single table</button><button className={mappingMode === 'relational' ? 'active' : ''} type="button" onClick={() => setMappingMode('relational')}>Relational catalog</button></div><form onSubmit={event => {event.preventDefault(); void save()}}>
        {mappingMode === 'relational' ? <><label>Source name<input aria-label="Source name" required value={name} onChange={event => setName(event.target.value)} maxLength={80}/></label><details className="builder-detail"><summary>Inspect tables, columns, and declared keys</summary><div className="schema-inspection">{inspection.tables.map(item => <article key={item.name}><h3>{item.name}</h3><p>{item.columns.map(column => `${column.name} (${column.type || 'untyped'}${column.primary_key ? ', primary key' : ''}${column.sensitive ? ', restricted' : ''})`).join(' · ')}</p>{item.foreign_keys?.map((key, index) => <p key={index}>{item.name}.{key.from} → {key.table}.{key.to}</p>)}</article>)}</div></details><label className="definition-label">Approved relational catalog JSON<textarea aria-label="Approved relational catalog JSON" required value={manifest} onChange={event => setManifest(event.target.value)} rows={16} spellCheck={false} placeholder={'{\n  "version": 2,\n  "fact": { ... },\n  "tables": { ... },\n  "relations": [ ... ],\n  "metrics": [ ... ],\n  "dimensions": [ ... ]\n}'}/></label><p className="setup-help">Use an owner-reviewed version 2 manifest. AIDA verifies every table, column, join key, and measure against this snapshot before accepting the catalog. Relationships must preserve the approved fact-row grain; child records are filtered through EXISTS.</p></> : <>
        <div className="builder-grid"><label>Source name<input aria-label="Source name" required value={name} onChange={event => setName(event.target.value)} maxLength={100}/></label><label>Table<select aria-label="Source table" value={table} onChange={event => {setTable(event.target.value); setMetrics([{...countMetric}]); setDimensions({}); setDateColumn(''); setAsOf('')}}>{inspection.tables.map(item => <option value={item.name} key={item.name}>{item.name}</option>)}</select></label></div>
        <h3>Define measures</h3><p className="setup-help">Choose each aggregation and write its business meaning. The scale divisor is 1 for whole units or 100 for amounts stored as cents. AIDA uses these definitions to interpret questions.</p>
        {metrics.map((metric, index) => <div className="mapping-measure" key={index}><div className="builder-grid">
          <label>Measure label<input aria-label={`Measure ${index + 1} label`} required value={metric.label} onChange={event => updateMetric(index, {label: event.target.value, id: identifier(event.target.value)})}/></label>
          <label>Aggregation<select aria-label={`Measure ${index + 1} aggregation`} value={metric.aggregate} onChange={event => updateMetric(index, {aggregate: event.target.value as Measure['aggregate'], column: event.target.value === 'COUNT' ? undefined : numericColumns[0]?.name})}>{['COUNT', 'SUM', 'AVG', 'MIN', 'MAX'].map(value => <option key={value}>{value}</option>)}</select></label>
          {metric.aggregate !== 'COUNT' && <label>Numeric column<select aria-label={`Measure ${index + 1} column`} required value={metric.column || ''} onChange={event => updateMetric(index, {column: event.target.value})}><option value="">Select column</option>{numericColumns.map(column => <option value={column.name} key={column.name}>{column.name}</option>)}</select></label>}
          <label>Scale divisor<input aria-label={`Measure ${index + 1} scale divisor`} type="number" min="0.000001" max="1000000000" step="any" value={metric.scale ?? 1} onChange={event => updateMetric(index, {scale: Number(event.target.value)})} required/></label><label>Display format<select aria-label={`Measure ${index + 1} format`} value={metric.format} onChange={event => updateMetric(index, {format: event.target.value as Measure['format']})}><option value="number">Number</option><option value="currency">Currency</option></select></label>
        </div><label className="definition-label">Business definition<textarea aria-label={`Measure ${index + 1} definition`} required maxLength={500} value={metric.description} onChange={event => updateMetric(index, {description: event.target.value})} rows={2}/></label>{metrics.length > 1 && <button type="button" className="text-button" aria-label={`Remove measure ${index + 1}`} onClick={() => setMetrics(items => items.filter((_, at) => at !== index))}><Trash2 size={12}/>Remove measure</button>}</div>)}
        <button className="button secondary" type="button" disabled={!numericColumns.length || metrics.length >= 12} onClick={() => {const column = numericColumns[0]?.name; setMetrics(items => [...items, {id: `sum_${identifier(column)}`, label: `Total ${labelFor(column)}`, description: '', aggregate: 'SUM', column, format: 'number'}])}}><Plus size={13}/>Add measure</button>
        <h3>Approve dimensions and filter values</h3><p className="setup-help">Select fields that can be grouped. Optionally list permitted filter values, separated by commas. Identifiers and likely sensitive columns are excluded.</p>
        <div className="mapping-dimensions">{approvedColumns.map(column => <div key={column.name}><label className="check-label"><input type="checkbox" aria-label={`Approve dimension ${column.name}`} checked={column.name in dimensions} onChange={event => setDimensions(previous => {const next = {...previous}; if (event.target.checked) next[column.name] = ''; else delete next[column.name]; return next})}/><span>{column.name}<small>{column.type || 'untyped'}</small></span></label>{column.name in dimensions && <input aria-label={`Approved values for ${column.name}`} placeholder="Approved filter values (optional)" value={dimensions[column.name]} onChange={event => setDimensions(previous => ({...previous, [column.name]: event.target.value}))}/>}</div>)}</div>
        <div className="builder-grid"><label>Date column (optional)<select aria-label="Date column" value={dateColumn} onChange={event => setDateColumn(event.target.value)}><option value="">No date filtering</option>{approvedColumns.map(column => <option key={column.name}>{column.name}</option>)}</select></label>{dateColumn && <label>Relative-date reference<input aria-label="Relative-date reference" type="date" required value={asOf} onChange={event => setAsOf(event.target.value)}/></label>}{metrics.some(metric => metric.format === 'currency') && <label>Currency code<input aria-label="Currency code" pattern="[A-Z]{3}" maxLength={3} required value={currency} onChange={event => setCurrency(event.target.value.toUpperCase())}/></label>}</div>
        <p className="setup-help">Review these definitions before saving. Use the relational catalog option to approve multiple tables and their join relationships.</p></>}
        <div className="builder-actions"><button type="button" className="text-button" disabled={busy} onClick={() => setInspection(null)}>Cancel mapping</button><button className="button primary" type="submit" disabled={busy || (mappingMode === 'simple' ? !table : !manifest.trim())}>{busy ? <LoaderCircle className="spin" size={15}/> : <Database size={15}/>}Save catalog and explore</button></div>
      </form></>}
    </>}
    {error && <p className="setup-error" role="alert">{error}</p>}
  </section></>
}
