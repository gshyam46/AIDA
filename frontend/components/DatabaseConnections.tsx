'use client'

import {useCallback, useEffect, useState} from 'react'
import {Database, LoaderCircle, RefreshCw} from 'lucide-react'
import {getSource, request, SourceInspection} from '../lib/api'

type Engine = 'postgresql' | 'mysql' | 'sqlserver'
type Schedule = 'manual' | 'hourly' | 'daily'
type RemoteTable = {name: string; primary_key: string[]; columns: {name: string; type: string; supported: boolean; primary_key: boolean}[]}
type Connection = {id: string; name: string; engine: Engine; source_id: string | null; schedule: Schedule; next_refresh: number | null; status: string; error: string | null; last_success: string | null; rows: number | null; history: {started_at: string; finished_at: string; status: string; error: string | null}[]}
const ports = {postgresql: 5432, mysql: 3306, sqlserver: 1433}
const labels = {postgresql: 'PostgreSQL', mysql: 'MySQL', sqlserver: 'SQL Server'}
const when = (value: string | null) => value ? new Date(value).toLocaleString() : 'No successful refresh yet'

export default function DatabaseConnections({enabled, onInspect, onOpen}: {enabled: boolean; onInspect: (data: SourceInspection) => void; onOpen: (id: string) => Promise<void>}) {
  const [available, setAvailable] = useState(false)
  const [connections, setConnections] = useState<Connection[]>([])
  const [editing, setEditing] = useState(false)
  const [engine, setEngine] = useState<Engine>('postgresql')
  const [host, setHost] = useState('')
  const [port, setPort] = useState(5432)
  const [database, setDatabase] = useState('')
  const [schema, setSchema] = useState('public')
  const [username, setUsername] = useState('')
  const [password, setPassword] = useState('')
  const [name, setName] = useState('')
  const [tls, setTls] = useState(true)
  const [schedule, setSchedule] = useState<Schedule>('manual')
  const [tables, setTables] = useState<RemoteTable[] | null>(null)
  const [selection, setSelection] = useState<Record<string, string[]>>({})
  const [busy, setBusy] = useState(false)
  const [error, setError] = useState('')
  const reload = useCallback(async () => {
    const data = await request('connections')
    setAvailable(data.enabled); setConnections(data.connections)
  }, [])
  useEffect(() => {
    if (!enabled) return
    let active = true
    const poll = async () => {
      try {const data = await request('connections'); if (active) {setAvailable(data.enabled); setConnections(data.connections)}}
      catch (err) {if (active) setError(err instanceof Error ? err.message : 'Could not load connections.')}
    }
    void poll()
    const timer = setInterval(() => void poll(), 3000)
    return () => {active = false; clearInterval(timer)}
  }, [enabled])
  const run = async (fn: () => Promise<void>) => {
    setBusy(true); setError('')
    try {await fn()} catch (err) {setError(err instanceof Error ? err.message : 'Connection request failed.')}
    finally {setBusy(false)}
  }
  const spec = () => ({engine, host, port, database, schema, username, password, tls})
  const inspect = () => run(async () => {
    const result = await request('connections/inspect', {method: 'POST', body: JSON.stringify(spec())})
    setTables(result.tables); setSelection({})
  })
  const create = () => run(async () => {
    await request('connections', {method: 'POST', body: JSON.stringify({name, connection: spec(), schedule,
      selection: Object.entries(selection).filter(([, columns]) => columns.length).map(([table, columns]) => ({table, columns}))})})
    setPassword(''); setTables(null); setSelection({}); setEditing(false); await reload()
  })
  const action = (id: string, operation: string, body = {}) => run(async () => {
    await request(`connections/${id}/${operation}`, {method: 'POST', body: JSON.stringify(body)}); await reload()
  })
  if (!enabled) return null
  return <section className="panel source-setup connection-setup" aria-labelledby="connections-heading">
    <div className="setup-heading"><div><h2 id="connections-heading">Database connections</h2><p>Connect with a read-only database account. Select the reporting columns to copy, approve their business definitions, then keep your snapshot up to date.</p></div><Database size={23}/></div>
    {!available && <p className="setup-notice">Database connections need an encryption key configured by the workspace operator. See docs/CONNECTIONS.md for the setup steps. SQLite upload remains available below.</p>}
    {available && !editing && <button className="button secondary" onClick={() => {setEditing(true); setError('')}}>Connect database</button>}
    {editing && <form onSubmit={event => {event.preventDefault(); void (tables ? create() : inspect())}}>
      {!tables ? <><div className="builder-grid">
        <label>Connection name<input aria-label="Connection name" required maxLength={80} value={name} onChange={e => setName(e.target.value)}/></label>
        <label>Database engine<select aria-label="Database engine" value={engine} onChange={e => {const next = e.target.value as Engine; setEngine(next); setPort(ports[next]); setSchema(next === 'postgresql' ? 'public' : next === 'sqlserver' ? 'dbo' : '')}}>{Object.entries(labels).map(([id, label]) => <option value={id} key={id}>{label}</option>)}</select></label>
        <label>Host<input aria-label="Database host" required maxLength={128} value={host} onChange={e => setHost(e.target.value)} placeholder="reporting.example.com"/></label>
        <label>Port<input aria-label="Database port" required type="number" min={1} max={65535} value={port} onChange={e => setPort(Number(e.target.value))}/></label>
        <label>Database<input aria-label="Database name" required maxLength={128} value={database} onChange={e => setDatabase(e.target.value)}/></label>
        {engine !== 'mysql' && <label>Schema<input aria-label="Database schema" required maxLength={128} value={schema} onChange={e => setSchema(e.target.value)}/></label>}
        <label>Read-only username<input aria-label="Database username" autoComplete="off" required maxLength={128} value={username} onChange={e => setUsername(e.target.value)}/></label>
        <label>Password<input aria-label="Database password" type="password" autoComplete="new-password" required maxLength={1024} value={password} onChange={e => setPassword(e.target.value)}/></label>
      </div><label className="check-label"><input type="checkbox" checked={tls} onChange={e => setTls(e.target.checked)}/>Verify TLS certificate and hostname (required for remote databases)</label>
      {!tls && <p className="setup-help">Plaintext works only with an operator-enabled loopback test database.</p>}</> : <>
        <p>Connected to {labels[engine]}. Select columns to copy. Unselected columns stay in the source database. Include primary and foreign keys needed by your reporting relationships.</p>
        <div className="connection-tables">{tables.map(table => <details key={table.name} open={tables.length === 1}><summary>{table.name} · {(selection[table.name] || []).length} columns selected</summary><div className="mapping-dimensions">{table.columns.map(column => <label className="check-label" key={column.name}><input type="checkbox" aria-label={`Copy ${table.name}.${column.name}`} disabled={!column.supported || busy} checked={(selection[table.name] || []).includes(column.name)} onChange={e => setSelection(previous => ({...previous, [table.name]: e.target.checked ? [...(previous[table.name] || []), column.name] : (previous[table.name] || []).filter(c => c !== column.name)}))}/><span>{column.name}<small>{column.type}{column.primary_key ? ' · primary key' : ''}{!column.supported ? ' · unsupported' : ''}</small></span></label>)}</div></details>)}</div>
        <label>Refresh schedule<select aria-label="Initial refresh schedule" value={schedule} onChange={e => setSchedule(e.target.value as Schedule)}><option value="manual">Manual</option><option value="hourly">Every hour</option><option value="daily">Every day</option></select></label>
        <p className="setup-help">AIDA stores the selected data locally and encrypts connection credentials. The first snapshot must finish before you can approve its catalog. Scheduled refresh runs while this backend is running.</p>
      </>}
      <div className="builder-actions"><button type="button" className="text-button" disabled={busy} onClick={() => {setEditing(false); setPassword(''); setTables(null); setSelection({})}}>Cancel connection</button>{tables && <button type="button" className="text-button" disabled={busy} onClick={() => setTables(null)}>Edit connection details</button>}<button className="button primary" type="submit" disabled={busy || (!!tables && !Object.values(selection).some(columns => columns.length))}>{busy && <LoaderCircle size={15} className="spin"/>}{tables ? 'Create reporting snapshot' : 'Test connection and inspect'}</button></div>
    </form>}
    <div className="connection-list">{connections.map(connection => {
      const refreshing = ['queued', 'running'].includes(connection.status)
      return <article className="connection-card" key={connection.id}>
        <h3>{connection.name} <small>{labels[connection.engine]} · {connection.status}</small></h3>
        <p>Last successful refresh: {when(connection.last_success)}{connection.rows !== null ? ` · ${connection.rows.toLocaleString()} rows` : ''}</p>
        {connection.next_refresh && <p>Next refresh: {new Date(connection.next_refresh * 1000).toLocaleString()}</p>}
        {connection.error && <p className="setup-error" role="alert">{connection.error}</p>}
        <div className="connection-actions"><label>Refresh schedule<select aria-label={`Refresh schedule for ${connection.name}`} disabled={busy || refreshing || !available} value={connection.schedule} onChange={e => void action(connection.id, 'schedule', {schedule: e.target.value})}><option value="manual">Manual</option><option value="hourly">Every hour</option><option value="daily">Every day</option></select></label>
          <button className="button secondary" disabled={busy || refreshing || !available} onClick={() => void action(connection.id, 'refresh')}><RefreshCw size={14} className={refreshing ? 'spin' : ''}/>{refreshing ? 'Refreshing…' : 'Refresh now'}</button>
          {connection.source_id && <button className="button primary" disabled={busy} onClick={() => void run(async () => {const source = await getSource(connection.source_id!); if (source.configured) await onOpen(source.id); else onInspect(source)})}>Open snapshot / approve catalog</button>}
          <button className="text-button" disabled={busy || refreshing || !available} onClick={() => void action(connection.id, 'disconnect')}>Disconnect and remove credentials</button>
        </div><p className="setup-help">Disconnecting stops refreshes and keeps the last snapshot available for analysis.</p>
        {!!connection.history.length && <details><summary>Refresh history</summary>{connection.history.map((job, i) => <p key={i}>{when(job.finished_at)} · {job.status}{job.error ? ` · ${job.error}` : ''}</p>)}</details>}
      </article>
    })}</div>
    {error && <p className="setup-error" role="alert">{error}</p>}
  </section>
}
