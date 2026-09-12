'use client'
import {useCallback, useEffect, useRef, useState} from 'react'
import {Activity, ArrowDownToLine, ArrowRight, ArrowUpRight, BarChart3, CalendarDays, Check, CheckCircle2, ChevronRight, Code2, Database, Filter, LayoutDashboard, LineChart, LoaderCircle, LockKeyhole, PanelLeftClose, Plus, RefreshCw, Search, ShieldCheck, SlidersHorizontal, Table2, Trash2, X, Zap} from 'lucide-react'
import QueryChart from '../components/QueryChart'
import SourceSetup from '../components/SourceSetup'
import RelationalBuilder from '../components/RelationalBuilder'
import QueryLineage from '../components/QueryLineage'
import AnswerContext from '../components/AnswerContext'
import {relationalChartTypes} from '../components/RelationalChart'
import {Catalog, executeQuery, formatValue, getCatalog, getSources, isRelational, labelFor, LegacyQueryPlan, newRelationalPlan, planDimensions, planMetrics, QueryPlan, QueryRequest, QueryResponse, RelationalQueryPlan, SourcesResponse} from '../lib/api'

type SavedCard = {id: string; source_id: string; catalog_version?: string; title: string; plan: QueryPlan; chartType: string}
type Tab = 'explorer' | 'dashboards' | 'data'
const STORAGE_KEY = 'aida:dashboard:v2'
const emptyPlan: LegacyQueryPlan = {metric: '', dimension: null, filters: {}, sort: 'value_desc', limit: 100}
const validSavedCard = (card: unknown): card is SavedCard => {
  if (!card || typeof card !== 'object') return false
  const item = card as SavedCard, plan = item.plan
  if (!(typeof item.id === 'string' && typeof item.source_id === 'string' && typeof item.title === 'string' && item.title.length <= 1500 && plan && ['bar', 'line', 'area', 'donut', 'scatter', 'stat', 'table'].includes(item.chartType))) return false
  if (isRelational(plan)) return Array.isArray(plan.metrics) && plan.metrics.length >= 1 && plan.metrics.length <= 3 && plan.metrics.every(id => typeof id === 'string') && Array.isArray(plan.dimensions) && plan.dimensions.length <= 2 && plan.dimensions.every(id => typeof id === 'string') && Array.isArray(plan.filters) && Array.isArray(plan.having) && !!plan.sort && typeof plan.sort.field === 'string' && ['asc', 'desc'].includes(plan.sort.direction) && Number.isInteger(plan.limit) && plan.limit >= 1 && plan.limit <= 100
  return typeof plan.metric === 'string' && (plan.dimension === null || typeof plan.dimension === 'string')
    && !!plan.filters && typeof plan.filters === 'object' && !Array.isArray(plan.filters)
    && Object.entries(plan.filters).every(([key, value]) => key.length <= 100 && typeof value === 'string')
    && ['value_desc', 'value_asc', 'dimension_asc'].includes(plan.sort) && Number.isInteger(plan.limit) && plan.limit >= 1 && plan.limit <= 100
    && (!plan.date_from || typeof plan.date_from === 'string') && (!plan.date_to || typeof plan.date_to === 'string')
    && ['bar', 'line', 'stat'].includes(item.chartType)
}
function defaultPlan(catalog: Catalog): QueryPlan {
  if (catalog.capabilities?.relational) return newRelationalPlan(catalog)
  const dimension = catalog.dimensions.find(item => item.id === 'month')?.id || catalog.dimensions[0]?.id || null
  return {...emptyPlan, metric: catalog.metrics[0]?.id || '', dimension, sort: dimension === 'month' ? 'dimension_asc' : 'value_desc'}
}
function planTitle(plan: QueryPlan) {return `${planMetrics(plan).map(labelFor).join(' and ')}${planDimensions(plan).length ? ` by ${planDimensions(plan).map(labelFor).join(' and ')}` : ''}${isRelational(plan) ? plan.filters.length ? ` · ${plan.filters.map(filter => `${labelFor(filter.field)} ${filter.op} ${filter.value}`).join(', ')}` : '' : Object.keys(plan.filters).length ? ` · ${Object.values(plan.filters).join(', ')}` : ''}`}
function dateRange(catalog: Catalog | null) {return catalog?.dataset.date_from && catalog.dataset.date_to ? `${catalog.dataset.date_from} → ${catalog.dataset.date_to}` : 'All available records'}
function sourceLabel(value?: string) {return value ? labelFor(value) : 'Not reported'}
function QueryTelemetry({result}: {result: QueryResponse}) {
  const meta = result.meta, inputTokens = meta.prompt_tokens ?? meta.usage?.prompt_tokens, outputTokens = meta.completion_tokens ?? meta.usage?.completion_tokens
  const tokens = meta.total_tokens ?? meta.usage?.total_tokens
  return <div className="trust-metrics telemetry">
    <div><ShieldCheck size={16}/><span>Execution<strong>{result.success ? 'Validated, read-only SQL' : 'No result returned'}</strong></span></div>
    <div><Zap size={16}/><span>Interpretation<strong>{sourceLabel(meta.interpretation_source)}</strong></span></div>
    <div><Activity size={16}/><span>Model calls this request<strong>{meta.model_calls ?? 'Not reported'}</strong></span></div>
    <div><Database size={16}/><span>Model<strong>{meta.model || meta.model_name || (meta.model_calls === 0 ? 'Not invoked' : 'Not reported')}</strong></span></div>
    <div><Code2 size={16}/><span>Tokens this request<strong>{tokens ?? ((inputTokens !== undefined || outputTokens !== undefined) ? (inputTokens || 0) + (outputTokens || 0) : 'Not reported')}</strong>{inputTokens !== undefined && <small>{inputTokens} input · {outputTokens ?? '—'} output</small>}</span></div>
    <div><RefreshCw size={16}/><span>Cache<strong>{meta.interpretation_cache_hit ? 'Reused validated interpretation' : meta.cache_hit ? 'Reused result' : 'No cache hit reported'}</strong></span></div>
  </div>
}
export default function Home() {
  const requestSequence = useRef(0)
  const [tab, setTab] = useState<Tab>('explorer')
  const [sources, setSources] = useState<SourcesResponse | null>(null)
  const [sourceId, setSourceId] = useState('')
  const [catalog, setCatalog] = useState<Catalog | null>(null)
  const [bootError, setBootError] = useState('')
  const [booting, setBooting] = useState(true)
  const [question, setQuestion] = useState('')
  const [mode, setMode] = useState<'question' | 'builder'>('question')
  const [builder, setBuilder] = useState<QueryPlan>(emptyPlan)
  const legacyBuilder = isRelational(builder) ? emptyPlan : builder
  const [result, setResult] = useState<QueryResponse | null>(null)
  const [resultTitle, setResultTitle] = useState('')
  const [busy, setBusy] = useState(false)
  const [interpreting, setInterpreting] = useState(false)
  const [queryError, setQueryError] = useState<QueryResponse | null>(null)
  const [lastRequest, setLastRequest] = useState<QueryRequest | null>(null)
  const [resultTab, setResultTab] = useState<'chart' | 'table' | 'sql'>('chart')
  const [chartType, setChartType] = useState('bar')
  const [kpis, setKpis] = useState<Record<string, number | null>>({})
  const [cards, setCards] = useState<SavedCard[]>([])
  const [cardResults, setCardResults] = useState<Record<string, QueryResponse>>({})
  const [cardsBusy, setCardsBusy] = useState(false)
  const [toast, setToast] = useState('')
  const [sidebarOpen, setSidebarOpen] = useState(false)
  const activeSource = sources?.sources.find(item => item.id === sourceId)
  const activeCards = cards.filter(card => card.source_id === sourceId)
  const loadSource = useCallback(async (id: string, list: SourcesResponse) => {
    const sequence = ++requestSequence.current
    setBooting(true); setBootError(''); setSourceId(id); setCatalog(null); setResult(null); setQueryError(null); setKpis({}); setBusy(false); setQuestion(''); setBuilder(emptyPlan); setLastRequest(null)
    try {
      if (!list.sources.find(item => item.id === id)?.configured) {setTab('data'); return}
      const data = await getCatalog(id)
      if (sequence !== requestSequence.current) return
      setCatalog(data)
      const plan = defaultPlan(data)
      setBuilder(plan); setQuestion(data.examples[0] || planTitle(plan)); setResultTitle(`${planTitle(plan)} · catalog preview`)
      const queries = await Promise.all([executeQuery({source_id: id, catalog_version: data.dataset.catalog_version, plan}), ...data.metrics.slice(0, 3).map(metric => executeQuery({source_id: id, catalog_version: data.dataset.catalog_version, plan: data.capabilities?.relational ? {...newRelationalPlan(data), metrics: [metric.id], dimensions: [], sort: {field: metric.id, direction: 'desc'}} : {...emptyPlan, metric: metric.id}}))])
      if (sequence !== requestSequence.current) return
      if (!queries[0].success) throw new Error(queries[0].error || 'Could not load the opening analysis.')
      setResult(queries[0]); setResultTab('chart'); setChartType(queries[0].chart?.type || 'bar')
      setKpis(Object.fromEntries(queries.slice(1).filter(response => response.success && response.data.length && response.plan).map(response => {const metricId = planMetrics(response.plan)[0], value = response.data[0][isRelational(response.plan) ? metricId : 'value']; return [metricId, value == null ? null : Number(value)]})))
    } catch (error) {if (sequence === requestSequence.current) setBootError(error instanceof Error ? error.message : 'The data service could not be reached.')}
    finally {if (sequence === requestSequence.current) setBooting(false)}
  }, [])
  const boot = useCallback(async (preferredId?: string) => {
    setBooting(true); setBootError('')
    try {
      const list = await getSources(); setSources(list)
      const id = preferredId && list.sources.some(item => item.id === preferredId) ? preferredId : list.default_source_id || list.sources[0]?.id
      if (!id) throw new Error('No data sources are configured.')
      await loadSource(id, list)
    } catch (error) {setBootError(error instanceof Error ? error.message : 'Could not load data sources.'); setBooting(false)}
  }, [loadSource])
  useEffect(() => {
    void boot()
    try {const saved = JSON.parse(localStorage.getItem(STORAGE_KEY) || '[]'); if (Array.isArray(saved)) setCards(saved.filter(validSavedCard).slice(0, 48))}
    catch {setToast('Saved dashboard could not be read in this browser.')}
  }, [boot])
  useEffect(() => {if (toast) {const timer = setTimeout(() => setToast(''), 4500); return () => clearTimeout(timer)}}, [toast])
  const refreshModel = async () => {try {setSources(await getSources())} catch (error) {setToast(error instanceof Error ? error.message : 'Could not check model status.')}}
  const run = async (input: QueryRequest, title?: string) => {
    if (booting || !catalog || !sourceId) return
    const sequence = ++requestSequence.current, request = {...input, source_id: sourceId, catalog_version: catalog.dataset.catalog_version}
    setBusy(true); setInterpreting(!!input.question); setQueryError(null); setResult(null); setLastRequest(request)
    setResultTitle(title || input.question || (input.plan ? planTitle(input.plan) : 'Analysis'))
    if (input.question) setQuestion(input.question)
    try {
      const response = await executeQuery(request)
      if (sequence !== requestSequence.current) return
      if (!response.success) {setQueryError(response); return}
      setResult(response); setResultTab('chart'); setChartType(response.chart?.type || 'bar')
      if (response.plan) setBuilder(response.plan)
    } catch (error) {
      if (sequence !== requestSequence.current) return
      setQueryError({success: false, data: [], meta: {}, error: error instanceof Error ? error.message : 'This query could not run.'})
    } finally {if (sequence === requestSequence.current) {setBusy(false); if (input.question) void refreshModel()}}
  }
  const persistCards = (next: SavedCard[]) => {
    try {localStorage.setItem(STORAGE_KEY, JSON.stringify(next)); setCards(next); return true}
    catch {setToast('Browser storage is unavailable. Your dashboard could not be saved.'); return false}
  }
  const saveCard = () => {
    if (!result?.plan) return
    if (activeCards.some(card => JSON.stringify(card.plan) === JSON.stringify(result.plan))) {setToast('This analysis is already on this source’s dashboard.'); return}
    if (activeCards.length >= 12) {setToast('A source dashboard holds 12 charts. Remove one to add another.'); return}
    const card = {id: crypto.randomUUID(), source_id: sourceId, catalog_version: catalog?.dataset.catalog_version, title: resultTitle, plan: result.plan, chartType}
    if (persistCards([...cards, card])) {setCardResults(previous => ({...previous, [card.id]: result})); setToast('Saved to this source’s dashboard.')}
  }
  const refreshCards = useCallback(async (items: SavedCard[]) => {
    setCardsBusy(true)
    const responses = await Promise.all(items.map(async card => {
      try {
        const current = await getCatalog(card.source_id)
        if (card.catalog_version !== current.dataset.catalog_version) throw new Error('The business definitions changed. Recreate this chart with the current catalog.')
        return [card.id, await executeQuery({source_id: card.source_id, catalog_version: card.catalog_version, plan: card.plan})] as const
      }
      catch (error) {return [card.id, {success: false, data: [], meta: {}, error: error instanceof Error ? error.message : 'Could not refresh this card.'} as QueryResponse] as const}
    }))
    setCardResults(previous => ({...previous, ...Object.fromEntries(responses)})); setCardsBusy(false)
  }, [])
  useEffect(() => {if (tab === 'dashboards' && catalog) void refreshCards(cards.filter(card => card.source_id === sourceId))}, [tab, cards, sourceId, catalog, refreshCards])
  const navigate = (next: Tab) => {setTab(next); setSidebarOpen(false)}
  const setFilter = (key: string, value: string) => setBuilder(previous => {if (isRelational(previous)) return previous; const filters = {...previous.filters}; if (value) filters[key] = value; else delete filters[key]; return {...previous, filters}})
  const drill = (dimension: string, value: string) => {
    if (!result?.plan || busy) return
    if (isRelational(result.plan)) {
      if (dimension === 'month') return
      const dimensions = result.plan.dimensions.filter(id => id !== dimension)
      const filteredValue = catalog?.dimensions.find(item => item.id === dimension)?.type === 'number' ? Number(value) : value
      const plan: RelationalQueryPlan = {...result.plan, dimensions, filters: [...result.plan.filters.filter(filter => filter.field !== dimension), {field: dimension, op: 'eq', value: filteredValue}], comparison: dimensions.length ? result.plan.comparison : null, sort: {field: result.plan.metrics[0], direction: 'desc'}}
      setMode('builder'); void run({plan}, planTitle(plan)); return
    }
    const nextDimension = catalog?.dimensions.find(item => item.id === 'month' && item.id !== dimension)?.id || catalog?.dimensions.find(item => item.id !== dimension)?.id || null
    const plan = {...result.plan, dimension: nextDimension, filters: {...result.plan.filters, [dimension]: value}, sort: nextDimension === 'month' ? 'dimension_asc' as const : 'value_desc' as const}
    setMode('builder'); void run({plan}, planTitle(plan))
  }
  const exportCSV = () => {
    if (!result?.data.length) return
    const keys = Object.keys(result.data[0])
    const escape = (value: unknown) => {let cell = String(value ?? ''); if (/^[=+@\-\t\r]/.test(cell)) cell = `'${cell}`; return `"${cell.replace(/"/g, '""')}"`}
    const csv = [keys.map(escape).join(','), ...result.data.map(row => keys.map(key => escape(row[key])).join(','))].join('\r\n')
    const url = URL.createObjectURL(new Blob([csv], {type: 'text/csv;charset=utf-8;'}))
    const anchor = document.createElement('a'); anchor.href = url; anchor.download = 'aida-analysis.csv'; anchor.click(); URL.revokeObjectURL(url); setToast('Analysis exported as CSV.')
  }
  const metricIds = planMetrics(result?.plan), dimensionIds = planDimensions(result?.plan)
  const metric = catalog?.metrics.find(item => item.id === metricIds[0])
  const legacyResultPlan = result?.plan && !isRelational(result.plan) ? result.plan : undefined
  const elapsed = result?.meta.elapsed_ms ?? result?.meta.execution_time_ms
  const hasData = !!result?.data.length
  const modelAvailable = sources?.model?.available
  const dimensionFilters = catalog?.dimensions.filter(item => item.id !== 'month') || []
  const canDrill = !!dimensionIds[0] && dimensionFilters.some(item => item.id === dimensionIds[0] && (item.values === undefined || item.values.length > 0))
  const chartOptions = result && isRelational(result.plan) ? relationalChartTypes(result, catalog) : legacyResultPlan?.dimension ? ['bar', 'line'] : []
  const hasDates = !!catalog?.dataset.date_column || catalog?.dimensions.some(item => item.id === 'month')
  return <div className="app-shell">
    <a className="skip-link" href="#main">Skip to main content</a>
    {sidebarOpen && <button className="sidebar-backdrop" aria-label="Close navigation" onClick={() => setSidebarOpen(false)}/>}
    <aside className={`sidebar ${sidebarOpen ? 'is-open' : ''}`}>
      <a className="brand" href="/" aria-label="AIDA home"><span className="brand-mark"><i/><i/><i/></span><span>aida<span className="brand-dot">.</span></span></a>
      <div className="workspace-switch"><div className="workspace-icon"><Database size={17}/></div><label className="source-select">Data source<select aria-label="Data source" value={sourceId} disabled={booting} onChange={event => {if (sources) void loadSource(event.target.value, sources)}}>{sources?.sources.map(source => <option key={source.id} value={source.id}>{source.name}{source.configured ? '' : ' · needs mapping'}</option>)}</select></label></div>
      <div className="nav-caption">WORKSPACE</div>
      <nav aria-label="Main navigation"><button onClick={() => navigate('explorer')} className={tab === 'explorer' ? 'active' : ''}><Search size={18}/>Explorer<ArrowUpRight size={14} className="nav-shortcut"/></button><button onClick={() => navigate('dashboards')} className={tab === 'dashboards' ? 'active' : ''}><LayoutDashboard size={18}/>Dashboards{activeCards.length > 0 && <span className="nav-badge">{activeCards.length}</span>}</button><button onClick={() => navigate('data')} className={tab === 'data' ? 'active' : ''}><Database size={18}/>Data catalog</button></nav>
      <div className="sidebar-bottom"><div className="private-card"><ShieldCheck size={22}/><strong>Language understands.<br/>Code verifies.</strong><p>A local model interprets questions. Approved definitions and read-only SQL control each answer.</p><span><span className="status-dot"/>Customer rows stay out of model prompts</span></div><div className="profile"><div>AI</div><span><strong>{sources?.uploads_enabled ? 'Local workspace' : 'Demo workspace'}</strong><small>Private inference</small></span><LockKeyhole size={14}/></div></div>
    </aside>
    <div className="main-shell">
      <header className="topbar"><div className="breadcrumbs"><button className="mobile-menu icon-button" aria-label="Open navigation" onClick={() => setSidebarOpen(true)}><PanelLeftClose size={20}/></button><span>Workspace</span><ChevronRight size={13}/><strong>{tab === 'explorer' ? 'Explorer' : tab === 'dashboards' ? 'Dashboards' : 'Data catalog'}</strong></div><div className="topbar-right"><span className="demo-badge"><span/>{activeSource?.synthetic ? 'Synthetic demo' : 'Local database'}</span><span className="topbar-divider"/><button className="text-button engine-status" aria-label="Refresh model status" onClick={() => void refreshModel()}><Activity size={14}/>{modelAvailable === true ? 'Local model ready' : modelAvailable === false ? 'Model unavailable' : 'Model status unknown'}</button></div></header>
      <main id="main" className="main-content">
        <div className="page-heading"><div><div className="eyebrow">{tab === 'explorer' ? 'YOUR BUSINESS, IN FOCUS' : tab === 'dashboards' ? 'THE BIGGER PICTURE' : 'DEFINE WHAT THE NUMBERS MEAN'}</div><h1>{tab === 'explorer' ? 'A clear view of your business.' : tab === 'dashboards' ? 'Your dashboard.' : 'Know what you’re querying.'}</h1><p>{tab === 'explorer' ? 'Ask naturally. Inspect the interpretation. Explore the results.' : tab === 'dashboards' ? 'Saved plans run directly against their original data source.' : 'Approve your schema and business definitions before asking questions.'}</p></div><div className="dataset-date"><CalendarDays size={15}/><span>{dateRange(catalog)}</span></div></div>
        {bootError && <div className="connection-error" role="alert"><Activity size={19}/><div><strong>We couldn’t connect to your data</strong><p>{bootError}</p></div><button className="button secondary" onClick={() => void boot(sourceId)} disabled={booting}><RefreshCw size={14}/>Retry connection</button></div>}
        {modelAvailable === false && <div className="connection-error" role="status"><Activity size={19}/><div><strong>The local language model is unavailable</strong><p>{sources?.model?.error || 'Natural-language questions need the local model service. Start it, then retry. The visual builder can execute an explicit plan independently.'}</p></div><button className="button secondary" onClick={() => void refreshModel()}><RefreshCw size={14}/>Check model</button></div>}
        {tab === 'explorer' && <>
          <div className="kpi-grid" aria-label="Dataset overview">{catalog?.metrics.slice(0, 3).map((item, index) => <article className="kpi-card" key={item.id}><div className="kpi-top"><span>{item.label}</span><span className={`kpi-icon tone-${index}`}><BarChart3 size={16}/></span></div><strong data-testid="metric-value">{kpis[item.id] == null ? '—' : formatValue(kpis[item.id]!, item.id, false, catalog)}</strong><div className="kpi-caption"><span className="tiny-dot"/>All available records<span>{item.format === 'currency' ? catalog.dataset.currency : 'Database aggregate'}</span></div></article>)}</div>
          <section className="query-panel panel" aria-labelledby="query-heading"><div className="query-panel-heading"><h2 id="query-heading"><span className="mint-icon"><Search size={17}/></span>What would you like to understand?</h2><span className="quiet-badge"><Zap size={12}/>Local interpretation · deterministic execution</span></div>
            <div className="query-mode" role="tablist" aria-label="Query input mode"><button role="tab" aria-selected={mode === 'question'} onClick={() => setMode('question')}><Search size={14}/>Ask a question</button><button role="tab" aria-selected={mode === 'builder'} onClick={() => setMode('builder')}><SlidersHorizontal size={14}/>Visual builder</button></div>
            {mode === 'question' ? <form onSubmit={event => {event.preventDefault(); if (question.trim() && !busy) void run({question: question.trim()})}}><div className="question-input"><textarea aria-label="Ask a question" maxLength={1500} value={question} onChange={event => setQuestion(event.target.value)} onKeyDown={event => {if ((event.ctrlKey || event.metaKey) && event.key === 'Enter' && question.trim() && !busy) {event.preventDefault(); void run({question: question.trim()})}}} placeholder={catalog?.examples[0] || 'Ask about a measure and dimension in your catalog'} rows={2}/><button className="button primary" type="submit" disabled={busy || booting || !catalog || !question.trim()}>{busy ? <LoaderCircle className="spin" size={16}/> : <ArrowRight size={16}/>}Run query</button></div><div className="query-input-hint"><span>One bounded local-model interpretation. Ambiguous requests ask for clarification.</span><kbd>Ctrl ↵</kbd></div></form> : catalog?.capabilities?.relational ? <RelationalBuilder catalog={catalog} plan={isRelational(builder) ? builder : newRelationalPlan(catalog)} onChange={setBuilder} onRun={() => void run({plan: builder})} busy={busy || booting}/> : <form className="builder" onSubmit={event => {event.preventDefault(); void run({plan: builder})}}>
              <div className="builder-grid"><label>Metric<select aria-label="Metric" value={legacyBuilder.metric} onChange={event => setBuilder({...legacyBuilder, metric: event.target.value})}>{catalog?.metrics.map(item => <option key={item.id} value={item.id}>{item.label}</option>)}</select></label><label>Group by<select aria-label="Group by" value={legacyBuilder.dimension || ''} onChange={event => setBuilder({...legacyBuilder, dimension: event.target.value || null, sort: event.target.value === 'month' ? 'dimension_asc' : 'value_desc'})}><option value="">Total only</option>{catalog?.dimensions.map(item => <option key={item.id} value={item.id}>{item.label}</option>)}</select></label>
                {dimensionFilters.map(item => <label key={item.id}>{item.label}{item.values?.length ? <select aria-label={item.label} value={legacyBuilder.filters[item.id] || ''} onChange={event => setFilter(item.id, event.target.value)}><option value="">All values</option>{item.values.map(value => <option key={value} value={value}>{value}</option>)}</select> : <input aria-label={item.label} value={legacyBuilder.filters[item.id] || ''} onChange={event => setFilter(item.id, event.target.value)} placeholder="Exact value (kept local)"/>}</label>)}
                {hasDates && <><label>From<input aria-label="From" type="date" value={legacyBuilder.date_from || ''} onChange={event => setBuilder({...legacyBuilder, date_from: event.target.value || null})}/></label><label>To<input aria-label="To" type="date" value={legacyBuilder.date_to || ''} onChange={event => setBuilder({...legacyBuilder, date_to: event.target.value || null})}/></label></>}
                <label>Sort by<select aria-label="Sort by" value={legacyBuilder.sort} onChange={event => setBuilder({...legacyBuilder, sort: event.target.value as LegacyQueryPlan['sort']})}><option value="value_desc">Highest value first</option><option value="value_asc">Lowest value first</option><option value="dimension_asc">Group ascending</option></select></label><label>Result limit<select aria-label="Result limit" value={legacyBuilder.limit} onChange={event => setBuilder({...legacyBuilder, limit: Number(event.target.value)})}>{Array.from(new Set([legacyBuilder.limit, 5, 10, 12, 25, 50, 100])).sort((a, b) => a - b).map(limit => <option key={limit} value={limit}>{limit} results</option>)}</select></label>
              </div><div className="builder-actions"><button className="text-button" type="button" onClick={() => {if (catalog) setBuilder(defaultPlan(catalog))}}>Reset filters</button><button className="button primary" type="submit" disabled={busy || booting || !catalog}>{busy ? <LoaderCircle size={15} className="spin"/> : <ArrowRight size={15}/>}Run analysis</button></div>
            </form>}
            <div className="examples"><span>TRY ASKING</span>{catalog?.examples.slice(0, 4).map(example => <button key={example} disabled={busy || booting} onClick={() => {setMode('question'); void run({question: example})}}>{example}<ArrowUpRight size={12}/></button>)}</div>
          </section>
          {busy || booting ? <div className="loading-panel panel" role="status"><LoaderCircle className="spin" size={25}/><strong>{booting ? 'Loading your source and catalog…' : interpreting ? 'Interpreting your question locally…' : 'Executing your validated plan…'}</strong><span>{booting ? 'Opening a catalog-defined preview' : interpreting ? 'The small model reads approved definitions. Code validates and compiles the plan. CPU inference can take a moment.' : 'Direct plan execution · no language interpretation needed'}</span></div> : queryError ? <section className="query-error panel" data-testid="query-error" role="alert"><span className="error-icon"><Search size={22}/></span><h2>{queryError.error_type === 'clarification_required' ? 'Let’s make that a little more specific.' : 'This query couldn’t be completed.'}</h2><p>{queryError.error}</p><div className="error-suggestions">{queryError.suggestions?.slice(0, 4).map(suggestion => <button key={suggestion} className="button secondary" onClick={() => {setMode('question'); void run({question: suggestion})}}>{suggestion}<ArrowRight size={13}/></button>)}</div><div className="error-suggestions">{lastRequest && <button className="button secondary" onClick={() => void run(lastRequest)}><RefreshCw size={13}/>Retry query</button>}<button className="text-button" onClick={() => setMode('builder')}>Choose exact fields in the visual builder<ArrowRight size={13}/></button></div>{queryError.meta.model_calls !== undefined && <QueryTelemetry result={queryError}/>}</section> : result && <div className="analysis-layout"><section className="result-panel panel" data-testid="result-panel" aria-label="Query result"><div className="result-heading"><div><div className="result-eyebrow"><span className="status-dot"/>QUERY COMPLETE<span>{elapsed === undefined ? '' : `${Number(elapsed).toFixed(1)} ms`}</span></div><h2>{resultTitle}</h2><p>{result.plan?.date_from || result.plan?.date_to ? `${result.plan.date_from || 'Beginning'} → ${result.plan.date_to || 'Latest'}` : dateRange(catalog)}<span>·</span>{result.data.length} {result.data.length === 1 ? 'result' : 'results'}</p></div><button className="button secondary save-button" onClick={saveCard} disabled={!hasData}><Plus size={15}/>Save to dashboard</button></div>
            <div className="result-toolbar"><div className="result-tabs" role="tablist" aria-label="Result view"><button role="tab" aria-selected={resultTab === 'chart'} onClick={() => setResultTab('chart')}><BarChart3 size={14}/>Chart</button><button role="tab" aria-selected={resultTab === 'table'} onClick={() => setResultTab('table')}><Table2 size={14}/>Table</button><button role="tab" aria-selected={resultTab === 'sql'} onClick={() => setResultTab('sql')}><Code2 size={14}/>SQL & trust</button></div>{resultTab === 'chart' && chartOptions.length > 1 && <div className="chart-type">{chartOptions.map(type => <button key={type} aria-label={`${labelFor(type)} chart`} aria-pressed={chartType === type} onClick={() => setChartType(type)}>{type === 'bar' ? <BarChart3 size={15}/> : type === 'line' ? <LineChart size={15}/> : <span>{labelFor(type)}</span>}</button>)}</div>}{resultTab === 'table' && <button className="text-button" disabled={!hasData} onClick={exportCSV}><ArrowDownToLine size={13}/>Export CSV</button>}</div>
            {resultTab === 'chart' ? <QueryChart key={`${sourceId}-${result.sql}-${JSON.stringify(result.parameters)}`} result={result} catalog={catalog} type={chartType} onSelect={canDrill ? drill : undefined}/> : resultTab === 'table' ? <div className="data-table-wrap"><table><caption className="sr-only">Results for {resultTitle}</caption><thead><tr>{result.data.length ? Object.keys(result.data[0]).map(key => <th key={key}>{key === 'value' ? metric?.label || 'Value' : catalog?.metrics.find(item => item.id === key)?.label || catalog?.dimensions.find(item => item.id === key)?.label || labelFor(key)}</th>) : <th>No results</th>}</tr></thead><tbody>{result.data.map((row, index) => <tr key={index}>{Object.entries(row).map(([key, value]) => <td key={key}>{value == null ? '—' : key === 'value' || metricIds.includes(key) ? formatValue(Number(value), key === 'value' ? metricIds[0] || '' : key, false, catalog) : value}</td>)}</tr>)}</tbody></table>{!hasData && <p className="empty-chart">No rows match these filters.</p>}</div> : <div className="sql-content"><QueryTelemetry result={result}/><QueryLineage result={result}/><p>{result.explanation || metric?.description}</p><label>COMPILED SQL</label><pre>{result.sql}</pre><label>BOUND PARAMETERS</label><pre className="parameters">{JSON.stringify(result.parameters ?? {}, null, 2)}</pre><details><summary>View the validated query plan</summary><pre className="parameters">{JSON.stringify(result.plan, null, 2)}</pre></details><details><summary>View execution metadata</summary><pre className="parameters">{JSON.stringify(result.meta, null, 2)}</pre></details><p className="trust-footnote">The local model interprets your question using approved metadata. Database result rows are never included in model prompts. Code validates fields and binds filters before executing read-only SQL.</p></div>}
            <div className="result-footer"><span><ShieldCheck size={13}/>{result.meta.model_calls ?? '—'} model calls this request</span><span><Database size={13}/>{activeSource?.name}</span><span>{sourceLabel(result.meta.interpretation_source)}{result.meta.cache_hit ? ' · cached result' : ''}</span>{metric?.format === 'currency' && <span>{catalog?.dataset.currency}</span>}</div>
          </section><AnswerContext result={result} catalog={catalog} canDrill={canDrill} busy={busy} onRun={plan => void run({plan})}/></div>}
        </>}
        {tab === 'dashboards' && <section className="dashboard-section"><div className="dashboard-top"><div><h2>{activeSource?.name || 'Source'} dashboard <span>{activeCards.length}</span></h2><p>Definitions are saved in this browser with their source. Refreshing a dashboard executes saved plans without model calls.</p></div><button className="button secondary" disabled={!activeCards.length || cardsBusy || booting} onClick={() => void refreshCards(activeCards)}><RefreshCw size={14} className={cardsBusy ? 'spin' : ''}/>Refresh dashboard</button></div>{!activeCards.length ? <div className="empty-dashboard panel"><span><LayoutDashboard size={32}/></span><h2>Make room for your best insights.</h2><p>Explore a question, then save its chart here.<br/>Each source has its own dashboard.</p><button className="button primary" onClick={() => navigate('explorer')}><Plus size={15}/>Build your first chart</button></div> : <div className="dashboard-grid">{activeCards.map(card => <article key={card.id} className="dashboard-card panel" data-testid="dashboard-card"><div className="dashboard-card-heading"><div><span>{planMetrics(card.plan).map(labelFor).join(' + ')} · {planDimensions(card.plan).map(labelFor).join(' + ') || 'Total'}</span><h3>{card.title}</h3></div><button className="icon-button" aria-label="Remove card" onClick={() => {if (persistCards(cards.filter(item => item.id !== card.id))) setToast('Chart removed from your dashboard.')}}><Trash2 size={15}/></button></div>{cardResults[card.id]?.success && catalog ? <QueryChart result={cardResults[card.id]} catalog={catalog} type={card.chartType} compact/> : cardResults[card.id]?.error ? <p className="dashboard-error">{cardResults[card.id].error}</p> : <div className="dashboard-loading"><LoaderCircle size={20} className="spin"/>Loading chart…</div>}<div className="dashboard-card-footer"><button className="text-button" disabled={booting} onClick={() => {if (card.catalog_version !== catalog?.dataset.catalog_version) {setToast('The catalog changed. Recreate this chart from the explorer.'); return} navigate('explorer'); setMode('builder'); void run({plan: card.plan}, card.title)}}>Open in explorer<ArrowUpRight size={13}/></button><button className="icon-button" aria-label="Refresh card" disabled={cardsBusy || booting} onClick={() => void refreshCards([card])}><RefreshCw size={14}/></button></div></article>)}</div>}</section>}
        {tab === 'data' && <section className="catalog-section"><div className="dataset-banner"><span><Database size={28}/></span><div><div className="eyebrow">CONNECTED DATASET</div><h2>{catalog?.dataset.name || activeSource?.name || 'Data source'}</h2><p>{catalog?.dataset.row_count !== undefined ? `${catalog.dataset.row_count.toLocaleString()} records · ` : ''}{dateRange(catalog)}{catalog?.dataset.currency ? ` · ${catalog.dataset.currency}` : ''}</p></div><span className="demo-badge">{activeSource?.synthetic ? 'Synthetic data' : 'Local database'}</span></div>
          <SourceSetup enabled={sources?.uploads_enabled === true} pendingSource={activeSource && !activeSource.configured ? activeSource : undefined} onConfigured={async id => {await boot(id); setTab('explorer'); setToast('Catalog saved. Ask a question about your database.')}}/>
          {catalog && <div className="catalog-grid"><section className="panel catalog-panel"><h2>Measures with explicit definitions</h2><p>Every question and dashboard uses the same approved business logic.</p>{catalog.metrics.map(item => <article className="catalog-metric" key={item.id}><div className="mint-icon"><BarChart3 size={16}/></div><div><h3>{item.label}</h3><p>{item.description}</p><code>{item.id}</code></div></article>)}</section><section className="panel catalog-panel"><h2>Ways to explore</h2><p>Approved dimensions and owner-supplied filter values.</p>{catalog.dimensions.map(item => <article className="catalog-dimension" key={item.id}><h3>{item.label}</h3><div>{item.values?.length ? item.values.map(value => <span key={value}>{value}</span>) : <span>{item.id === 'month' ? 'Calendar month' : 'Exact filters are validated locally'}</span>}</div></article>)}</section></div>}
          <div className="privacy-explainer"><ShieldCheck size={25}/><div><h3>Local language understanding. Controlled database access.</h3><p>The model receives your question and the approved semantic catalog on the AIDA server. Result rows stay in the database and application. Code validates the model’s structured intent and compiles read-only SQL. Local inference has no hosted API token fee; it uses your machine’s compute. Relational sources use approved table relationships, measures, and fields. The compiler selects the required joins and validates filters, subqueries, and combined record sets. Uploaded databases need an owner-approved catalog before they can be queried.</p></div></div>
        </section>}
        <footer className="page-footer"><span><span className="mini-brand">aida.</span>Clarity, without the complexity.</span><span>Local model interpretation · Validated SQL · Source-scoped dashboards</span></footer>
      </main>
    </div>
    {toast && <div className="toast" role="status"><Check size={16}/>{toast}<button aria-label="Dismiss notification" onClick={() => setToast('')}><X size={14}/></button></div>}
  </div>
}
