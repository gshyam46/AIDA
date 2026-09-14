export type Metric = string
export type Dimension = string
export type CalculationOp = 'ratio' | 'difference' | 'share_of_total' | 'running_total' | 'percent_change'
export type Calculation = {id: string; label: string; op: CalculationOp; inputs: string[]; format?: string}
export type CalculationPost = {thresholds?: {target: string; op: string; value: number}[]; sort?: {by: string; direction: 'asc' | 'desc'}; limit?: number}
export type LegacyQueryPlan = {
  metric: Metric; dimension: Dimension | null; filters: Record<string, string>;
  date_from?: string | null; date_to?: string | null;
  sort: 'value_desc' | 'value_asc' | 'dimension_asc'; limit: number;
  calculations?: Calculation[]; post?: CalculationPost
}
export type RelationalFilter = {field: string; op: 'eq' | 'ne' | 'gt' | 'gte' | 'lt' | 'lte' | 'in'; value: string | number | (string | number)[]}
export type RelationalQueryPlan = {
  version: 2; metrics: string[]; dimensions: string[]; filters: RelationalFilter[];
  having: {metric: string; op: 'eq' | 'ne' | 'gt' | 'gte' | 'lt' | 'lte'; value: number}[];
  population: 'primary' | 'all'; set_operation: 'union_all' | 'union';
  exists: {relation: string; negate: boolean; filters: RelationalFilter[]} | null;
  comparison: {kind: 'above_average'; metric: string} | null;
  date_from?: string | null; date_to?: string | null;
  sort: {field: string; direction: 'asc' | 'desc'}; limit: number;
  calculations?: Calculation[]; post?: CalculationPost
}
export type QueryPlan = LegacyQueryPlan | RelationalQueryPlan
export function isRelational(plan?: QueryPlan | null): plan is RelationalQueryPlan {return !!plan && 'version' in plan && plan.version === 2}
export function planMetrics(plan?: QueryPlan) {return !plan ? [] : isRelational(plan) ? plan.metrics : [plan.metric]}
export function planDimensions(plan?: QueryPlan) {return !plan ? [] : isRelational(plan) ? plan.dimensions : plan.dimension ? [plan.dimension] : []}
export function newRelationalPlan(catalog: Catalog): RelationalQueryPlan {
  const metric = catalog.metrics[0]?.id || '', dimension = catalog.dimensions.find(item => item.id === 'month')?.id || catalog.dimensions[0]?.id
  return {version: 2, metrics: [metric], dimensions: dimension ? [dimension] : [], filters: [], having: [], population: 'primary', set_operation: 'union_all', exists: null, comparison: null, date_from: null, date_to: null, sort: {field: dimension === 'month' ? dimension : metric, direction: dimension === 'month' ? 'asc' : 'desc'}, limit: 100}
}
export function withRelationalDimensions(plan: RelationalQueryPlan, dimensions: string[]): RelationalQueryPlan {
  const sort = dimensions[0] === 'month' && plan.dimensions[0] !== 'month'
    ? {field: 'month', direction: 'asc' as const}
    : [...plan.metrics, ...dimensions].includes(plan.sort.field) ? plan.sort : {field: plan.metrics[0], direction: 'desc' as const}
  return {...plan, dimensions, sort, comparison: dimensions.length ? plan.comparison : null}
}
export type QueryRequest = {source_id?: string; catalog_version?: string; question?: string; plan?: QueryPlan}
export type QueryMeta = {
  model_calls?: number; guard_calls?: number; repair_calls?: number; model?: string; model_name?: string; provider?: string; pipeline?: string;
  prompt_tokens?: number; completion_tokens?: number; total_tokens?: number; model_latency_ms?: number;
  estimated_model_cost_usd?: number | null; estimated_cost_usd?: number | null; inference_location?: string;
  execution_time_ms?: number; elapsed_ms?: number; row_count?: number; synthetic?: boolean;
  snapshot_updated_at?: string | null; engine?: string; execution_id?: string; cache_hit?: boolean; source_id?: string;
  interpretation_source?: string; interpretation_cache_hit?: boolean; interpretation_time_ms?: number;
  usage?: {prompt_tokens?: number; completion_tokens?: number; total_tokens?: number};
}
export type InterpretationMention = {text: string; role: string; label: string | null; how: 'exact' | 'synonym' | 'inferred'}
export type QueryResponse = {
  success: boolean; data: Record<string, string | number | null>[];
  plan?: QueryPlan; sql?: string; parameters?: unknown;
  chart?: {type: string; x: string | null; y: string; series?: string[]; formats?: Record<string, string>; additive?: Record<string, boolean>; format?: string; currency?: string; calculations?: string[]};
  columns?: (string | {name: string; type?: string; label?: string; role?: string})[];
  lineage?: {tables: string[]; columns: {table: string; column: string; role: string; semantic_id?: string}[]; joins: {from: string; from_column: string; to: string; to_column: string; kind: string}[]; operations: string[]};
  calculations?: Calculation[];
  interpretation?: {notes: string[]; mentions: InterpretationMention[]};
  meta: QueryMeta; error?: string; error_type?: string; clarification_reason?: string; suggestions?: string[]; explanation?: string; retry_after?: number
}
export type Catalog = {
  source_id?: string;
  metrics: {id: Metric; label: string; format: string; description: string; additive?: boolean; aggregate?: string}[];
  dimensions: CatalogField[];
  fields?: CatalogField[];
  capabilities?: {relational?: boolean; [key: string]: unknown};
  exists_relations?: {id: string; label: string; fields: CatalogField[]}[];
  populations?: {id: 'primary' | 'all'; label: string; description?: string}[];
  relational?: Record<string, unknown>;
  examples: string[];
  dataset: {name: string; synthetic: boolean; catalog_version?: string; date_from?: string | null; date_to?: string | null; as_of?: string | null; currency?: string; row_count?: number; date_column?: string | null}
}
export type CatalogField = {id: Dimension; label: string; type?: 'string' | 'number' | 'date'; values?: string[]}
export type Source = {id: string; name: string; configured: boolean; synthetic: boolean}
export type ModelStatus = {available?: boolean; model?: string; model_name?: string; provider?: string; status?: string; error?: string; inference_location?: string; pipeline?: string; guard_model?: string | null; data_sent?: string}
export type SourcesResponse = {sources: Source[]; default_source_id?: string; uploads_enabled?: boolean; mode?: string; model?: ModelStatus}
export type SourceColumn = {name: string; type: string; numeric: boolean; sensitive: boolean; primary_key: boolean}
export type SourceInspection = Source & {tables: {name: string; columns: SourceColumn[]; unique_keys?: string[][]; foreign_keys?: {from: string; table: string; to: string}[]}[]}
export type SourceMapping = {
  name: string; table: string;
  metrics: {id: string; label: string; description: string; aggregate: 'COUNT' | 'SUM' | 'AVG' | 'MIN' | 'MAX'; column?: string; format: 'number' | 'currency'; scale?: number}[];
  dimensions: {id: string; label: string; column: string; values?: string[]}[];
  date_column?: string; as_of?: string; currency?: string;
}
export type SessionUser = {id: string; email: string; name: string; role: 'owner' | 'member'; created_at: number}
export type Onboarding = {company: string; role_title: string; team_size: string; use_cases: string[]; primary_goal?: string | null; data_choice: 'demo' | 'sample_logistics' | 'upload_later'; hosted_inference_consent: boolean; completed_at?: number}
export type SessionState = {auth_required: boolean; signup_enabled: boolean; user: SessionUser | null; onboarding: Onboarding | null; hosted_inference: boolean}

export class ApiError extends Error {
  constructor(message: string, readonly status: number, readonly body: Record<string, unknown> | null) {super(message)}
}

export async function request(path: string, init?: RequestInit) {
  const response = await fetch(`/api/v1/${path}`, {
    ...init, credentials: 'same-origin',
    headers: {'Content-Type': 'application/json', 'X-AIDA-Request': '1', ...init?.headers},
    signal: AbortSignal.timeout(120000), cache: 'no-store',
  })
  const body = await response.json().catch(() => null)
  if (!response.ok) {
    const detail = typeof body?.detail === 'string' ? body.detail : body?.error
    throw new ApiError(detail || `The data service could not complete this request (${response.status}). Please try again.`, response.status, body)
  }
  return body
}
export async function getSources(): Promise<SourcesResponse> {return request('sources')}
export async function getSource(id: string): Promise<SourceInspection> {return request(`sources/${encodeURIComponent(id)}`)}
export async function uploadSource(file: File): Promise<SourceInspection> {
  return request('sources', {method: 'POST', headers: {'Content-Type': 'application/octet-stream', 'X-Source-Name': encodeURIComponent(file.name)}, body: file})
}
export async function configureSource(id: string, mapping: SourceMapping | Record<string, unknown>): Promise<Catalog> {
  return request(`sources/${encodeURIComponent(id)}/configure`, {method: 'POST', body: JSON.stringify(mapping)})
}
export async function getCatalog(sourceId: string): Promise<Catalog> {return request(`catalog?source_id=${encodeURIComponent(sourceId)}`)}
export async function executeQuery(input: QueryRequest): Promise<QueryResponse> {
  try {
    const body = await request('query', {method: 'POST', body: JSON.stringify(input)})
    return {...body, data: body.data || body.results || [], meta: body.meta || body.metadata || {}}
  } catch (error) {
    // Rate limits and paused questions are answers the interface should explain, not crashes.
    if (error instanceof ApiError && error.status === 429 && error.body) return {success: false, data: [], meta: {}, ...(error.body as Partial<QueryResponse>), error: error.message}
    throw error
  }
}
export async function getSession(): Promise<SessionState> {return request('auth/session')}
export async function signUp(input: {name: string; email: string; password: string}): Promise<SessionState> {return request('auth/signup', {method: 'POST', body: JSON.stringify(input)})}
export async function signIn(input: {email: string; password: string}): Promise<SessionState> {return request('auth/login', {method: 'POST', body: JSON.stringify(input)})}
export async function signOut(): Promise<void> {await request('auth/logout', {method: 'POST', body: '{}'})}
export async function saveOnboarding(input: Omit<Onboarding, 'completed_at'>): Promise<{onboarding: Onboarding; sample_source: {id: string; name: string; created: boolean} | null}> {
  return request('onboarding', {method: 'POST', body: JSON.stringify(input)})
}
export function formatValue(value: number, metric: string, compact = false, catalog?: Catalog | null) {
  const definition = catalog?.metrics.find(item => item.id === metric)
  if (definition?.format === 'percent') return new Intl.NumberFormat('en-US', {style: 'percent', maximumFractionDigits: compact ? 0 : 1}).format(value)
  const currency = definition?.format === 'currency' && catalog?.dataset.currency
  return new Intl.NumberFormat('en-US', {
    style: currency ? 'currency' : 'decimal', ...(currency ? {currency} : {}),
    notation: compact ? 'compact' : 'standard', maximumFractionDigits: compact ? 1 : 2,
  }).format(value)
}
export function labelFor(value: string) {return value.replace(/_/g, ' ').replace(/^\w/, c => c.toUpperCase())}
// True only when the data service answers its health check, so account pages can be offered.
export async function checkBackend(timeoutMs = 5000): Promise<boolean> {
  try {
    const response = await fetch('/api/v1/health', {cache: 'no-store', credentials: 'same-origin', signal: AbortSignal.timeout(timeoutMs)})
    if (!response.ok) return false
    const body = await response.json().catch(() => null)
    return body?.status === 'healthy'
  } catch {
    return false
  }
}
// Network failures, timeouts and 5xx responses mean the service is unavailable, not that the input was wrong.
export function isServiceUnavailable(error: unknown) {
  return !(error instanceof ApiError) || error.status >= 500
}
