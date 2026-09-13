import {Catalog, isRelational, QueryResponse, RelationalQueryPlan} from './api'

/** Present calculated columns as chartable measures, labelled and formatted like approved metrics. */
export function presentationView(result: QueryResponse, catalog: Catalog | null): {result: QueryResponse; catalog: Catalog | null} {
  const calculations = result.calculations || []
  if (!calculations.length || !result.plan || !catalog) return {result, catalog}
  const label = (id: string) => catalog.metrics.find(item => item.id === id)?.label || id
  const calculated = calculations.map(item => ({id: item.id, label: item.label, format: item.format || 'number', additive: false,
    description: `Calculated in code after aggregation: ${item.op.replace(/_/g, ' ')} of ${item.inputs.map(label).join(' and ')}.`}))
  if (isRelational(result.plan)) {
    return {result: {...result, plan: {...result.plan, metrics: [...calculations.map(item => item.id), ...result.plan.metrics]}},
            catalog: {...catalog, metrics: [...calculated, ...catalog.metrics]}}
  }
  const base = catalog.metrics.find(item => item.id === (result.plan && !isRelational(result.plan) ? result.plan.metric : ''))
  const plan: RelationalQueryPlan = {version: 2, metrics: [...calculations.map(item => item.id), 'value'], dimensions: result.plan.dimension ? [result.plan.dimension] : [],
    filters: [], having: [], population: 'primary', set_operation: 'union_all', exists: null, comparison: null,
    sort: {field: 'value', direction: 'desc'}, limit: result.plan.limit}
  return {result: {...result, plan}, catalog: {...catalog, metrics: [...calculated, {id: 'value', label: base?.label || 'Value', format: base?.format || 'number', description: base?.description || '', additive: true}, ...catalog.metrics]}}
}

export function columnLabel(key: string, result: QueryResponse | null, catalog: Catalog | null) {
  const metricId = result?.plan && !isRelational(result.plan) && key === 'value' ? result.plan.metric : key
  return result?.calculations?.find(item => item.id === key)?.label
    || catalog?.metrics.find(item => item.id === metricId)?.label
    || catalog?.dimensions.find(item => item.id === key)?.label
    || key.replace(/_/g, ' ').replace(/^\w/, character => character.toUpperCase())
}
