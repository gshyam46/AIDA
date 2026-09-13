import raw from '../app/benchmarks/benchmark-data.json'

// Facts shown on the website come only from recorded runs (scripts/report_benchmark.py --web).
export type BenchmarkSummary = {
  cases: number; supported_cases: number; correct_supported: number; supported_accuracy: number | null
  refusal_cases: number; correct_refusals: number; refusal_accuracy: number | null
  overall_correct: number; overall_accuracy: number | null; wrong_or_unsafe_answers: number; false_refusals: number
  model_calls: number; repair_calls: number; tokens_per_question: number | null
  median_answer_ms_excluding_rate_limit_waits: number | null; p95_answer_ms_excluding_rate_limit_waits: number | null; estimated_cost_usd: number | null
}
export type BenchmarkRun = {
  label: string; model: string; pipeline: string; prompt_version: string | null; repair_attempts: number | null; status: string; started_at: string | null
  scored: number; not_run: number; summary: BenchmarkSummary; categories: Record<string, number>; by_suite: Record<string, BenchmarkSummary>
  plan_correct_answers: number; rows_correct_when_plan_correct: number; false_refusal_reasons: Record<string, number>
}
export type BenchmarkComparison = {runs: string[]; questions: number; expectation_changed: string[]; summaries: Record<string, BenchmarkSummary>}
export type BenchmarkQuestion = {key: string; suite: string; source: string; question: string; expected: string; outcomes: Record<string, string>}
export type BenchmarkData = {generated_at: string; source: string; runs: BenchmarkRun[]; before_after: BenchmarkComparison | null; prompt_pairs: BenchmarkComparison[]; questions: BenchmarkQuestion[]}

export const benchmark = raw as unknown as BenchmarkData

const MODEL_NAMES: Record<string, string> = {'openai/gpt-oss-120b': 'gpt-oss-120b', 'openai/gpt-oss-20b': 'gpt-oss-20b', 'qwen/qwen3.8-27b': 'Qwen3.8 27B', 'qwen/qwen3.6-27b': 'Qwen3.6 27B'}
export const modelName = (id: string) => MODEL_NAMES[id] || id.split('/').pop() || id
export const runName = (run: BenchmarkRun) => `${modelName(run.model)} · ${run.pipeline}`
export const pct = (value: number | null | undefined) => value == null ? '—' : `${(value * 100).toFixed(1)}%`
export const isAida4 = (run: BenchmarkRun) => !!run.prompt_version

export function keyFacts(data: BenchmarkData = benchmark) {
  const byLabel = new Map(data.runs.map(run => [run.label, run]))
  const complete = data.runs.filter(run => isAida4(run) && run.status === 'complete')
  const bestRun = [...complete].sort((a, b) => (b.summary.overall_accuracy ?? 0) - (a.summary.overall_accuracy ?? 0) || a.summary.wrong_or_unsafe_answers - b.summary.wrong_or_unsafe_answers)[0] ?? null
  let shared: {questions: number; before: BenchmarkRun; after: BenchmarkRun; beforeSummary: BenchmarkSummary; afterSummary: BenchmarkSummary} | null = null
  const comparison = data.before_after
  if (comparison) {
    const beforeLabel = comparison.runs.find(label => !byLabel.get(label)?.prompt_version)
    const afterLabel = comparison.runs.filter(label => byLabel.get(label)?.prompt_version)
      .sort((a, b) => (comparison.summaries[b].overall_accuracy ?? 0) - (comparison.summaries[a].overall_accuracy ?? 0))[0]
    if (beforeLabel && afterLabel) shared = {questions: comparison.questions, before: byLabel.get(beforeLabel)!, after: byLabel.get(afterLabel)!,
      beforeSummary: comparison.summaries[beforeLabel], afterSummary: comparison.summaries[afterLabel]}
  }
  const plan = data.runs.reduce((total, run) => total + run.plan_correct_answers, 0)
  const rows = data.runs.reduce((total, run) => total + run.rows_correct_when_plan_correct, 0)
  return {shared, bestRun, complete, plan, rows, runCount: data.runs.length}
}
