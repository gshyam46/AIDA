'use client'
import {useState} from 'react'
import {ArrowLeft, ArrowRight} from 'lucide-react'
import DataTable from '../../components/DataTable'
import {benchmark, BenchmarkRun, BenchmarkSummary, isAida4, keyFacts, modelName, pct, runName} from '../../lib/benchmarks'
import {PREVIEW} from '../../lib/mode'

const SUITE_LABELS: Record<string, string> = {logistics: 'Logistics regression', relational: 'Relational demos', semantic: 'Semantic regression', capabilities: 'Calculations', resolution: 'Name resolution'}
const REASON_LABELS: Record<string, string> = {code_rejected: 'Reply rejected by code checks', unsupported: 'Model judged it unsupported', ambiguous: 'Model asked to disambiguate',
  vague: 'Model called it too vague', unknown_term: 'Model did not recognise a name', prompt_injection: 'Flagged as an instruction attack', sensitive_data: 'Flagged as restricted data', not_recorded: 'Reason not recorded'}
const REASON_TONES = ['warn', 'amber', 'sand', 'clay', 'olive', 'muted']
const OUTCOME_LABELS: Record<string, string> = {correct_answer: 'Correct', correct_refusal: 'Correct refusal', false_refusal: 'False refusal', wrong_accepted_meaning: 'Wrong meaning',
  execution_mismatch: 'Wrong rows', unsafe_acceptance: 'Unsafe answer', execution_failure: 'Error', availability_failure: 'Not run (quota)'}
const STATUS_LABELS: Record<string, string> = {complete: 'Complete', stopped_quota: 'Partial · stopped by provider quota', 'in progress': 'Partial'}
type Metric = {id: string; label: string; direction: string; value: (summary: BenchmarkSummary) => number | null; format: (value: number) => string; ratio?: boolean}
const METRICS: Metric[] = [
  {id: 'supported', label: 'Supported questions answered correctly', direction: 'Higher is better', value: s => s.supported_accuracy, format: v => pct(v), ratio: true},
  {id: 'refusals', label: 'Requests correctly refused or clarified', direction: 'Higher is better', value: s => s.refusal_accuracy, format: v => pct(v), ratio: true},
  {id: 'wrong', label: 'Wrong or unsafe answers', direction: 'Lower is better', value: s => s.wrong_or_unsafe_answers, format: v => String(v)},
  {id: 'false', label: 'False refusals', direction: 'Lower is better', value: s => s.false_refusals, format: v => String(v)},
  {id: 'tokens', label: 'Tokens per question', direction: 'Lower is cheaper', value: s => s.tokens_per_question, format: v => Math.round(v).toLocaleString('en-US')},
  {id: 'latency', label: 'Median answer time, excluding provider rate-limit waits', direction: 'Lower is faster', value: s => s.median_answer_ms_excluding_rate_limit_waits, format: v => `${(v / 1000).toFixed(2)} s`},
  {id: 'cost', label: 'Estimated cost per 100 questions at list price', direction: 'Lower is cheaper', value: s => s.estimated_cost_usd == null || !s.cases ? null : s.estimated_cost_usd / s.cases * 100, format: v => `$${v.toFixed(3)}`},
]
type Part = {label: string; value: number; tone: string}

function outcomeParts(summary: BenchmarkSummary): Part[] {
  const known = summary.correct_supported + summary.correct_refusals + summary.false_refusals + summary.wrong_or_unsafe_answers
  return [{label: 'Correct answers', value: summary.correct_supported, tone: 'good'}, {label: 'Correct refusals', value: summary.correct_refusals, tone: 'calm'},
    {label: 'False refusals', value: summary.false_refusals, tone: 'warn'}, {label: 'Wrong or unsafe', value: summary.wrong_or_unsafe_answers, tone: 'bad'},
    {label: 'Other errors', value: summary.cases - known, tone: 'muted'}]
}

function Stacked({parts, total}: {parts: Part[]; total: number}) {
  const shown = parts.filter(part => part.value > 0)
  return <div className="bench-stack" role="img" aria-label={shown.map(part => `${part.label}: ${part.value} of ${total}`).join(', ')}>
    {shown.map(part => <span key={part.label} className={`seg ${part.tone}`} style={{width: `${part.value / total * 100}%`}} title={`${part.label}: ${part.value}`}>{part.value / total >= 0.07 ? part.value : ''}</span>)}
  </div>
}

function Legend({parts}: {parts: Part[]}) {
  return <div className="bench-legend">{parts.map(part => <span key={part.label}><i className={`seg ${part.tone}`}/>{part.label}</span>)}</div>
}

export default function BenchmarksPage() {
  const data = benchmark
  const facts = keyFacts(data)
  const [metricId, setMetricId] = useState(METRICS[0].id)
  const metric = METRICS.find(item => item.id === metricId) || METRICS[0]
  const byLabel = new Map(data.runs.map(run => [run.label, run]))
  const complete = facts.complete
  const values = complete.map(run => ({run, value: metric.value(run.summary)}))
  const top = metric.ratio ? 1 : Math.max(0, ...values.map(item => item.value ?? 0)) || 1
  const reasons = [...new Set(complete.flatMap(run => Object.keys(run.false_refusal_reasons)))]
  const failureParts = (run: BenchmarkRun): Part[] => [...reasons.map((reason, index) => ({label: REASON_LABELS[reason] || reason, value: run.false_refusal_reasons[reason] || 0, tone: REASON_TONES[index % REASON_TONES.length]})),
    {label: 'Wrong or unsafe answer', value: run.summary.wrong_or_unsafe_answers, tone: 'bad'}]
  const unmeasured = [...new Set(data.runs.filter(run => !isAida4(run)).map(run => run.model))].filter(model => !data.runs.some(run => isAida4(run) && run.model === model && run.scored > 0))
  const selectionSize = Math.max(0, ...complete.map(run => run.scored))
  const columns = ['key', 'suite', 'source', 'question', 'expected', ...data.runs.map(run => run.label)]
  const rows = data.questions.map(item => ({key: item.key, suite: item.suite, source: item.source, question: item.question, expected: item.expected === 'answer' ? 'Answer' : 'Refuse or clarify',
    ...Object.fromEntries(data.runs.map(run => [run.label, item.outcomes[run.label] ? OUTCOME_LABELS[item.outcomes[run.label]] || item.outcomes[run.label] : null]))}))
  const columnLabel = (key: string) => byLabel.has(key) ? runName(byLabel.get(key)!) : {key: 'Case', suite: 'Suite', source: 'Source', question: 'Question', expected: 'Expected'}[key] || key
  const generated = `${data.generated_at.slice(0, 10)} ${data.generated_at.slice(11, 16)} UTC`
  const comparison = data.before_after

  return <div className="landing bench">
    <header className="landing-nav">
      <a className="brand" href="/" aria-label="AIDA home"><span className="brand-mark"><i/><i/><i/></span><span className="brand-word">AIDA<span className="brand-dot">.</span></span></a>
      <nav className="landing-links" aria-label="Site"><a href="/#how">How it works</a><a href="/#trust">Security</a><a href="/benchmarks" aria-current="page">Benchmarks</a></nav>
      <div className="landing-actions"><a className="pill-button pill-ghost" href="/"><ArrowLeft size={14}/>Back to AIDA</a>{PREVIEW ? <span className="beta-chip">Private beta</span> : <a className="pill-button pill-lime" href="/signup">Get started free<ArrowRight size={14}/></a>}</div>
    </header>
    <main>
      <section className="bench-hero" aria-labelledby="bench-title"><div className="section-inner">
        <div className="section-eyebrow">Benchmarks</div>
        <h1 id="bench-title">Real questions, real model calls, scored against independent SQL.</h1>
        <p>Every number on this page is read from recorded benchmark runs. Only cost is derived: it uses Groq list prices and the token counts each run reported.</p>
        <p className="bench-meta">Generated {generated} from <code>{data.source}</code></p>
        <div className="bench-facts">
          {facts.shared && <div><strong>{pct(facts.shared.beforeSummary.overall_accuracy)} → {pct(facts.shared.afterSummary.overall_accuracy)}</strong><span>correct on the {facts.shared.questions} questions every compared run answered</span><small>{runName(facts.shared.before)} → {runName(facts.shared.after)}</small></div>}
          {facts.shared && <div><strong>{facts.shared.beforeSummary.wrong_or_unsafe_answers} → {facts.shared.afterSummary.wrong_or_unsafe_answers}</strong><span>wrong or unsafe answers on those questions</span><small>Same runs as above</small></div>}
          <div><strong>{facts.rows}/{facts.plan}</strong><span>answers with a correct plan that also returned the correct rows</span><small>Across all {facts.runCount} runs</small></div>
          {facts.bestRun && <div><strong>{facts.bestRun.summary.correct_refusals}/{facts.bestRun.summary.refusal_cases}</strong><span>requests correctly refused or clarified</span><small>{runName(facts.bestRun)}</small></div>}
        </div>
      </div></section>

      {comparison && <section className="landing-section" aria-labelledby="before-title"><div className="section-inner">
        <div className="section-eyebrow">Before and after</div>
        <h2 className="section-title" id="before-title">The same {comparison.questions} questions across {comparison.runs.length} runs.</h2>
        <p className="section-lead">Only questions scored by every run below are counted, so each bar covers identical questions.{comparison.expectation_changed.length > 0 && ` ${comparison.expectation_changed.join(', ')} was a required refusal for the previous pipeline, which had no calculations, and is scored as an answer for AIDA 4.`}</p>
        <div className="bench-card">
          {comparison.runs.map(label => {const run = byLabel.get(label)!, summary = comparison.summaries[label]; return <div className="bench-row" key={label}>
            <div className="bench-row-head"><strong>{runName(run)}</strong><span>{summary.overall_correct}/{summary.cases} correct ({pct(summary.overall_accuracy)}) · {summary.wrong_or_unsafe_answers} wrong or unsafe</span></div>
            <Stacked parts={outcomeParts(summary)} total={summary.cases}/>
          </div>})}
          <Legend parts={outcomeParts(comparison.summaries[comparison.runs[0]])}/>
        </div>
      </div></section>}

      {complete.length > 0 && <section className="landing-section bench-alt" aria-labelledby="models-title"><div className="section-inner">
        <div className="section-eyebrow">Model comparison</div>
        <h2 className="section-title" id="models-title">AIDA 4 models on the same {selectionSize}-question selection.</h2>
        <p className="section-lead">Complete runs only. Choose a measure.</p>
        <div className="bench-toggle" role="group" aria-label="Measure">{METRICS.map(item => <button key={item.id} type="button" aria-pressed={item.id === metric.id} onClick={() => setMetricId(item.id)}>{item.label}</button>)}</div>
        <div className="bench-card">
          <div className="bench-card-head"><strong>{metric.label}</strong><span>{metric.direction}</span></div>
          {values.map(({run, value}) => <div className="bench-bar" key={run.label}>
            <span className="bench-bar-name">{runName(run)}</span>
            <span className="bench-track"><span className="bench-fill" style={{width: `${value == null ? 0 : Math.max(value / top * 100, value > 0 ? 1.5 : 0)}%`}}/></span>
            <strong>{value == null ? 'Not recorded' : metric.format(value)}</strong>
          </div>)}
        </div>
      </div></section>}

      {complete.length > 0 && <section className="landing-section" aria-labelledby="lost-title"><div className="section-inner">
        <div className="section-eyebrow">Where answers were lost</div>
        <h2 className="section-title" id="lost-title">What stopped each run on the questions it missed.</h2>
        <p className="section-lead">A false refusal is recorded with the reason the model gave, or as rejected by code checks when the model&apos;s reply broke AIDA&apos;s grounding or format rules.</p>
        <div className="bench-card">
          {complete.map(run => {const parts = failureParts(run), total = parts.reduce((sum, part) => sum + part.value, 0); return <div className="bench-row" key={run.label}>
            <div className="bench-row-head"><strong>{runName(run)}</strong><span>{total} of {run.scored} questions missed</span></div>
            {total > 0 ? <Stacked parts={parts} total={total}/> : <p className="bench-empty">No missed questions.</p>}
          </div>})}
          <Legend parts={failureParts(complete[0])}/>
        </div>
        <div className="bench-plan">
          <div className="bench-card-head"><strong>When the plan was right, were the rows right?</strong><span>Answers whose plan matched the expected plan, and how many of those returned the expected rows</span></div>
          <div className="bench-plan-grid">{data.runs.map(run => <div key={run.label}><strong>{run.rows_correct_when_plan_correct}/{run.plan_correct_answers}</strong><span>{runName(run)}</span></div>)}</div>
        </div>
      </div></section>}

      {facts.bestRun && <section className="landing-section bench-alt" aria-labelledby="suites-title"><div className="section-inner">
        <div className="section-eyebrow">By suite</div>
        <h2 className="section-title" id="suites-title">{runName(facts.bestRun)} by kind of question.</h2>
        <div className="bench-card">
          {Object.entries(facts.bestRun.by_suite).map(([suite, summary]) => <div className="bench-suite" key={suite}>
            <span className="bench-bar-name">{SUITE_LABELS[suite] || suite}</span>
            <div>
              {summary.supported_cases > 0 && <div className="bench-mini"><span>Answers {summary.correct_supported}/{summary.supported_cases}</span><span className="bench-track"><span className="bench-fill" style={{width: `${summary.correct_supported / summary.supported_cases * 100}%`}}/></span></div>}
              {summary.refusal_cases > 0 && <div className="bench-mini"><span>Refusals {summary.correct_refusals}/{summary.refusal_cases}</span><span className="bench-track"><span className="bench-fill calm" style={{width: `${summary.correct_refusals / summary.refusal_cases * 100}%`}}/></span></div>}
            </div>
          </div>)}
        </div>
      </div></section>}

      {data.prompt_pairs.length > 0 && <section className="landing-section" aria-labelledby="prompt-title"><div className="section-inner">
        <div className="section-eyebrow">Prompt and repair changes</div>
        <h2 className="section-title" id="prompt-title">Same model, updated prompt, on the questions both runs scored.</h2>
        <p className="section-lead">These runs are partial: the provider&apos;s daily token quota stopped them. Small question counts are shown as they are.</p>
        <div className="bench-pairs">{data.prompt_pairs.map(pair => <div className="bench-card" key={pair.runs.join()}>
          <div className="bench-card-head"><strong>{modelName(byLabel.get(pair.runs[0])!.model)}</strong><span>{pair.questions} shared questions</span></div>
          {pair.runs.map(label => {const run = byLabel.get(label)!, summary = pair.summaries[label]; return <div className="bench-bar" key={label}>
            <span className="bench-bar-name">{run.pipeline}<small>{STATUS_LABELS[run.status] || run.status}</small></span>
            <span className="bench-track"><span className="bench-fill" style={{width: `${summary.cases ? summary.overall_correct / summary.cases * 100 : 0}%`}}/></span>
            <strong>{summary.overall_correct}/{summary.cases}</strong>
          </div>})}
        </div>)}</div>
      </div></section>}

      <section className="landing-section bench-alt" aria-labelledby="runs-title"><div className="section-inner">
        <div className="section-eyebrow">All runs</div>
        <h2 className="section-title" id="runs-title">Every recorded run.</h2>
        <div className="bench-table-wrap"><table className="bench-table">
          <thead><tr><th>Run</th><th>Status</th><th>Questions scored</th><th>Supported correct</th><th>Refusals correct</th><th>Wrong or unsafe</th><th>False refusals</th><th>Tokens / question</th><th>Median answer</th><th>Cost / 100 questions</th></tr></thead>
          <tbody>{data.runs.map(run => {const s = run.summary; return <tr key={run.label}>
            <td><strong>{runName(run)}</strong><small>{run.label}</small></td>
            <td>{STATUS_LABELS[run.status] || run.status}</td>
            <td>{run.scored}{run.not_run ? ` (+${run.not_run} not run)` : ''}</td>
            <td>{s.correct_supported}/{s.supported_cases} <small>{pct(s.supported_accuracy)}</small></td>
            <td>{s.correct_refusals}/{s.refusal_cases} <small>{pct(s.refusal_accuracy)}</small></td>
            <td>{s.wrong_or_unsafe_answers}</td><td>{s.false_refusals}</td>
            <td>{s.tokens_per_question == null ? '—' : Math.round(s.tokens_per_question).toLocaleString('en-US')}</td>
            <td>{!isAida4(run) || s.median_answer_ms_excluding_rate_limit_waits == null ? 'Not recorded' : `${(s.median_answer_ms_excluding_rate_limit_waits / 1000).toFixed(2)} s`}</td>
            <td>{s.estimated_cost_usd == null || !s.cases ? '—' : `$${(s.estimated_cost_usd / s.cases * 100).toFixed(3)}`}</td>
          </tr>})}</tbody>
        </table></div>
        <p className="bench-note">Runs with different question sets are not directly comparable; use the before-and-after and model comparison sections for like-for-like figures. The previous-pipeline runner did not record time spent waiting on provider rate limits, so its answer time is not shown.</p>
      </div></section>

      <section className="landing-section" aria-labelledby="questions-title"><div className="section-inner">
        <div className="section-eyebrow">Every question</div>
        <h2 className="section-title" id="questions-title">Search, filter and sort the {data.questions.length} questions and their outcomes.</h2>
        <div className="bench-card bench-explorer"><DataTable rows={rows} columns={columns} label={columnLabel} format={(_, value) => value == null ? '—' : String(value)} fileName="AIDA-benchmark-questions.csv"/></div>
      </div></section>

      <section className="landing-section bench-alt" aria-labelledby="method-title"><div className="section-inner">
        <div className="section-eyebrow">Method and limits</div>
        <h2 className="section-title" id="method-title">How these numbers were produced.</h2>
        <ul className="bench-method">
          <li>Each question runs through the same code path as the product, with real model inference, Prompt Guard screening, validation, SQL compilation and calculations.</li>
          <li>Expected rows come from independently written SQL or independently calculated values. Expected plans and rows are never sent to the model.</li>
          <li>A supported question counts as correct only when both the plan and the rows match. A refusal case counts as correct only when AIDA does not answer.</li>
          <li>These questions are a regression set: prompts were improved after inspecting failures, so these are not blind results on unseen data.</li>
          <li>Answer times exclude time spent waiting on provider rate limits. Costs are estimates from reported token usage and Groq list prices.</li>
          {data.runs.some(run => run.status !== 'complete') && <li>Runs marked partial were stopped by the provider&apos;s daily token quota and cover fewer questions.</li>}
          {unmeasured.length > 0 && <li>No AIDA 4 results are recorded yet for {unmeasured.map(modelName).join(', ')}.</li>}
        </ul>
        <div className="final-cta" style={{marginTop: 48}}><div><h2>See the answers for yourself.</h2><p>Every answer in AIDA shows its plan, SQL and lineage.</p></div><div className="hero-cta"><a className="pill-button pill-lime" href={PREVIEW ? '/' : '/signup'}>{PREVIEW ? 'Explore AIDA' : 'Get started free'}<ArrowRight size={15}/></a></div></div>
      </div></section>
    </main>
    <footer className="landing-footer"><span><strong style={{color: 'var(--ink)'}}>AIDA</strong> · Artificial Intelligence Data Analyst</span><span>Designed and built by Ghanashyam</span></footer>
  </div>
}
