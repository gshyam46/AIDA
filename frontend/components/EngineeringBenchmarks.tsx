import {BenchmarkData, isAida4, runName} from '../lib/benchmarks'

const SUITES: Record<string, string> = {logistics: 'Logistics regression', relational: 'Relational demos', semantic: 'Semantic regression', capabilities: 'Calculations', resolution: 'Name resolution'}
const BUCKETS: Record<string, string> = {login_client: 'Sign-in attempts per client', login_email: 'Sign-in attempts per email', signup_client: 'Sign-ups per client',
  question_user: 'Questions per user', plan_user: 'Explicit plans per user', model_global: 'Model-backed questions, all users', upload_user: 'Uploads per user',
  configure_user: 'Catalog changes per user', onboarding_user: 'Onboarding saves per user'}
const PARTS = [['guard_ms', 'Prompt Guard screen', 'calm'], ['model_ms', 'Model calls (resolve, plan, repair)', 'good'], ['code_and_sql_ms', 'AIDA code and SQL', 'amber']] as const

const duration = (value: number | null | undefined) => value == null ? '—' : value >= 1000 ? `${(value / 1000).toFixed(2)} s` : `${value < 10 ? value.toFixed(1) : Math.round(value)} ms`
const period = (seconds: number) => seconds === 60 ? 'minute' : seconds === 3600 ? 'hour' : seconds % 3600 === 0 ? `${seconds / 3600} h` : `${Math.round(seconds / 60)} min`

function Bar({label, sub, value, max, text}: {label: string; sub?: string; value: number | null; max: number; text: string}) {
  return <div className="bench-bar"><span className="bench-bar-name">{label}{sub && <small>{sub}</small>}</span>
    <span className="bench-track"><span className="bench-fill" style={{width: `${value == null || !max ? 0 : Math.max(value / max * 100, value > 0 ? 1.5 : 0)}%`}}/></span><strong>{text}</strong></div>
}

export default function EngineeringBenchmarks({data}: {data: BenchmarkData}) {
  const runs = data.runs.filter(run => run.engineering)
  const timed = runs.filter(run => run.engineering?.breakdown_mean_ms && (run.engineering.timed_questions ?? 0) > 0)
  const maxCost = Math.max(0, ...runs.map(run => run.engineering!.cost_per_1000_questions_usd ?? 0))
  const maxTokens = Math.max(0, ...runs.map(run => run.engineering!.prompt_tokens_per_question + run.engineering!.completion_tokens_per_question))
  const facts = data.engineering
  const engine = facts?.engine
  const replay = facts ? Object.entries(facts.replay) : []
  const byLabel = new Map(data.runs.map(run => [run.label, run]))
  if (!runs.length && !facts) return null
  return <section className="landing-section bench-dark" aria-labelledby="engineering-title"><div className="section-inner">
    <div className="section-eyebrow">Engineering</div>
    <h2 className="section-title" id="engineering-title">Latency, cost and the code around the model.</h2>
    <p className="section-lead">Accuracy is half the story. These measurements show where each answer&apos;s time and money go, how the deterministic engine performs with no model at all, and the gates every change passes.</p>

    {timed.length > 0 && <div className="bench-card">
      <div className="bench-card-head"><strong>Where the time goes in one answer</strong><span>Average per question, with provider rate-limit waits removed</span></div>
      {timed.map(run => {
        const e = run.engineering!, breakdown = e.breakdown_mean_ms!
        const parts = PARTS.map(([key, label, tone]) => ({label, tone, value: breakdown[key] ?? 0}))
        const total = parts.reduce((sum, part) => sum + part.value, 0)
        const code = breakdown.code_and_sql_ms ?? 0
        return <div className="bench-row" key={run.label}>
          <div className="bench-row-head"><strong>{runName(run)}</strong><span>median answer {duration(e.answer_ms_p50)} · p95 {duration(e.answer_ms_p95)} · AIDA code and SQL {duration(code)} ({total ? (code / total * 100).toFixed(1) : '0'}%) · {e.timed_questions} questions</span></div>
          <div className="bench-stack" role="img" aria-label={parts.map(part => `${part.label}: ${duration(part.value)}`).join(', ')}>
            {parts.filter(part => part.value > 0).map(part => <span key={part.label} className={`seg ${part.tone}`} style={{width: `${part.value / total * 100}%`}} title={`${part.label}: ${duration(part.value)}`}>{part.value / total >= 0.1 ? duration(part.value) : ''}</span>)}
          </div>
        </div>
      })}
      <div className="bench-legend">{PARTS.map(([, label, tone]) => <span key={label}><i className={`seg ${tone}`}/>{label}</span>)}</div>
    </div>}

    {runs.length > 0 && <div className="bench-pairs" style={{marginTop: 30}}>
      <div className="bench-card" style={{marginTop: 0}}>
        <div className="bench-card-head"><strong>Cost per 1,000 questions</strong><span>Reported tokens at list price</span></div>
        {runs.map(run => {const e = run.engineering!; return <Bar key={run.label} label={runName(run)} value={e.cost_per_1000_questions_usd} max={maxCost}
          sub={`${e.model_calls_per_question} model calls per question${isAida4(run) ? ` · repair on ${Math.round(e.repair_rate * 100)}% of questions` : ''}`}
          text={e.cost_per_1000_questions_usd == null ? 'Not recorded' : `$${e.cost_per_1000_questions_usd.toFixed(2)}`}/>})}
      </div>
      <div className="bench-card" style={{marginTop: 0}}>
        <div className="bench-card-head"><strong>Tokens per question</strong><span>Input: instructions and approved catalog · Output: the plan</span></div>
        {runs.map(run => {const e = run.engineering!, total = e.prompt_tokens_per_question + e.completion_tokens_per_question; return <div className="bench-row" key={run.label}>
          <div className="bench-row-head"><strong>{runName(run)}</strong><span>{Math.round(total).toLocaleString('en-US')} tokens</span></div>
          <div className="bench-stack" style={{width: `${maxTokens ? total / maxTokens * 100 : 0}%`}} role="img" aria-label={`${Math.round(e.prompt_tokens_per_question)} input and ${Math.round(e.completion_tokens_per_question)} output tokens`}>
            <span className="seg good" style={{width: `${total ? e.prompt_tokens_per_question / total * 100 : 0}%`}}>{Math.round(e.prompt_tokens_per_question).toLocaleString('en-US')}</span>
            <span className="seg amber" style={{width: `${total ? e.completion_tokens_per_question / total * 100 : 0}%`}}>{total && e.completion_tokens_per_question / total >= 0.12 ? Math.round(e.completion_tokens_per_question) : ''}</span>
          </div>
        </div>})}
        <div className="bench-legend"><span><i className="seg good"/>Input tokens</span><span><i className="seg amber"/>Output tokens</span></div>
      </div>
    </div>}

    {timed.length > 0 && <div className="bench-plan">
      <div className="bench-card-head"><strong>Time lost to provider rate limits</strong><span>Free-tier waits, excluded from the answer times above</span></div>
      <div className="bench-plan-grid">{runs.filter(run => run.engineering?.latency_recorded).map(run => {const e = run.engineering!; return <div key={run.label}>
        <strong>{Math.round((e.provider_wait_seconds_total ?? 0) / 60)} min</strong><span>{e.questions_waiting_on_provider} of {e.questions} questions waited · {runName(run)}</span></div>})}</div>
    </div>}

    {engine && <div className="bench-card">
      <div className="bench-card-head"><strong>Deterministic engine, no model</strong><span>Every benchmark question with an expected plan, executed {engine.repeats} times with result caches cleared</span></div>
      <div className="eng-grid">
        <div className="eng-stat"><strong>{engine.summary.rows_match}/{engine.summary.plans}</strong><span>explicit plans returned the independent oracle rows</span></div>
        <div className="eng-stat"><strong>{duration(engine.summary.uncached_ms.p50)}</strong><span>median uncached plan, request to result (p95 {duration(engine.summary.uncached_ms.p95)})</span></div>
        <div className="eng-stat"><strong>{duration(engine.summary.database_ms.p50)}</strong><span>median time inside the database</span></div>
        <div className="eng-stat"><strong>{duration(engine.summary.cached_ms.p50)}</strong><span>median repeat served from the result cache ({engine.summary.cache_hits} of {engine.summary.plans})</span></div>
      </div>
      <div className="bench-table-wrap"><table className="bench-table compact">
        <thead><tr><th>Suite</th><th>Plans</th><th>Rows match oracle</th><th>Uncached p50</th><th>Uncached p95</th><th>Cached p50</th></tr></thead>
        <tbody>{Object.entries(engine.by_suite).map(([suite, s]) => <tr key={suite}><td>{SUITES[suite] || suite}</td><td>{s.plans}</td><td>{s.rows_match}/{s.plans}</td>
          <td>{duration(s.uncached_ms.p50)}</td><td>{duration(s.uncached_ms.p95)}</td><td>{duration(s.cached_ms.p50)}</td></tr>)}</tbody>
      </table></div>
    </div>}

    {replay.length > 0 && <div className="bench-card">
      <div className="bench-card-head"><strong>Recorded model outputs replayed through today&apos;s code</strong><span>No model calls: validation, SQL and calculations only</span></div>
      <div className="bench-table-wrap"><table className="bench-table compact">
        <thead><tr><th>Run</th><th>Answers replayed</th><th>Identical rows</th><th>Identical plan</th><th>Code and SQL p50</th><th>Code and SQL p95</th><th>Repeat question p50</th></tr></thead>
        <tbody>{replay.map(([label, r]) => {const run = byLabel.get(label); return <tr key={label}>
          <td><strong>{run ? runName(run) : label}</strong></td><td>{r.replayed}</td><td>{r.rows_identical}/{r.replayed}</td><td>{r.plans_identical}/{r.replayed}</td>
          <td>{duration(r.code_ms.p50)}</td><td>{duration(r.code_ms.p95)}</td><td>{duration(r.repeat_ms.p50)}<small>{r.repeat_without_model_calls}/{r.replayed} with no model call</small></td>
        </tr>})}</tbody>
      </table></div>
    </div>}

    {facts && <div className="bench-pairs" style={{marginTop: 30}}>
      {facts.quality && <div className="bench-card" style={{marginTop: 0}}>
        <div className="bench-card-head"><strong>Quality gates</strong><span>Recorded {facts.generated_at.slice(0, 10)}</span></div>
        <div className="eng-grid">
          <div className="eng-stat"><strong>{facts.quality.backend_tests.passed ?? 0}{facts.quality.backend_tests.failed ? ` · ${facts.quality.backend_tests.failed} failed` : ''}</strong><span>backend tests passed</span></div>
          {facts.quality.security_tests != null && <div className="eng-stat"><strong>{facts.quality.security_tests}</strong><span>attack and misuse tests</span></div>}
          {facts.quality.interpreter_tests != null && <div className="eng-stat"><strong>{facts.quality.interpreter_tests}</strong><span>interpreter contract tests, including malformed model output</span></div>}
          {facts.quality.browser_journey && <div className="eng-stat"><strong>{facts.quality.browser_journey.passed}/{facts.quality.browser_journey.steps}</strong><span>browser journey steps passed ({facts.quality.browser_journey.finished_at?.slice(0, 10)})</span></div>}
        </div>
      </div>}
      <div className="bench-card" style={{marginTop: 0}}>
        <div className="bench-card-head"><strong>Configured protections</strong><span>Read from the running code</span></div>
        <div className="bench-table-wrap" style={{marginTop: 0}}><table className="bench-table compact"><tbody>
          {Object.entries(facts.limits).map(([bucket, limit]) => <tr key={bucket}><td>{BUCKETS[bucket] || bucket}</td><td>{limit.requests} per {period(limit.window_seconds)}</td></tr>)}
          <tr><td>Misuse pause</td><td>{facts.misuse.strikes} restricted refusals in {period(facts.misuse.window_seconds)} pause questions for {period(facts.misuse.pause_seconds)}</td></tr>
        </tbody></table></div>
      </div>
    </div>}

    {facts && <p className="bench-note">Engine and replay figures come from <code>scripts/benchmark_engine.py</code> on {facts.environment.system}, Python {facts.environment.python}, {facts.environment.cpu_count} logical CPUs. Model latency and cost come from the recorded runs on this page.</p>}
  </div></section>
}
