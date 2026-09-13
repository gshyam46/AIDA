'use client'
import {useEffect, useState} from 'react'
import {useRouter} from 'next/navigation'
import {ArrowLeft, ArrowRight, Boxes, Check, Database, LoaderCircle, ShieldCheck, Upload} from 'lucide-react'
import {getSession, Onboarding, saveOnboarding, SessionState} from '../../lib/api'
import {PREVIEW} from '../../lib/mode'
import PreviewNotice from '../../components/PreviewNotice'

const TEAM_SIZES = ['1', '2-10', '11-50', '51-200', '201-1000', '1000+']
const USE_CASES: [string, string][] = [['sales', 'Sales'], ['finance', 'Finance'], ['operations', 'Operations'], ['support', 'Customer support'], ['marketing', 'Marketing'], ['product', 'Product'], ['logistics', 'Logistics'], ['other', 'Something else']]
const STEPS = ['Your organization', 'Your goals', 'Your data', 'Privacy and launch']
const DATA_CHOICES: {id: Onboarding['data_choice']; title: string; body: string; icon: typeof Database}[] = [
  {id: 'demo', title: 'Explore demo sources', body: 'Commerce, support, retail, billing and the public Chinook music store are ready now.', icon: Database},
  {id: 'sample_logistics', title: 'Add the logistics sample', body: 'A private, synthetic 12-table logistics database with joins, exceptions and archives.', icon: Boxes},
  {id: 'upload_later', title: 'Connect my SQLite file', body: 'Upload a snapshot in the Data catalog and approve which business fields AIDA may use.', icon: Upload},
]

function OnboardingFlow() {
  const router = useRouter()
  const [session, setSession] = useState<SessionState | null>(null)
  const [step, setStep] = useState(0)
  const [company, setCompany] = useState('')
  const [role, setRole] = useState('')
  const [team, setTeam] = useState('')
  const [useCases, setUseCases] = useState<string[]>([])
  const [goal, setGoal] = useState('')
  const [dataChoice, setDataChoice] = useState<Onboarding['data_choice']>('demo')
  const [consent, setConsent] = useState(false)
  const [error, setError] = useState('')
  const [busy, setBusy] = useState(false)
  useEffect(() => {
    getSession().then(state => {
      if (!state.auth_required) {router.replace('/workspace'); return}
      if (!state.user) {router.replace('/login?next=/onboarding'); return}
      setSession(state)
      const saved = state.onboarding
      if (saved) {setCompany(saved.company); setRole(saved.role_title); setTeam(saved.team_size); setUseCases(saved.use_cases); setGoal(saved.primary_goal || ''); setDataChoice(saved.data_choice); setConsent(saved.hosted_inference_consent)}
    }).catch(() => setError('The workspace could not be reached. Start the backend and reload.'))
  }, [router])
  const hosted = !!session?.hosted_inference
  const ready = [company.trim() && role.trim() && team, useCases.length > 0, !!dataChoice, !hosted || consent][step]
  const finish = async () => {
    setBusy(true); setError('')
    try {
      const result = await saveOnboarding({company: company.trim(), role_title: role.trim(), team_size: team, use_cases: useCases, primary_goal: goal.trim() || null, data_choice: dataChoice, hosted_inference_consent: consent})
      router.replace(result.sample_source ? `/workspace?source=${encodeURIComponent(result.sample_source.id)}` : dataChoice === 'upload_later' ? '/workspace?tab=data' : '/workspace')
    } catch (failure) {setError(failure instanceof Error ? failure.message : 'Onboarding could not be saved.'); setBusy(false)}
  }
  if (!session) return <div className="onboarding"><div className="loading-panel"><LoaderCircle className="spin" size={24}/><strong>Preparing your workspace…</strong>{error && <span>{error}</span>}</div></div>
  return <div className="onboarding">
    <header className="onboarding-top"><a className="brand" href="/" aria-label="AIDA home"><span className="brand-mark"><i/><i/><i/></span><span className="brand-word">AIDA<span className="brand-dot">.</span></span></a><span className="demo-badge"><span/>Signed in as {session.user?.name}</span></header>
    <section className="onboarding-card" aria-labelledby="onboarding-title">
      <nav className="onboarding-steps" aria-label="Onboarding steps"><h2>Set up AIDA for your team</h2>{STEPS.map((label, index) => <button key={label} type="button" aria-current={index === step ? 'step' : undefined} className={index < step ? 'done' : ''} disabled={index > step} onClick={() => setStep(index)}><b>{index < step ? <Check size={13}/> : index + 1}</b><span>{label}</span></button>)}</nav>
      <div className="onboarding-body">
        <div className="progress" aria-hidden="true"><span style={{width: `${(step + 1) / STEPS.length * 100}%`}}/></div>
        {step === 0 && <><h3 id="onboarding-title">Tell us about your organization</h3><p>AIDA uses this to tailor starter questions. It is stored with your account, never sent to the model.</p>
          <label className="field">Company or team name<input maxLength={120} value={company} onChange={event => setCompany(event.target.value)} placeholder="Acme Logistics"/></label>
          <label className="field">Your role<input maxLength={80} value={role} onChange={event => setRole(event.target.value)} placeholder="Operations analyst"/></label>
          <div className="field">Team size<div className="chip-group">{TEAM_SIZES.map(size => <button key={size} type="button" aria-pressed={team === size} onClick={() => setTeam(size)}>{size === '1' ? 'Just me' : `${size} people`}</button>)}</div></div></>}
        {step === 1 && <><h3 id="onboarding-title">What do you want to understand?</h3><p>Choose every area you plan to analyze.</p>
          <div className="chip-group">{USE_CASES.map(([id, label]) => <button key={id} type="button" aria-pressed={useCases.includes(id)} onClick={() => setUseCases(current => current.includes(id) ? current.filter(item => item !== id) : [...current, id])}>{label}</button>)}</div>
          <label className="field">The first question you want answered <small>Optional</small><textarea rows={3} maxLength={300} value={goal} onChange={event => setGoal(event.target.value)} placeholder="Which carriers cost the most per kilogram this quarter?"/></label></>}
        {step === 2 && <><h3 id="onboarding-title">Choose where to start</h3><p>You can switch sources or connect more data at any time.</p>
          <div className="choice-grid">{DATA_CHOICES.map(choice => <button key={choice.id} type="button" className="choice" aria-pressed={dataChoice === choice.id} onClick={() => setDataChoice(choice.id)}><choice.icon size={22}/><strong>{choice.title}</strong><span>{choice.body}</span></button>)}</div></>}
        {step === 3 && <><h3 id="onboarding-title">How your questions are handled</h3><p>Review exactly what leaves this server before you start.</p>
          <div className="privacy-box"><strong>{hosted ? 'Hosted interpretation (Groq).' : 'Local interpretation.'}</strong> {hosted
            ? 'Each new question and the approved catalog labels, definitions and allowed values are sent over HTTPS to Groq so the model can resolve names and build a plan. Database rows, query results, SQL, table and column names and file paths are never sent.'
            : 'Questions are interpreted by a model running on this machine. Nothing is sent to a hosted model provider.'} Code validates every plan and runs read-only SQL inside AIDA.</div>
          {hosted && <label className="consent"><input type="checkbox" checked={consent} onChange={event => setConsent(event.target.checked)}/><span>I understand that question text and approved catalog labels are processed by the hosted model provider, and that database rows are not.</span></label>}
          <div className="privacy-box" style={{marginTop: 16}}><ShieldCheck size={16} style={{verticalAlign: '-3px', marginRight: 6}}/>Your uploaded sources are visible only to your account. Repeated attempts to extract restricted data pause questions automatically.</div></>}
        {error && <div className="form-error" role="alert">{error}</div>}
        <div className="onboarding-actions">
          <button type="button" className="text-button" disabled={step === 0 || busy} onClick={() => setStep(step - 1)}><ArrowLeft size={14}/>Back</button>
          {step < STEPS.length - 1
            ? <button type="button" className="pill-button pill-dark" disabled={!ready} onClick={() => setStep(step + 1)}>Continue<ArrowRight size={15}/></button>
            : <button type="button" className="pill-button pill-dark" disabled={!ready || busy} onClick={() => void finish()}>{busy ? <LoaderCircle className="spin" size={15}/> : <Check size={15}/>}Open my workspace</button>}
        </div>
      </div>
    </section>
  </div>
}

export default function OnboardingPage() {
  return PREVIEW ? <PreviewNotice/> : <OnboardingFlow/>
}
