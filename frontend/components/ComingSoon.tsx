'use client'
import {FormEvent, useState} from 'react'
import {ArrowLeft, BarChart3, Check, LoaderCircle, Send} from 'lucide-react'
import AuthShell from './AuthShell'

export type InterestSource = 'signup' | 'login' | 'onboarding' | 'workspace'
export type InterestDraft = {name?: string; email?: string; company?: string; role?: string; interest?: string}

const SAVE_FAILED = 'We could not save your details right now. Please try again a little later.'

// Shown when accounts are unavailable. Records interest without ever sending a password.
export default function ComingSoon({source, draft = {}}: {source: InterestSource; draft?: InterestDraft}) {
  const [name, setName] = useState(draft.name || '')
  const [email, setEmail] = useState(draft.email || '')
  const [company, setCompany] = useState(draft.company || '')
  const [role, setRole] = useState(draft.role || '')
  const [interest, setInterest] = useState(draft.interest || '')
  const [consent, setConsent] = useState(false)
  const [website, setWebsite] = useState('')
  const [state, setState] = useState<'idle' | 'sending' | 'saved' | 'failed'>('idle')
  const [message, setMessage] = useState('')
  const submit = async (event: FormEvent) => {
    event.preventDefault()
    setState('sending'); setMessage('')
    try {
      const response = await fetch('/api/interest', {method: 'POST', headers: {'Content-Type': 'application/json'}, credentials: 'same-origin', cache: 'no-store',
        signal: AbortSignal.timeout(15000), body: JSON.stringify({name, email, company, role, interest, source, consent, website})})
      const body = await response.json().catch(() => null)
      if (response.ok && body?.stored) {setState('saved'); return}
      setMessage(typeof body?.error === 'string' ? body.error : SAVE_FAILED)
      setState('failed')
    } catch {
      setMessage(SAVE_FAILED)
      setState('failed')
    }
  }
  return <AuthShell title="AIDA is opening soon." lead="We are rolling out access in stages. Leave your details and we will let you know as soon as your workspace is ready.">
    <a className="back-link" href="/"><ArrowLeft size={14}/>Back to AIDA</a>
    {state === 'saved' ? <div className="interest-done" role="status">
      <span className="interest-icon"><Check size={20}/></span>
      <h2>Thank you{name.trim() ? `, ${name.trim().split(' ')[0]}` : ''}.</h2>
      <p>We appreciate your interest in AIDA and will keep you posted at <strong>{email.trim()}</strong>.</p>
      <a className="pill-button pill-dark auth-submit" href="/benchmarks"><BarChart3 size={16}/>See the benchmarks meanwhile</a>
    </div> : <>
      <span className="soon-chip">Currently unavailable</span>
      <h2>{source === 'login' ? 'Sign-in is paused for now' : 'Sign-ups are coming soon'}</h2>
      <p>AIDA is not accepting sessions at the moment. We appreciate your interest, and we will keep you posted.</p>
      {message && <div className="form-error" role="alert">{message}</div>}
      <form onSubmit={submit}>
        <label className="field">Full name{source === 'login' && <small>Optional</small>}<input autoComplete="name" required={source !== 'login'} maxLength={80} value={name} onChange={event => setName(event.target.value)}/></label>
        <label className="field">Work email<input type="email" autoComplete="email" required maxLength={254} value={email} onChange={event => setEmail(event.target.value)}/></label>
        <label className="field">Company or team <small>Optional</small><input autoComplete="organization" maxLength={120} value={company} onChange={event => setCompany(event.target.value)}/></label>
        <label className="field">Your role <small>Optional</small><input autoComplete="organization-title" maxLength={80} value={role} onChange={event => setRole(event.target.value)}/></label>
        <label className="field">What would you like to analyze? <small>Optional</small><textarea rows={3} maxLength={300} value={interest} onChange={event => setInterest(event.target.value)}/></label>
        <label className="interest-trap" aria-hidden="true">Website<input tabIndex={-1} autoComplete="off" value={website} onChange={event => setWebsite(event.target.value)}/></label>
        <label className="consent"><input type="checkbox" required checked={consent} onChange={event => setConsent(event.target.checked)}/><span>Email me about AIDA access. My details are used only for this, and I can ask for them to be deleted.</span></label>
        <button className="pill-button pill-dark auth-submit" disabled={state === 'sending'}>{state === 'sending' ? <LoaderCircle className="spin" size={16}/> : <Send size={16}/>}Keep me posted</button>
      </form>
      <p className="auth-switch"><a href="/benchmarks">See how AIDA performs</a></p>
    </>}
  </AuthShell>
}
