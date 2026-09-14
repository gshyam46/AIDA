'use client'
import {FormEvent, useState} from 'react'
import {useRouter} from 'next/navigation'
import {ArrowLeft, ArrowRight, LoaderCircle} from 'lucide-react'
import AuthShell from './AuthShell'
import RegistrationConfirmation, {REGISTRATION_CONFIRMATION_KEY} from './RegistrationConfirmation'

export type InterestSource = 'signup' | 'login' | 'onboarding' | 'workspace'
export type InterestDraft = {name?: string; email?: string; company?: string; role?: string; interest?: string}

const SAVE_FAILED = 'We could not save your registration. Your details are still here; please try again.'

// Preview registration is separate from account creation and never accepts a password.
export default function ComingSoon({source, draft = {}}: {source: InterestSource; draft?: InterestDraft}) {
  const router = useRouter()
  const [name, setName] = useState(draft.name || '')
  const [email, setEmail] = useState(draft.email || '')
  const [company, setCompany] = useState(draft.company || '')
  const [role, setRole] = useState(draft.role || '')
  const [interest, setInterest] = useState(draft.interest || '')
  const [consent, setConsent] = useState(false)
  const [website, setWebsite] = useState('')
  const [state, setState] = useState<'idle' | 'sending' | 'saved' | 'failed'>('idle')
  const [message, setMessage] = useState('')
  const signingIn = source === 'login'
  const submit = async (event: FormEvent) => {
    event.preventDefault()
    setState('sending'); setMessage('')
    try {
      const response = await fetch('/api/interest', {method: 'POST', headers: {'Content-Type': 'application/json'}, credentials: 'same-origin', cache: 'no-store',
        signal: AbortSignal.timeout(15000), body: JSON.stringify({name, email, company, role, interest, source, consent, website})})
      const body = await response.json().catch(() => null)
      if (response.ok && body?.stored === true) {
        // No personal information is placed in the URL or browser storage.
        setState('saved')
        try {
          sessionStorage.setItem(REGISTRATION_CONFIRMATION_KEY, source)
          router.replace('/waitlist')
        } catch {
          // Keep a truthful inline confirmation when browser storage is disabled.
        }
        return
      }
      setMessage(typeof body?.error === 'string' ? body.error : SAVE_FAILED)
      setState('failed')
    } catch {
      setMessage(SAVE_FAILED)
      setState('failed')
    }
  }
  return <AuthShell title="A clearer view starts here." lead="A place for your data, your questions, and the decisions that follow.">
    <a className="back-link" href="/"><ArrowLeft size={14}/>Back to AIDA</a>
    {state === 'saved' ? <RegistrationConfirmation existingAccount={source !== 'signup'}/> : <>
      <span className="auth-eyebrow">Your AIDA workspace</span>
      <h2>{signingIn ? 'Sign in' : 'Sign up'}</h2>
      <p>{signingIn ? 'Workspace sign-in is temporarily paused. Leave your email for an update when you can return.' : 'Start with your name and email. We’ll guide you through the next step.'}</p>
      {message && <div className="form-error" role="alert">{message}</div>}
      <form onSubmit={submit}>
        <label className="field">Full name{signingIn && <small>Optional</small>}<input autoComplete="name" required={!signingIn} maxLength={80} value={name} onChange={event => setName(event.target.value)} placeholder="Your name"/></label>
        <label className="field">Work email<input type="email" autoComplete="email" required maxLength={254} value={email} onChange={event => setEmail(event.target.value)} placeholder="you@company.com"/></label>
        <details className="registration-details" open={source === 'onboarding' || Boolean(draft.company || draft.role || draft.interest)}>
          <summary>Tell us about your team <span>Optional</span></summary>
          <label className="field">Company or team<input autoComplete="organization" maxLength={120} value={company} onChange={event => setCompany(event.target.value)}/></label>
          <label className="field">Your role<input autoComplete="organization-title" maxLength={80} value={role} onChange={event => setRole(event.target.value)}/></label>
          <label className="field">What would you like to analyze?<textarea rows={3} maxLength={300} value={interest} onChange={event => setInterest(event.target.value)}/></label>
        </details>
        <label className="interest-trap" aria-hidden="true">Website<input tabIndex={-1} autoComplete="off" value={website} onChange={event => setWebsite(event.target.value)}/></label>
        <label className="consent"><input type="checkbox" required checked={consent} onChange={event => setConsent(event.target.checked)}/><span>Email me about my registration and workspace availability. I can ask to have my details deleted at any time.</span></label>
        <button className="pill-button pill-dark auth-submit" disabled={state === 'sending'}>{state === 'sending' ? <LoaderCircle className="spin" size={16}/> : <ArrowRight size={16}/>}<span>{state === 'sending' ? 'Saving your details…' : signingIn ? 'Keep me updated' : 'Sign up'}</span></button>
      </form>
      <p className="auth-switch">{signingIn ? <>New to AIDA? <a href="/signup">Sign up</a></> : <>Already registered? <a href="/login">Sign in</a></>}</p>
    </>}
  </AuthShell>
}
