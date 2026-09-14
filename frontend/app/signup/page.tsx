'use client'
import {FormEvent, useEffect, useState} from 'react'
import {useRouter} from 'next/navigation'
import {ArrowLeft, ArrowRight, LoaderCircle} from 'lucide-react'
import AuthShell from '../../components/AuthShell'
import BackendGate from '../../components/BackendGate'
import ComingSoon from '../../components/ComingSoon'
import {getSession, isServiceUnavailable, signUp} from '../../lib/api'

function SignupForm() {
  const router = useRouter()
  const [name, setName] = useState('')
  const [email, setEmail] = useState('')
  const [password, setPassword] = useState('')
  const [confirm, setConfirm] = useState('')
  const [error, setError] = useState('')
  const [busy, setBusy] = useState(false)
  const [unavailable, setUnavailable] = useState(false)
  useEffect(() => {
    getSession().then(session => {
      if (!session.auth_required) router.replace('/workspace')
      else if (session.user) router.replace(session.onboarding ? '/workspace' : '/onboarding')
    }).catch(failure => {if (isServiceUnavailable(failure)) setUnavailable(true)})
  }, [router])
  const local = email.split('@')[0].toLowerCase()
  const checks = [password.length >= 10, /[A-Za-z]/.test(password) && /\d/.test(password), new Set(password).size >= 5, !(local.length >= 4 && password.toLowerCase().includes(local))]
  const strength = password ? checks.filter(Boolean).length : 0
  const submit = async (event: FormEvent) => {
    event.preventDefault()
    if (password !== confirm) {setError('The passwords do not match.'); return}
    if (checks.includes(false)) {setError('Use at least 10 characters with letters and a number, avoid repeated characters, and do not include your email name.'); return}
    setBusy(true); setError('')
    try {
      await signUp({name, email, password})
      router.replace('/onboarding')
    } catch (failure) {
      if (isServiceUnavailable(failure)) setUnavailable(true)
      else setError(failure instanceof Error ? failure.message : 'The account could not be created.')
    } finally {
      setBusy(false)
    }
  }
  // The password is never carried over: the interest form only receives name and email.
  if (unavailable) return <ComingSoon source="signup" draft={{name, email}}/>
  return <AuthShell title="Ask your data. Verify every answer." lead="Create your AIDA account, tell us about your team, and ask your first question in about two minutes.">
    <a className="back-link" href="/"><ArrowLeft size={14}/>Back to AIDA</a>
    <h2>Create your account</h2>
    <p>The first account becomes the workspace owner and can review security events.</p>
    {error && <div className="form-error" role="alert">{error}</div>}
    <form onSubmit={submit}>
      <label className="field">Full name<input autoComplete="name" required maxLength={80} value={name} onChange={event => setName(event.target.value)}/></label>
      <label className="field">Work email<input type="email" autoComplete="email" required maxLength={254} value={email} onChange={event => setEmail(event.target.value)}/></label>
      <label className="field">Password<input type="password" autoComplete="new-password" required maxLength={128} value={password} onChange={event => setPassword(event.target.value)} aria-describedby="password-help"/>
        <span className="password-meter" aria-hidden="true">{[0, 1, 2, 3].map(index => <span key={index} className={index < strength ? 'on' : ''}/>)}</span>
        <small id="password-help">At least 10 characters, with letters and a number.</small>
      </label>
      <label className="field">Confirm password<input type="password" autoComplete="new-password" required maxLength={128} value={confirm} onChange={event => setConfirm(event.target.value)}/></label>
      <button className="pill-button pill-dark auth-submit" disabled={busy}>{busy ? <LoaderCircle className="spin" size={16}/> : <ArrowRight size={16}/>}Create account</button>
    </form>
    <p className="auth-switch">Already have an account? <a href="/login">Sign in</a></p>
  </AuthShell>
}

export default function SignupPage() {
  return <BackendGate source="signup"><SignupForm/></BackendGate>
}
