'use client'
import {FormEvent, useEffect, useState} from 'react'
import {useRouter} from 'next/navigation'
import {ArrowLeft, ArrowRight, LoaderCircle} from 'lucide-react'
import AuthShell from '../../components/AuthShell'
import {getSession, signIn} from '../../lib/api'
import {PREVIEW} from '../../lib/mode'
import PreviewNotice from '../../components/PreviewNotice'

function nextPath() {
  const next = new URLSearchParams(window.location.search).get('next') || ''
  return next.startsWith('/') && !next.startsWith('//') ? next : '/workspace'
}

function LoginForm() {
  const router = useRouter()
  const [email, setEmail] = useState('')
  const [password, setPassword] = useState('')
  const [error, setError] = useState('')
  const [busy, setBusy] = useState(false)
  useEffect(() => {
    getSession().then(session => {
      if (!session.auth_required) router.replace('/workspace')
      else if (session.user) router.replace(session.onboarding ? nextPath() : '/onboarding')
    }).catch(() => undefined)
  }, [router])
  const submit = async (event: FormEvent) => {
    event.preventDefault()
    setBusy(true); setError('')
    try {
      const session = await signIn({email, password})
      router.replace(session.onboarding ? nextPath() : '/onboarding')
    } catch (failure) {setError(failure instanceof Error ? failure.message : 'Sign-in failed. Try again.')}
    finally {setBusy(false)}
  }
  return <AuthShell title="Welcome back to your data." lead="Pick up where you left off: saved dashboards, approved catalogs and every answer's full lineage.">
    <a className="back-link" href="/"><ArrowLeft size={14}/>Back to AIDA</a>
    <h2>Sign in</h2>
    <p>Use the account you created for this workspace.</p>
    {error && <div className="form-error" role="alert">{error}</div>}
    <form onSubmit={submit}>
      <label className="field">Work email<input type="email" autoComplete="email" required maxLength={254} value={email} onChange={event => setEmail(event.target.value)}/></label>
      <label className="field">Password<input type="password" autoComplete="current-password" required maxLength={128} value={password} onChange={event => setPassword(event.target.value)}/></label>
      <button className="pill-button pill-dark auth-submit" disabled={busy}>{busy ? <LoaderCircle className="spin" size={16}/> : <ArrowRight size={16}/>}Sign in</button>
    </form>
    <p className="auth-switch">New to AIDA? <a href="/signup">Create an account</a></p>
  </AuthShell>
}

export default function LoginPage() {
  return PREVIEW ? <PreviewNotice/> : <LoginForm/>
}
