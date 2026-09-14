'use client'
import {useEffect, useState} from 'react'
import {ArrowLeft, ArrowRight, LoaderCircle} from 'lucide-react'
import AuthShell from '../../components/AuthShell'
import RegistrationConfirmation, {REGISTRATION_CONFIRMATION_KEY} from '../../components/RegistrationConfirmation'

export default function WaitlistPage() {
  const [confirmation, setConfirmation] = useState<'checking' | 'signup' | 'existing' | 'none'>('checking')
  useEffect(() => {
    try {
      const source = sessionStorage.getItem(REGISTRATION_CONFIRMATION_KEY)
      setConfirmation(source === 'signup' ? 'signup' : source && ['login', 'onboarding', 'workspace'].includes(source) ? 'existing' : 'none')
    } catch {setConfirmation('none')}
  }, [])
  return <AuthShell title="Good questions are worth making room for." lead="While your workspace takes shape, explore what a conversation with your data can look like.">
    <a className="back-link" href="/"><ArrowLeft size={14}/>Back to AIDA</a>
    {confirmation === 'checking' ? <div className="loading-panel" role="status"><LoaderCircle className="spin" size={20}/><span>Loading your registration…</span></div> : confirmation !== 'none' ? <RegistrationConfirmation existingAccount={confirmation === 'existing'}/> : <>
      <span className="auth-eyebrow">Your AIDA workspace</span>
      <h2>Make a start.</h2>
      <p>Sign up with your name and email to register for an AIDA workspace.</p>
      <a className="pill-button pill-dark auth-submit" href="/signup">Sign up<ArrowRight size={16}/></a>
      <p className="auth-switch">Already have an account? <a href="/login">Sign in</a></p>
    </>}
  </AuthShell>
}
