import {ArrowRight, Check} from 'lucide-react'

export const REGISTRATION_CONFIRMATION_KEY = 'aida.registration.saved'

export default function RegistrationConfirmation({existingAccount = false}: {existingAccount?: boolean}) {
  return <div className="interest-done" role="status">
    <span className="interest-icon"><Check size={20}/></span>
    <span className="auth-eyebrow">{existingAccount ? 'Details received' : 'Registration received'}</span>
    <h2>{existingAccount ? 'We’ll keep you updated.' : 'You’re on the list.'}</h2>
    <p>{existingAccount ? 'Your details are saved for workspace availability updates. We’ll email you when you can return.' : 'Your registration is saved. We’re opening new workspaces in stages, and we’ll email you when yours is ready.'}</p>
    {!existingAccount && <p className="registration-note">You’ll set up your account when your workspace opens.</p>}
    <a className="pill-button pill-dark auth-submit" href="/#demo">Explore the demo<ArrowRight size={16}/></a>
    <a className="back-link" href="/benchmarks">Read the benchmarks<ArrowRight size={14}/></a>
  </div>
}
