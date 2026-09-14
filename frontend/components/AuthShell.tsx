import {Database, LockKeyhole, ShieldCheck} from 'lucide-react'
import BrandLogo from './BrandLogo'

export default function AuthShell({title, lead, children}: {title: string; lead: string; children: React.ReactNode}) {
  return <div className="auth-shell">
    <aside className="auth-story">
      <a className="brand" href="/" aria-label="AIDA home"><BrandLogo/></a>
      <h1>{title}</h1>
      <p>{lead}</p>
      <div className="auth-points">
        <div><ShieldCheck size={16}/><span>Answers built on business definitions your team approves.</span></div>
        <div><Database size={16}/><span>A clear path from every chart back to its source.</span></div>
        <div><LockKeyhole size={16}/><span>Your database rows stay out of model prompts.</span></div>
      </div>
    </aside>
    <main className="auth-panel"><div className="auth-card">{children}</div></main>
  </div>
}
