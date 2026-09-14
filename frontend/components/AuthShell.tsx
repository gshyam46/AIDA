import {Database, LockKeyhole, ShieldCheck} from 'lucide-react'

export default function AuthShell({title, lead, children}: {title: string; lead: string; children: React.ReactNode}) {
  return <div className="auth-shell">
    <aside className="auth-story">
      <a className="brand" href="/" aria-label="AIDA home"><span className="brand-mark"><i/><i/><i/></span><span className="brand-word">AIDA<span className="brand-dot">.</span></span></a>
      <h1>{title}</h1>
      <p>{lead}</p>
      <div className="auth-points">
        <div><ShieldCheck size={16}/><span>An LLM interprets your words; code validates every plan and compiles read-only SQL.</span></div>
        <div><Database size={16}/><span>Database rows, SQL and table names never enter model prompts.</span></div>
        <div><LockKeyhole size={16}/><span>Hashed passwords, HttpOnly sessions, rate limits and private uploads per account.</span></div>
      </div>
    </aside>
    <main className="auth-panel"><div className="auth-card">{children}</div></main>
  </div>
}
