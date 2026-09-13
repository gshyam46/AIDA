import {ArrowLeft, BarChart3} from 'lucide-react'
import AuthShell from './AuthShell'

export default function PreviewNotice() {
  return <AuthShell title="AIDA is in private beta." lead="This public preview shows how AIDA works, how it is secured and what it has measured. Accounts and the live workspace run on the private deployment.">
    <a className="back-link" href="/"><ArrowLeft size={14}/>Back to AIDA</a>
    <h2>Accounts are not open yet</h2>
    <p>Explore how a question becomes a verified answer, the security layers, and every recorded benchmark run.</p>
    <a className="pill-button pill-dark auth-submit" href="/benchmarks"><BarChart3 size={16}/>See the benchmarks</a>
    <p className="auth-switch"><a href="/#how">How AIDA works</a></p>
  </AuthShell>
}
