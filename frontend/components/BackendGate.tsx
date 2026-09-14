'use client'
import {ReactNode, useEffect, useState} from 'react'
import {LoaderCircle} from 'lucide-react'
import ComingSoon, {InterestSource} from './ComingSoon'
import {checkBackend} from '../lib/api'
import {PREVIEW} from '../lib/mode'

// Accounts need the data service. When it is not deployed or not answering, offer the interest form instead.
export default function BackendGate({source, children}: {source: InterestSource; children: ReactNode}) {
  const [status, setStatus] = useState<'checking' | 'up' | 'down'>(PREVIEW ? 'down' : 'checking')
  useEffect(() => {
    if (PREVIEW) return
    let active = true
    checkBackend().then(up => {if (active) setStatus(up ? 'up' : 'down')})
    return () => {active = false}
  }, [])
  if (status === 'down') return <ComingSoon source={source}/>
  if (status === 'checking') return <div className="loading-panel" style={{minHeight: '100vh'}} role="status"><LoaderCircle className="spin" size={24}/><strong>Checking AIDA availability…</strong></div>
  return <>{children}</>
}
