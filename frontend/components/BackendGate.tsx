'use client'
import {ReactNode, useEffect, useState} from 'react'
import {LoaderCircle} from 'lucide-react'
import ComingSoon, {InterestSource} from './ComingSoon'
import {checkBackend} from '../lib/api'
import {PREVIEW} from '../lib/mode'

// Keep registration open in the public preview and when the account service is offline.
export default function BackendGate({source, children}: {source: InterestSource; children: ReactNode}) {
  const [status, setStatus] = useState<'checking' | 'up' | 'down'>(PREVIEW ? 'down' : 'checking')
  useEffect(() => {
    if (PREVIEW) return
    let active = true
    checkBackend().then(up => {if (active) setStatus(up ? 'up' : 'down')})
    return () => {active = false}
  }, [])
  if (status === 'down') return <ComingSoon source={source}/>
  if (status === 'checking') return <div className="loading-panel" style={{minHeight: '100vh'}} role="status"><LoaderCircle className="spin" size={24}/><strong>Opening your workspace…</strong></div>
  return <>{children}</>
}
