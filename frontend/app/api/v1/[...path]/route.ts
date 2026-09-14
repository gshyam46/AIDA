import {NextRequest, NextResponse} from 'next/server'
export const dynamic = 'force-dynamic'
type RouteContext = {params: Promise<{path: string[]}>}
const GET_ENDPOINTS = new Set(['catalog', 'examples', 'health', 'schema', 'sources', 'connections', 'auth/session', 'security/events'])
const POST_ENDPOINTS = new Set(['query', 'sources', 'auth/signup', 'auth/login', 'auth/logout', 'onboarding', 'samples/logistics'])
const PUBLIC_DEMO_POSTS = new Set(['query', 'auth/signup', 'auth/login', 'auth/logout', 'onboarding'])
async function proxy(request: NextRequest, context: RouteContext) {
  if (process.env.NEXT_PUBLIC_AIDA_MODE === 'preview') return NextResponse.json({error: 'AIDA is in private beta. The data service is not enabled on this deployment.'}, {status: 503})
  const {path} = await context.params
  const endpoint = path.join('/')
  const connectionPath = endpoint === 'connections' || endpoint === 'connections/inspect' || /^connections\/[a-f0-9]{32}\/(refresh|schedule|disconnect)$/.test(endpoint)
  const sourcePath = /^sources\/[a-zA-Z0-9_-]+$/.test(endpoint)
  const configurePath = /^sources\/[a-zA-Z0-9_-]+\/configure$/.test(endpoint)
  const allowed = request.method === 'GET'
    ? GET_ENDPOINTS.has(endpoint) || sourcePath
    : request.method === 'POST' && (POST_ENDPOINTS.has(endpoint) || configurePath || connectionPath)
  if (!allowed) return NextResponse.json({error: 'Endpoint unavailable.'}, {status: 404})
  const publicDemo = process.env.AIDA_PUBLIC_DEMO === '1'
  const host = request.headers.get('host') || ''
  let hostname: string
  try {hostname = new URL(`http://${host}`).hostname} catch {return NextResponse.json({error: 'Invalid request host.'}, {status: 400})}
  if (!publicDemo && !['127.0.0.1', 'localhost', '[::1]'].includes(hostname)) return NextResponse.json({error: 'Local workspaces accept loopback requests only.'}, {status: 403})
  if (request.method === 'POST') {
    const origin = request.headers.get('origin')
    if (origin) {
      try {if (new URL(origin).host !== host) return NextResponse.json({error: 'Cross-origin writes are not allowed.'}, {status: 403})}
      catch {return NextResponse.json({error: 'Invalid request origin.'}, {status: 403})}
    }
    if (publicDemo && !PUBLIC_DEMO_POSTS.has(endpoint)) return NextResponse.json({error: 'Database onboarding is disabled in public demo mode.'}, {status: 403})
  }
  const backend = (process.env.BACKEND_URL || 'http://127.0.0.1:8000').replace(/\/$/, '')
  try {
    let body: ArrayBuffer | undefined
    if (request.method === 'POST') {
      const maximum = endpoint === 'sources' ? 20 * 1024 * 1024 : configurePath || connectionPath ? 65536 : 8192
      const oversized = () => NextResponse.json({error: 'Request exceeds the size limit.'}, {status: 413})
      if (Number(request.headers.get('content-length')) > maximum) return oversized()
      const reader = request.body?.getReader(), chunks: Uint8Array[] = []
      let bytes = 0
      if (reader) while (true) {
        const chunk = await reader.read()
        if (chunk.done) break
        bytes += chunk.value.byteLength
        if (bytes > maximum) {await reader.cancel(); return oversized()}
        chunks.push(chunk.value)
      }
      body = new Uint8Array(Buffer.concat(chunks)).buffer
    }
    const query = request.nextUrl.searchParams.get('source_id')
    const suffix = query ? `?source_id=${encodeURIComponent(query)}` : ''
    const headers: Record<string, string> = {'Host': new URL(backend).host, 'Content-Type': endpoint === 'sources' && request.method === 'POST' ? 'application/octet-stream' : 'application/json'}
    if (request.headers.has('x-source-name')) headers['X-Source-Name'] = request.headers.get('x-source-name')!
    if (request.headers.get('x-aida-request') === '1') headers['X-AIDA-Request'] = '1'
    const cookie = request.headers.get('cookie')
    if (cookie) headers['Cookie'] = cookie
    const client = (request.headers.get('x-forwarded-for') || '').split(',')[0].trim() || request.headers.get('x-real-ip') || ''
    if (/^[0-9a-fA-F:.]{2,45}$/.test(client)) headers['X-AIDA-Client'] = client
    const response = await fetch(`${backend}/api/v1/${endpoint}${suffix}`, {method: request.method, headers, body, cache: 'no-store', redirect: 'manual', signal: AbortSignal.timeout(115000)})
    const result = NextResponse.json(await response.json(), {status: response.status, headers: {'Cache-Control': 'no-store'}})
    for (const value of response.headers.getSetCookie()) result.headers.append('Set-Cookie', value)
    const retry = response.headers.get('retry-after')
    if (retry) result.headers.set('Retry-After', retry)
    return result
  } catch (error) {
    const timeout = error instanceof Error && (error.name === 'TimeoutError' || error.name === 'AbortError')
    return NextResponse.json({error: timeout ? 'The model request timed out. Check the model service, then retry.' : 'The data service is unavailable. Start the backend, then try again.'}, {status: timeout ? 504 : 503})
  }
}
export const GET = proxy
export const POST = proxy
