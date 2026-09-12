// Run the exact standalone bundle used by Docker, with its browser assets.
const fs = require('node:fs')
const path = require('node:path')

const frontendRoot = path.resolve(__dirname, '..')
const standaloneRoot = path.join(frontendRoot, '.next', 'standalone')
const serverPath = path.join(standaloneRoot, 'server.js')
const staticPath = path.join(frontendRoot, '.next', 'static')

if (!fs.existsSync(serverPath) || !fs.existsSync(staticPath)) {
  console.error('AIDA has no production build yet. Run npm run build, then npm start.')
  process.exit(1)
}

fs.cpSync(staticPath, path.join(standaloneRoot, '.next', 'static'), {recursive: true, force: true})
const publicPath = path.join(frontendRoot, 'public')
if (fs.existsSync(publicPath)) fs.cpSync(publicPath, path.join(standaloneRoot, 'public'), {recursive: true, force: true})

process.env.NODE_ENV = 'production'
process.env.NEXT_TELEMETRY_DISABLED = '1'
process.env.HOSTNAME = process.env.HOSTNAME || '127.0.0.1'
process.env.PORT = process.env.PORT || '3000'
process.chdir(frontendRoot)
require(serverPath)
