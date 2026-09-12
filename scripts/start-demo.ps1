param([switch]$SkipInstall, [switch]$SkipBuild, [switch]$PublicDemo, [string]$GpuLayers = 'all')
$ErrorActionPreference = 'Stop'
$aidaRoot = Split-Path $PSScriptRoot -Parent
Set-Location -LiteralPath $aidaRoot
$env:NEXT_TELEMETRY_DISABLED = '1'
$env:AIDA_PUBLIC_DEMO = if ($PublicDemo) { '1' } else { '0' }
# This launcher owns a local topology. Do not inherit unrelated shell/server
# settings that could expose the UI or forward private questions off-machine.
$env:HOSTNAME = '127.0.0.1'
$env:PORT = '3000'
$env:BACKEND_URL = 'http://127.0.0.1:8000'
$env:AIDA_MODEL_ENDPOINT = 'http://127.0.0.1:8081/v1/chat/completions'
$env:AIDA_MODEL_NAME = 'aida-semantic'
$aidaPython = Join-Path $aidaRoot '.venv\Scripts\python.exe'
$aidaFrontend = Join-Path $aidaRoot 'frontend'
$aidaArtifacts = Join-Path $aidaRoot 'artifacts'
New-Item -ItemType Directory -Force -Path $aidaArtifacts | Out-Null

foreach ($aidaPort in @(8000, 3000)) {
    if (Get-NetTCPConnection -LocalPort $aidaPort -State Listen -ErrorAction SilentlyContinue) {
        throw "Port $aidaPort is already in use. Stop the existing demo with scripts/stop-demo.ps1 or free that port."
    }
}
if (-not $SkipInstall) {
    & (Join-Path $PSScriptRoot 'setup-model.ps1')
    if (-not (Test-Path -LiteralPath $aidaPython)) {
        & python -m venv .venv
        if ($LASTEXITCODE -ne 0) { throw 'Python 3.11+ is required.' }
    }
    & $aidaPython -m pip install -r backend/requirements-dev.txt
    if ($LASTEXITCODE -ne 0) { throw 'Backend dependency installation failed.' }
    Push-Location -LiteralPath $aidaFrontend
    try {
        & npm.cmd ci --cache .npm-cache
        if ($LASTEXITCODE -ne 0) { throw 'Frontend dependency installation failed.' }
    } finally { Pop-Location }
}

& (Join-Path $PSScriptRoot 'start-model.ps1') -GpuLayers $GpuLayers
$aidaModelId = [int](Get-Content -LiteralPath (Join-Path $aidaArtifacts 'model.pid'))
$aidaProcessRecord = Join-Path $aidaArtifacts 'demo-processes.json'
$aidaProcessState = @{model=$aidaModelId; root=$aidaRoot}
$aidaProcessState | ConvertTo-Json | Set-Content -LiteralPath $aidaProcessRecord
if (-not $SkipBuild) {
    Push-Location -LiteralPath $aidaFrontend
    try {
        & npm.cmd run build
        if ($LASTEXITCODE -ne 0) { throw 'Frontend build failed.' }
    } finally { Pop-Location }
}

$aidaBackend = Start-Process -FilePath $aidaPython -ArgumentList '-m uvicorn backend.main:app --host 127.0.0.1 --port 8000 --no-access-log' -WorkingDirectory $aidaRoot -WindowStyle Hidden -RedirectStandardOutput (Join-Path $aidaArtifacts 'backend.log') -RedirectStandardError (Join-Path $aidaArtifacts 'backend-error.log') -PassThru
$aidaProcessState.backend = $aidaBackend.Id
$aidaProcessState | ConvertTo-Json | Set-Content -LiteralPath $aidaProcessRecord
$aidaNode = (Get-Command node.exe).Source
$aidaNext = Join-Path $aidaFrontend 'scripts\start.cjs'
$aidaWeb = Start-Process -FilePath $aidaNode -ArgumentList "`"$aidaNext`"" -WorkingDirectory $aidaFrontend -WindowStyle Hidden -RedirectStandardOutput (Join-Path $aidaArtifacts 'frontend.log') -RedirectStandardError (Join-Path $aidaArtifacts 'frontend-error.log') -PassThru
$aidaProcessState.frontend = $aidaWeb.Id
$aidaProcessState | ConvertTo-Json | Set-Content -LiteralPath $aidaProcessRecord

for ($aidaAttempt = 0; $aidaAttempt -lt 30; $aidaAttempt++) {
    try {
        $aidaHealth = Invoke-RestMethod 'http://127.0.0.1:3000/api/v1/health' -TimeoutSec 2
        if ($aidaHealth.status -eq 'healthy' -and $aidaHealth.model.available) {
            Write-Host 'AIDA is ready at http://127.0.0.1:3000'
            Write-Host 'Qwen3 4B interprets questions locally. Validated code compiles and executes SQL.'
            Write-Host "Public synthetic-only mode: $PublicDemo. No hosted inference key required."
            Write-Host 'Stop: powershell -ExecutionPolicy Bypass -File scripts/stop-demo.ps1'
            exit 0
        }
    } catch { Start-Sleep -Seconds 1 }
}
throw 'Startup did not complete. Check artifacts/backend-error.log and artifacts/frontend-error.log. Run scripts/stop-demo.ps1 before retrying.'
