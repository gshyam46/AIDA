param([string]$GpuLayers = 'all')
$ErrorActionPreference = 'Stop'
$aidaRoot = Split-Path $PSScriptRoot -Parent
$aidaExecutable = Join-Path $aidaRoot '.runtime\llama\llama-server.exe'
$aidaManifest = Get-Content -LiteralPath (Join-Path $PSScriptRoot 'model-runtime.json') -Raw | ConvertFrom-Json
$aidaModel = Join-Path $aidaRoot ('.runtime\' + $aidaManifest.model.file)
$aidaArtifacts = Join-Path $aidaRoot 'artifacts'
New-Item -ItemType Directory -Force -Path $aidaArtifacts | Out-Null
if (-not (Test-Path -LiteralPath $aidaExecutable) -or -not (Test-Path -LiteralPath $aidaModel)) {
    throw 'Run scripts/setup-model.ps1 first to install the pinned local model and runtime.'
}
$aidaListener = Get-NetTCPConnection -LocalPort 8081 -State Listen -ErrorAction SilentlyContinue
if ($aidaListener) {
    $aidaExisting = Get-CimInstance Win32_Process -Filter "ProcessId = $($aidaListener[0].OwningProcess)"
    $aidaPublicListeners = @($aidaListener | Where-Object { $_.LocalAddress -notin @('127.0.0.1', '::1') })
    $aidaCacheArguments = [regex]::Matches($aidaExisting.CommandLine, '(?:^|\s)(?:--cache-ram|-cram)(?:\s+|=)(\S+)')
    $aidaBoundedCache = $aidaCacheArguments.Count -eq 1 -and $aidaCacheArguments[0].Groups[1].Value -eq '0'
    if ($aidaExisting.ExecutablePath -ne $aidaExecutable -or $aidaExisting.CommandLine -notlike "*$aidaModel*" -or $aidaPublicListeners.Count -gt 0 -or $aidaExisting.CommandLine -notlike '*--log-disable*' -or $aidaExisting.CommandLine -notlike '*--no-webui*' -or $aidaExisting.CommandLine -notlike '*--no-slots*' -or -not $aidaBoundedCache) {
        throw 'Port 8081 does not belong to the expected private AIDA model configuration. Stop that service before starting the local model.'
    }
    $aidaModelId = $aidaExisting.ProcessId
} else {
    if ($GpuLayers -notmatch '^(all|auto|[0-9]+)$') { throw 'GpuLayers must be all, auto, or a nonnegative layer count.' }
    # AIDA caches exact semantic plans itself. Disable the runtime's auxiliary
    # multi-prompt RAM cache so changing catalogs cannot retain gigabytes of KV snapshots.
    $aidaArgs = "-m `"$aidaModel`" --alias aida-semantic --host 127.0.0.1 --port 8081 -c 4096 -np 1 -t 4 -ngl $GpuLayers --cache-ram 0 --no-webui --no-slots --log-disable"
    $aidaProcess = Start-Process -FilePath $aidaExecutable -ArgumentList $aidaArgs -WorkingDirectory $aidaRoot -WindowStyle Hidden -PassThru -RedirectStandardOutput (Join-Path $aidaArtifacts 'model.stdout.log') -RedirectStandardError (Join-Path $aidaArtifacts 'model.stderr.log')
    $aidaModelId = $aidaProcess.Id
}
$aidaModelId | Set-Content -LiteralPath (Join-Path $aidaArtifacts 'model.pid')
for ($aidaAttempt = 0; $aidaAttempt -lt 120; $aidaAttempt++) {
    if (-not (Get-Process -Id $aidaModelId -ErrorAction SilentlyContinue)) { throw 'Local model stopped during startup. Try -GpuLayers 0 for CPU inference.' }
    try {
        $aidaStatus = Invoke-RestMethod 'http://127.0.0.1:8081/health' -TimeoutSec 2
        if ($aidaStatus.status -eq 'ok') { Write-Host "Local semantic model ready (PID $aidaModelId)."; return }
    } catch { }
    Start-Sleep -Seconds 1
}
throw 'Model is still loading. Check the local process and retry startup; no hosted fallback is used.'
