$ErrorActionPreference = 'Stop'
$aidaRoot = Split-Path $PSScriptRoot -Parent
$aidaRecord = Join-Path $aidaRoot 'artifacts\demo-processes.json'
$aidaModelRecord = Join-Path $aidaRoot 'artifacts\model.pid'
if (-not (Test-Path -LiteralPath $aidaRecord) -and -not (Test-Path -LiteralPath $aidaModelRecord)) {
    Write-Host 'No AIDA process record found.'
    exit 0
}
$aidaProcesses = if (Test-Path -LiteralPath $aidaRecord) { Get-Content -LiteralPath $aidaRecord -Raw | ConvertFrom-Json } else { [pscustomobject]@{root=$aidaRoot} }
if ($aidaProcesses.root -ne $aidaRoot) { throw 'Process record belongs to another checkout.' }
$aidaStandaloneModelId = if (Test-Path -LiteralPath $aidaModelRecord) { [int](Get-Content -LiteralPath $aidaModelRecord) } else { $null }
$aidaProcessIds = @($aidaProcesses.backend, $aidaProcesses.frontend, $aidaProcesses.model, $aidaStandaloneModelId) | Where-Object { $_ } | Select-Object -Unique
foreach ($aidaProcessId in $aidaProcessIds) {
    if (-not $aidaProcessId) { continue }
    $aidaProcess = Get-CimInstance Win32_Process -Filter "ProcessId = $aidaProcessId"
    if (-not $aidaProcess) { continue }
    # Verify the recorded process still belongs to this demo before stopping it.
    $aidaCommand = $aidaProcess.CommandLine
    $aidaBackendMatch = $aidaCommand -like '*uvicorn backend.main:app*' -and $aidaCommand -like "*$aidaRoot*"
    $aidaFrontendMatch = ($aidaCommand -like '*next*start --hostname 127.0.0.1 --port 3000*' -or $aidaCommand -like '*frontend\scripts\start.cjs*') -and $aidaCommand -like "*$aidaRoot*"
    $aidaModelMatch = $aidaProcess.ExecutablePath -eq (Join-Path $aidaRoot '.runtime\llama\llama-server.exe') -and $aidaCommand -like "*$aidaRoot*Qwen3*gguf*"
    if (-not ($aidaBackendMatch -or $aidaFrontendMatch -or $aidaModelMatch)) {
        Write-Warning "Process $aidaProcessId no longer matches AIDA; leaving it running."
        continue
    }
    $aidaChildren = Get-CimInstance Win32_Process -Filter "ParentProcessId = $aidaProcessId"
    foreach ($aidaChild in $aidaChildren) {
        if ($aidaChild.CommandLine -like '*uvicorn backend.main:app*') {
            Stop-Process -Id $aidaChild.ProcessId -ErrorAction SilentlyContinue
        }
    }
    Stop-Process -Id $aidaProcessId -ErrorAction SilentlyContinue
}
Remove-Item -LiteralPath $aidaRecord -ErrorAction SilentlyContinue
Remove-Item -LiteralPath $aidaModelRecord -ErrorAction SilentlyContinue
Write-Host 'Stopped verified AIDA demo processes.'
