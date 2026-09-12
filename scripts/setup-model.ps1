$ErrorActionPreference = 'Stop'
$aidaRoot = Split-Path $PSScriptRoot -Parent
$aidaRuntime = Join-Path $aidaRoot '.runtime'
$aidaManifest = Get-Content -LiteralPath (Join-Path $PSScriptRoot 'model-runtime.json') -Raw | ConvertFrom-Json
New-Item -ItemType Directory -Force -Path $aidaRuntime | Out-Null

function Get-AidaVerifiedDownload($Url, $Destination, $Sha256) {
    if ((Test-Path -LiteralPath $Destination) -and (Get-FileHash -LiteralPath $Destination -Algorithm SHA256).Hash.ToLower() -eq $Sha256) {
        Write-Host "Verified existing $(Split-Path $Destination -Leaf)"
        return
    }
    $aidaPartial = "$Destination.part"
    & curl.exe --silent --show-error --fail --location --retry 2 --connect-timeout 30 --continue-at - --output $aidaPartial $Url
    if ($LASTEXITCODE -ne 0) { throw "Download failed. Re-run setup-model.ps1 to resume." }
    if ((Get-FileHash -LiteralPath $aidaPartial -Algorithm SHA256).Hash.ToLower() -ne $Sha256) {
        throw 'Downloaded artifact checksum did not match the pinned manifest.'
    }
    Move-Item -LiteralPath $aidaPartial -Destination $Destination -Force
}

Write-Host 'Installing a project-local model runtime and 2.5 GB model. Downloads contain software/model weights only.'
$aidaArchive = Join-Path $aidaRuntime 'llama-runtime.zip'
Get-AidaVerifiedDownload $aidaManifest.runtime.url $aidaArchive $aidaManifest.runtime.sha256
Expand-Archive -LiteralPath $aidaArchive -DestinationPath (Join-Path $aidaRuntime 'llama') -Force
Get-AidaVerifiedDownload $aidaManifest.model.url (Join-Path $aidaRuntime $aidaManifest.model.file) $aidaManifest.model.sha256
Write-Host 'Local model installed and checksums verified. No hosted inference key is required.'
