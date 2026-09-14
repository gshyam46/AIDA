# Dot-source from the launcher. The encryption key is protected using Windows DPAPI
# for this Windows account; the encrypted file can survive restarts without prompts.
$aidaKeyFile = Join-Path (Split-Path $PSScriptRoot -Parent) '.runtime\connector-key.dpapi'
if (-not $env:AIDA_CONNECTOR_KEY) {
    Add-Type -AssemblyName System.Security
    $aidaKeyScope = [System.Security.Cryptography.DataProtectionScope]::CurrentUser
    if (Test-Path -LiteralPath $aidaKeyFile) {
        $aidaEncryptedKey = [System.IO.File]::ReadAllBytes($aidaKeyFile)
        $aidaKeyBytes = [System.Security.Cryptography.ProtectedData]::Unprotect($aidaEncryptedKey, $null, $aidaKeyScope)
    } else {
        New-Item -ItemType Directory -Force -Path (Split-Path $aidaKeyFile -Parent) | Out-Null
        $aidaKeyBytes = New-Object byte[] 32
        $aidaRandom = [System.Security.Cryptography.RandomNumberGenerator]::Create()
        try { $aidaRandom.GetBytes($aidaKeyBytes) } finally { $aidaRandom.Dispose() }
        $aidaEncryptedKey = [System.Security.Cryptography.ProtectedData]::Protect($aidaKeyBytes, $null, $aidaKeyScope)
        [System.IO.File]::WriteAllBytes($aidaKeyFile, $aidaEncryptedKey)
    }
    $env:AIDA_CONNECTOR_KEY = [Convert]::ToBase64String($aidaKeyBytes).Replace('+', '-').Replace('/', '_')
    [Array]::Clear($aidaKeyBytes, 0, $aidaKeyBytes.Length)
    Remove-Variable aidaKeyBytes, aidaEncryptedKey
}
