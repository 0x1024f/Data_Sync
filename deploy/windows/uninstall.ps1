param([Parameter(Mandatory=$true)][string]$ServiceDirectory)
$ErrorActionPreference = 'Stop'
if ((Get-Service DataSyncAgent -ErrorAction SilentlyContinue)) {
    Stop-Service DataSyncAgent
}
& (Join-Path (Resolve-Path $ServiceDirectory).Path 'data-sync-service.exe') remove
if ($LASTEXITCODE -ne 0) { throw 'Service removal failed' }
Write-Host 'Service removed. Configuration, logs and state are retained.'
