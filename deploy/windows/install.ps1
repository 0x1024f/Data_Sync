param([Parameter(Mandatory=$true)][string]$ServiceDirectory)
$ErrorActionPreference = 'Stop'
$directory = (Resolve-Path $ServiceDirectory).Path
$exe = Join-Path $directory 'data-sync-service.exe'
if (!(Test-Path (Join-Path $directory 'config.yaml'))) { throw 'Create config.yaml first' }
& $exe --startup auto install
if ($LASTEXITCODE -ne 0) { throw 'Service installation failed' }
sc.exe failure DataSyncAgent reset= 86400 actions= restart/60000/restart/60000/restart/300000
if ($LASTEXITCODE -ne 0) { throw 'Recovery configuration failed' }
sc.exe failureflag DataSyncAgent 1
Write-Host 'Installed. Configure the service account and its environment before Start-Service DataSyncAgent.'
