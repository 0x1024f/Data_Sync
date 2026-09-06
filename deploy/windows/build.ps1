$ErrorActionPreference = 'Stop'
Set-Location (Join-Path $PSScriptRoot '../..')
python -m pip install '.[windows,test]'
if ($LASTEXITCODE -ne 0) { throw 'Dependency installation failed' }
python -m pytest
if ($LASTEXITCODE -ne 0) { throw 'Tests failed' }
python -m PyInstaller --noconfirm --clean --onedir --name data-sync --paths . tools/agent_entry.py
if ($LASTEXITCODE -ne 0) { throw 'CLI build failed' }
python -m PyInstaller --noconfirm --clean --onedir --name data-sync-service --paths . --hidden-import win32timezone --hidden-import servicemanager tools/service_entry.py
if ($LASTEXITCODE -ne 0) { throw 'Service build failed' }
Copy-Item config.example.yaml dist/data-sync-service/config.example.yaml
Write-Host 'Built dist/data-sync and dist/data-sync-service. Edit config.yaml before installation.'
