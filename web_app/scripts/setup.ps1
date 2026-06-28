$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent $PSScriptRoot

function Require-Command($Name) {
    if (-not (Get-Command $Name -ErrorAction SilentlyContinue)) {
        throw "Не найден $Name. Установи и добавь в PATH."
    }
}

Write-Host "=== Setup: Sales Report Web App ===" -ForegroundColor Cyan

Require-Command node
Require-Command npm
Require-Command python

Write-Host "`n[1/4] Backend venv..." -ForegroundColor Yellow
Set-Location (Join-Path $Root "backend")
if (-not (Test-Path ".venv")) {
    python -m venv .venv
}
.\.venv\Scripts\python.exe -m pip install --upgrade pip
.\.venv\Scripts\pip.exe install -r requirements.txt
if (-not (Test-Path ".env")) {
    Copy-Item ".env.example" ".env"
}

Write-Host "`n[2/4] Frontend deps..." -ForegroundColor Yellow
Set-Location (Join-Path $Root "frontend")
npm install
if (-not (Test-Path ".env")) {
    Copy-Item ".env.example" ".env"
}

Write-Host "`n[3/4] Verify backend..." -ForegroundColor Yellow
Set-Location (Join-Path $Root "backend")
.\.venv\Scripts\python.exe -c "from app.main import app; print('Backend OK')"

Write-Host "`n[4/4] Verify frontend build..." -ForegroundColor Yellow
Set-Location (Join-Path $Root "frontend")
npm run build

Set-Location $Root
Write-Host "`nГотово! Запуск: .\scripts\dev.ps1" -ForegroundColor Green
