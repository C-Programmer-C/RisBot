$Root = Split-Path -Parent $PSScriptRoot
$ProjectRoot = Split-Path -Parent $Root
$Frontend = Join-Path $Root "frontend"

Write-Host "Starting bot server (API + webhook) on :60080..." -ForegroundColor Cyan
Start-Process powershell -ArgumentList "-NoExit", "-Command", "cd '$ProjectRoot'; python -m server.main"

Start-Sleep -Seconds 2

Write-Host "Starting frontend dev server on :5173..." -ForegroundColor Cyan
Start-Process powershell -ArgumentList "-NoExit", "-Command", "cd '$Frontend'; npm run dev"

Write-Host "Done." -ForegroundColor Green
Write-Host "  Bot/API:  http://127.0.0.1:60080  (/webhook, /delete, /api, /info)" -ForegroundColor Green
Write-Host "  Frontend: http://127.0.0.1:5173  (proxy /api, /info -> :60080)" -ForegroundColor Green
