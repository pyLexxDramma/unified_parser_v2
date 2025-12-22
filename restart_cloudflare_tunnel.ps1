# Скрипт для перезапуска Cloudflare Tunnel
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "Перезапуск Cloudflare Tunnel" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""

# Останавливаем старые процессы
Get-Process | Where-Object {$_.ProcessName -like "*cloudflared*"} | Stop-Process -Force -ErrorAction SilentlyContinue
Start-Sleep -Seconds 2

Write-Host "Проверка сервера на порту 8000..." -ForegroundColor Yellow
$serverRunning = netstat -ano | Select-String "LISTENING" | Select-String ":8000"
if (-not $serverRunning) {
    Write-Host "ОШИБКА: Сервер не запущен на порту 8000!" -ForegroundColor Red
    Write-Host "Сначала запустите: python run_server.py" -ForegroundColor Yellow
    exit 1
}

Write-Host "Сервер запущен. Создаю новый туннель..." -ForegroundColor Green
Write-Host ""
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "Ссылка появится ниже через несколько секунд" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""

# Запускаем туннель
cd "D:\Working Flow\unified_parser_v2"
cloudflared tunnel --url http://127.0.0.1:8000

