# Скрипт для запуска Cloudflare Tunnel для локального сервера
# Использование: .\start_cloudflare_tunnel.ps1

Write-Host "Запуск Cloudflare Tunnel для порта 8000..." -ForegroundColor Green
Write-Host "Сервер должен быть запущен на http://localhost:8000" -ForegroundColor Yellow
Write-Host ""

# Проверяем, запущен ли сервер
$serverRunning = netstat -ano | Select-String "LISTENING" | Select-String ":8000"
if (-not $serverRunning) {
    Write-Host "ОШИБКА: Сервер не запущен на порту 8000!" -ForegroundColor Red
    Write-Host "Сначала запустите: python run_server.py" -ForegroundColor Yellow
    exit 1
}

Write-Host "Сервер запущен. Создаю туннель..." -ForegroundColor Green
Write-Host ""

# Запускаем быстрый туннель (quick tunnel)
# Это создаст временный туннель без необходимости регистрации
cloudflared tunnel --url http://localhost:8000

