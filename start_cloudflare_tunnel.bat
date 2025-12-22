@echo off
echo Запуск Cloudflare Tunnel для порта 8000...
echo.
echo Сервер должен быть запущен на http://localhost:8000
echo.

REM Проверяем, запущен ли сервер
netstat -ano | findstr ":8000" | findstr "LISTENING" >nul
if errorlevel 1 (
    echo ОШИБКА: Сервер не запущен на порту 8000!
    echo Сначала запустите: python run_server.py
    pause
    exit /b 1
)

echo Сервер запущен. Создаю туннель...
echo.

REM Запускаем быстрый туннель
cloudflared tunnel --url http://localhost:8000

pause

