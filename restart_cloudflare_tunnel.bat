@echo off
echo ========================================
echo Перезапуск Cloudflare Tunnel
echo ========================================
echo.

REM Останавливаем старые процессы
taskkill /F /IM cloudflared.exe >nul 2>&1
timeout /t 2 /nobreak >nul

echo Проверка сервера на порту 8000...
netstat -ano | findstr ":8000" | findstr "LISTENING" >nul
if errorlevel 1 (
    echo ОШИБКА: Сервер не запущен на порту 8000!
    echo Сначала запустите: python run_server.py
    pause
    exit /b 1
)

echo Сервер запущен. Создаю новый туннель...
echo.
echo ========================================
echo Ссылка появится ниже через несколько секунд
echo ========================================
echo.

REM Запускаем туннель с 127.0.0.1
cloudflared tunnel --url http://127.0.0.1:8000

pause

