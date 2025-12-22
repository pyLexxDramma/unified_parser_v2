@echo off
chcp 65001 >nul
echo ========================================
echo Запуск туннеля через LocalTunnel
echo ========================================
echo.

REM Проверка сервера на порту 8000
echo Проверка сервера на порту 8000...
netstat -ano | findstr ":8000" | findstr "LISTENING" >nul
if errorlevel 1 (
    echo ОШИБКА: Сервер не запущен на порту 8000!
    echo Сначала запустите: python run_server.py
    pause
    exit /b 1
)

echo Сервер запущен. Создаю туннель...
echo.
echo ========================================
echo Ссылка появится ниже через несколько секунд
echo Скопируйте её и отправьте заказчику
echo ========================================
echo.

REM Запускаем LocalTunnel через npx
npx localtunnel --port 8000

pause

