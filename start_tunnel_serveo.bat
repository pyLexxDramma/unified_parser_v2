@echo off
echo ========================================
echo Запуск туннеля через Serveo
echo ========================================
echo.
echo Сервер должен быть запущен на http://localhost:8000
echo.

REM Проверяем сервер
netstat -ano | findstr ":8000" | findstr "LISTENING" >nul
if errorlevel 1 (
    echo ОШИБКА: Сервер не запущен на порту 8000!
    echo Сначала запустите: python run_server.py
    pause
    exit /b 1
)

echo Сервер запущен. Создаю туннель через Serveo...
echo.
echo ========================================
echo Ссылка появится ниже через несколько секунд
echo ========================================
echo.

REM Запускаем SSH туннель
ssh -R 80:localhost:8000 serveo.net

pause

