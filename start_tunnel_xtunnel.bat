@echo off
chcp 65001 >nul
echo ========================================
echo Запуск туннеля через xTunnel
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

REM Запускаем xTunnel на порт 8000
REM Если xtunnel.exe не в PATH, используем полный путь
where.exe xtunnel.exe >nul 2>&1
if errorlevel 1 (
    REM Пробуем найти в стандартных местах
    if exist "E:\Драйверы\xtunnel.win-x64.1.0.20\xtunnel.exe" (
        "E:\Драйверы\xtunnel.win-x64.1.0.20\xtunnel.exe" 8000
    ) else (
        echo ОШИБКА: xtunnel.exe не найден!
        echo Убедитесь, что xTunnel установлен и добавлен в PATH
        echo Или укажите полный путь к xtunnel.exe в этом файле
        pause
        exit /b 1
    )
) else (
    xtunnel.exe 8000
)

pause

