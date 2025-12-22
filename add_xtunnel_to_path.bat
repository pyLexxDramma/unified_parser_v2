@echo off
chcp 65001 >nul
echo ========================================
echo Добавление xTunnel в PATH
echo ========================================
echo.

set "TUNNEL_PATH=E:\Драйверы\xtunnel.win-x64.1.0.20"

REM Проверяем, есть ли уже в PATH
echo %PATH% | findstr /C:"%TUNNEL_PATH%" >nul
if %errorlevel% == 0 (
    echo xTunnel уже добавлен в PATH
    pause
    exit /b 0
)

REM Добавляем в PATH через setx
echo Добавление xTunnel в PATH...
setx PATH "%PATH%;%TUNNEL_PATH%" >nul 2>&1

if %errorlevel% == 0 (
    echo.
    echo ========================================
    echo xTunnel успешно добавлен в PATH!
    echo ========================================
    echo.
    echo ВАЖНО: Перезапустите PowerShell для применения изменений
    echo.
    echo После перезапуска вы сможете запускать:
    echo   xtunnel.exe 8000
    echo.
) else (
    echo.
    echo ОШИБКА: Не удалось добавить в PATH
    echo Попробуйте добавить вручную через:
    echo Пуск -^> Система -^> Дополнительные параметры системы
    echo.
)

pause

