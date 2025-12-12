# Скрипт для мониторинга теста парсинга
Write-Host "=== МОНИТОРИНГ ТЕСТА ПАРСИНГА ===" -ForegroundColor Cyan
Write-Host "Нажмите Ctrl+C для остановки`n" -ForegroundColor Yellow

$logFile = "test_pobeda_output.log"
$parserLog = "logs\parser.log"

while ($true) {
    Clear-Host
    Write-Host "=== МОНИТОРИНГ ТЕСТА ПАРСИНГА ===" -ForegroundColor Cyan
    Write-Host "Время: $(Get-Date -Format 'HH:mm:ss')`n" -ForegroundColor Gray
    
    if (Test-Path $logFile) {
        Write-Host "--- Последние 25 строк из test_pobeda_output.log ---" -ForegroundColor Yellow
        Get-Content $logFile -Tail 25 -Encoding UTF8
    } else {
        Write-Host "Лог файл test_pobeda_output.log еще не создан" -ForegroundColor Yellow
    }
    
    Write-Host "`n--- Статистика ---" -ForegroundColor Cyan
    if (Test-Path $logFile) {
        $content = Get-Content $logFile -Raw -Encoding UTF8
        $cardsFound = ([regex]::Matches($content, "Найдено карточек|Found.*cards|Total: \d+")).Count
        $filtered = ([regex]::Matches($content, "Отфильтровано|Фильтрация завершена")).Count
        $errors = ([regex]::Matches($content, "ERROR|Ошибка|Error")).Count
        
        Write-Host "Найдено упоминаний карточек: $cardsFound" -ForegroundColor White
        Write-Host "Фильтраций: $filtered" -ForegroundColor White
        Write-Host "Ошибок: $errors" -ForegroundColor $(if ($errors -gt 0) { "Red" } else { "Green" })
    }
    
    Write-Host "`nОбновление через 10 секунд... (Ctrl+C для остановки)" -ForegroundColor Gray
    Start-Sleep -Seconds 10
}


