import sys
import os
import io

# Устанавливаем переменную окружения для отключения буферизации
os.environ['PYTHONUNBUFFERED'] = '1'

# Настраиваем кодировку для Windows PowerShell
if sys.platform == 'win32':
    # Пытаемся установить UTF-8 для консоли Windows
    try:
        # Для Python 3.7+
        if hasattr(sys.stdout, 'reconfigure'):
            sys.stdout.reconfigure(line_buffering=True, encoding='utf-8', errors='replace')
        else:
            # Для старых версий Python
            sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace', line_buffering=True)
    except Exception:
        pass
    
    try:
        if hasattr(sys.stderr, 'reconfigure'):
            sys.stderr.reconfigure(line_buffering=True, encoding='utf-8', errors='replace')
        else:
            sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace', line_buffering=True)
    except Exception:
        pass
else:
    # Для Linux/Mac
if hasattr(sys.stdout, 'reconfigure'):
    try:
        sys.stdout.reconfigure(line_buffering=True, encoding='utf-8')
    except:
        pass

if __name__ == "__main__":
    import uvicorn
    import logging
    
    # Настраиваем uvicorn для вывода логов в реальном времени
    # Отключаем стандартное логирование uvicorn, чтобы использовать наше
    uvicorn_logger = logging.getLogger("uvicorn")
    uvicorn_logger.setLevel(logging.INFO)
    uvicorn_access_logger = logging.getLogger("uvicorn.access")
    uvicorn_access_logger.setLevel(logging.INFO)
    
    uvicorn.run(
        "src.webapp.app:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info",
        access_log=True,
        use_colors=False,  # Отключаем цвета для лучшей совместимости
        log_config=None  # Используем настройки логирования из settings.py
    )


