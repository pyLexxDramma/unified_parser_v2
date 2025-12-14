#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Запуск сервера с выводом логов в терминал
"""
import sys
import os

os.environ['PYTHONUNBUFFERED'] = '1'

if hasattr(sys.stdout, 'reconfigure'):
    try:
        sys.stdout.reconfigure(line_buffering=True, encoding='utf-8')
    except:
        pass

if __name__ == "__main__":
    import uvicorn
    import logging
    
    # Настраиваем логирование для вывода в терминал
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s | %(levelname)-8s | %(message)s',
        datefmt='%d/%m/%Y %H:%M:%S',
        handlers=[
            logging.StreamHandler(sys.stdout)  # Вывод в терминал
        ],
        force=True
    )
    
    # Настраиваем uvicorn для вывода логов
    uvicorn_logger = logging.getLogger("uvicorn")
    uvicorn_logger.setLevel(logging.INFO)
    uvicorn_logger.addHandler(logging.StreamHandler(sys.stdout))
    
    uvicorn_access_logger = logging.getLogger("uvicorn.access")
    uvicorn_access_logger.setLevel(logging.INFO)
    uvicorn_access_logger.addHandler(logging.StreamHandler(sys.stdout))
    
    print("=" * 80)
    print("🚀 ЗАПУСК СЕРВЕРА С ВЫВОДОМ ЛОГОВ В ТЕРМИНАЛ")
    print("=" * 80)
    print(f"URL: http://localhost:8000")
    print("Логи будут выводиться здесь в реальном времени")
    print("=" * 80)
    print()
    
    uvicorn.run(
        "src.webapp.app:app",
        host="0.0.0.0",
        port=8000,
        reload=False,  # Отключаем reload для стабильности
        log_level="info",
        access_log=True,
        use_colors=False,
        log_config=None
    )






















