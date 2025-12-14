# Чеклист для запуска парсера на удаленном сервере

## ✅ Проверки перед запуском

### 1. Файлы проекта
- [x] `run_server.py` - исправлен, синтаксис корректен
- [x] `src/webapp/app.py` - основной файл приложения
- [x] `src/config/settings.py` - настройки
- [x] `src/parsers/yandex_parser.py` - парсер Яндекс
- [x] `src/parsers/gis_parser.py` - парсер 2GIS
- [x] `INSTRUCTIONS.md` - инструкция по использованию

### 2. Зависимости Python
Убедитесь, что установлены:
- `fastapi`
- `uvicorn`
- `selenium`
- `beautifulsoup4`
- `pydantic`
- `jinja2`
- `reportlab`
- `starlette`

### 3. Проверка на сервере

Выполните на сервере:

```bash
# 1. Перейти в директорию проекта
cd /var/www/www-root/data/www/ai.pbd.space/unified_parser_v2

# 2. Активировать виртуальное окружение
source venv/bin/activate

# 3. Проверить синтаксис run_server.py
python -m py_compile run_server.py

# 4. Проверить импорты
python -c "from src.webapp.app import app; print('OK')"

# 5. Проверить запуск (тест, не запускать в фоне)
python run_server.py
# (остановить через Ctrl+C)

# 6. Если все ОК, перезапустить сервис
sudo systemctl restart unified-parser.service

# 7. Проверить статус
sudo systemctl status unified-parser.service
```

### 4. Возможные проблемы

**Проблема**: `ModuleNotFoundError`
**Решение**: Установить зависимости: `pip install -r requirements.txt`

**Проблема**: `IndentationError`
**Решение**: Убедиться, что файл `run_server.py` обновлен через `git pull`

**Проблема**: `Permission denied`
**Решение**: Проверить права доступа к файлам и директориям

**Проблема**: Порт 8000 занят
**Решение**: Проверить: `sudo netstat -tulpn | grep 8000`

### 5. Логи для диагностики

```bash
# Последние логи сервиса
sudo journalctl -u unified-parser.service -n 100 --no-pager

# Логи в реальном времени
sudo journalctl -u unified-parser.service -f

# Логи парсера
tail -f logs/parser.log
```

## ✅ Статус готовности

- [x] Файл `run_server.py` исправлен и проверен
- [x] Все ненужные MD файлы удалены (оставлен только `INSTRUCTIONS.md`)
- [x] Синтаксис Python корректен
- [x] Основные файлы на месте

**Готово к запуску на сервере!**

