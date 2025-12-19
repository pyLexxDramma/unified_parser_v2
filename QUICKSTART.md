# Быстрый старт

## Локальный запуск через Docker

```bash
# 1. Клонируйте репозиторий
git clone <your-repo-url>
cd unified_parser_v2

# 2. Создайте .env файл
cp env.docker.example .env
# Отредактируйте .env и установите:
# - SITE_PASSWORD (пароль для доступа)
# - URL_PREFIX=/parser (если деплоите на test.pbd.space/parser)

# 3. Запустите контейнеры
docker-compose up -d

# 4. Откройте браузер
# http://localhost:8000
```

## Проверка работы

```bash
# Проверка статуса контейнеров
docker-compose ps

# Просмотр логов
docker-compose logs -f parser

# Проверка Selenium
curl http://localhost:4444/wd/hub/status

# Проверка парсера
curl http://localhost:8000/login
```

## Остановка

```bash
docker-compose down
```

## Для разработки (с монтированием кода)

```bash
docker-compose -f docker-compose.dev.yml up -d
```

## Деплой на сервер

См. подробную инструкцию в [DOCKER_DEPLOY.md](DOCKER_DEPLOY.md)
