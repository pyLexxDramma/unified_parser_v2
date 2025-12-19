# Сводка настроек для деплоя на test.pbd.space/parser

## Что было настроено

### 1. Nginx конфигурация
- ✅ Настроен для работы с доменом `test.pbd.space`
- ✅ Настроен префикс `/parser` для всех маршрутов
- ✅ Настроен редирект с HTTP на HTTPS
- ✅ Настроена поддержка SSL сертификатов

### 2. Docker Compose
- ✅ Парсер и Selenium синхронизированы в одной сети
- ✅ Nginx настроен как reverse proxy
- ✅ По умолчанию установлен `URL_PREFIX=/parser`
- ✅ Volumes для output, logs и config

### 3. Приложение
- ✅ Поддержка работы с префиксом через `URL_PREFIX`
- ✅ Поддержка заголовка `X-Forwarded-Prefix` из Nginx
- ✅ Правильная генерация URL с учетом префикса

## Быстрый старт для деплоя

### 1. На сервере

```bash
# Клонируйте репозиторий
cd /opt/unified_parser
git clone <your-repo-url> .

# Создайте .env файл
cp env.docker.example .env
nano .env  # Установите SITE_PASSWORD и проверьте URL_PREFIX=/parser

# Настройте SSL сертификаты (см. nginx/SSL_SETUP.md)
mkdir -p nginx/ssl
# Скопируйте сертификаты в nginx/ssl/

# Запустите контейнеры
docker-compose up -d
```

### 2. Проверка

```bash
# Проверьте доступность
curl -I https://test.pbd.space/parser/login

# Откройте в браузере
# https://test.pbd.space/parser/login
```

## Важные файлы

- `docker-compose.yml` - конфигурация всех сервисов
- `nginx/conf.d/default.conf` - конфигурация Nginx для test.pbd.space/parser
- `env.docker.example` - пример переменных окружения
- `.env` - ваши настройки (не коммитится в git)
- `nginx/SSL_SETUP.md` - инструкция по настройке SSL
- `DEPLOYMENT_CHECKLIST.md` - подробный чеклист деплоя

## Переменные окружения

**Обязательные:**
- `SITE_PASSWORD` - пароль для доступа к веб-интерфейсу
- `SESSION_SECRET_KEY` - секретный ключ для сессий
- `URL_PREFIX=/parser` - префикс URL (уже установлен по умолчанию)

**Опциональные:**
- `PROXY_ENABLED`, `PROXY_SERVER`, etc. - настройки прокси
- `SMTP_SERVER`, `SMTP_USER`, etc. - настройки email

## Структура URL

После деплоя приложение будет доступно по адресам:

- `https://test.pbd.space/parser/login` - страница входа
- `https://test.pbd.space/parser/` - главная страница (после входа)
- `https://test.pbd.space/parser/tasks/{task_id}` - страница задачи
- `https://test.pbd.space/parser/static/...` - статические файлы

## Автоматический деплой

После настройки GitHub Secrets, каждый push в `main` или `develop` автоматически:
1. Соберет Docker образ
2. Подключится к серверу по SSH
3. Остановит старые контейнеры
4. Запустит новые контейнеры

## Поддержка

При проблемах см.:
- `DOCKER_DEPLOY.md` - подробная инструкция
- `DEPLOYMENT_CHECKLIST.md` - чеклист и решение проблем
- `nginx/SSL_SETUP.md` - настройка SSL сертификатов
