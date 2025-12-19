# Инструкция по деплою парсера на тестовый домен

## Обзор

Проект настроен для работы в Docker с использованием:
- **Парсер приложение** - FastAPI приложение в отдельном контейнере
- **Selenium Grid** - отдельный контейнер для работы с браузером
- **Nginx** (опционально) - reverse proxy для продакшена

## Предварительные требования

1. Docker и Docker Compose установлены на сервере
2. GitHub Actions настроены с секретами (см. ниже)
3. SSH доступ к серверу

## Локальная разработка

### Запуск через Docker Compose

```bash
# Клонируем репозиторий
git clone <your-repo-url>
cd unified_parser_v2

# Создаем .env файл (скопируйте из .env.example)
cp .env.example .env
# Отредактируйте .env и установите необходимые переменные

# Запускаем все сервисы
docker-compose up -d

# Просмотр логов
docker-compose logs -f parser
docker-compose logs -f selenium

# Остановка
docker-compose down
```

### Доступ к приложению

- Парсер: http://localhost:8000
- Selenium Grid: http://localhost:4444
- VNC (для отладки браузера): http://localhost:7900 (пароль: secret)

## Настройка GitHub Actions для автоматического деплоя

### 1. Создайте секреты в GitHub

Перейдите в Settings → Secrets and variables → Actions и добавьте:

- `SERVER_HOST` - IP адрес или домен вашего сервера
- `SERVER_USER` - имя пользователя для SSH
- `SERVER_SSH_KEY` - приватный SSH ключ для доступа к серверу
- `SERVER_PORT` - порт SSH (обычно 22)
- `DEPLOY_PATH` - путь на сервере, куда деплоить (например, `/opt/unified_parser`)
- `DOCKER_USERNAME` - (опционально) логин Docker Hub
- `DOCKER_PASSWORD` - (опционально) пароль Docker Hub

### 2. Настройка сервера

На сервере выполните:

```bash
# Установите Docker и Docker Compose (если еще не установлены)
sudo apt-get update
sudo apt-get install -y docker.io docker-compose

# Добавьте пользователя в группу docker
sudo usermod -aG docker $USER

# Создайте директорию для проекта
sudo mkdir -p /opt/unified_parser
sudo chown $USER:$USER /opt/unified_parser

# Клонируйте репозиторий
cd /opt/unified_parser
git clone <your-repo-url> .

# Создайте .env файл
cp .env.example .env
nano .env  # Отредактируйте переменные окружения
```

### 3. Настройка переменных окружения

Создайте `.env` файл на сервере со следующими переменными:

```env
# Пароль для доступа к веб-интерфейсу
SITE_PASSWORD=your_secure_password_here

# Секретный ключ для сессий (сгенерируйте случайную строку)
SESSION_SECRET_KEY=your_random_secret_key_here

# Префикс URL (если приложение работает не в корне, например /parser)
URL_PREFIX=

# Настройки прокси (если нужны)
PROXY_ENABLED=false
PROXY_SERVER=
PROXY_PORT=8080
PROXY_USERNAME=
PROXY_PASSWORD=

# Настройки email (если нужны)
SMTP_SERVER=
SMTP_PORT=587
SMTP_USER=
SMTP_PASSWORD=
```

### 4. Первый запуск на сервере

```bash
cd /opt/unified_parser

# Запустите контейнеры
docker-compose up -d

# Проверьте статус
docker-compose ps

# Просмотрите логи
docker-compose logs -f
```

## Деплой через GitHub Actions

После настройки секретов, каждый push в ветку `main` или `develop` автоматически запустит деплой:

1. GitHub Actions соберет Docker образ
2. Подключится к серверу по SSH
3. Остановит старые контейнеры
4. Запустит новые контейнеры

Также можно запустить деплой вручную через GitHub Actions → Deploy to Test Server → Run workflow

## Настройка Nginx для продакшена

Для работы по адресу `https://test.pbd.space/parser/login`:

1. **Настройте SSL сертификаты** (см. [nginx/SSL_SETUP.md](nginx/SSL_SETUP.md))
   ```bash
   mkdir -p nginx/ssl
   # Скопируйте ваши SSL сертификаты в nginx/ssl/
   ```

2. **Убедитесь, что в `.env` установлен `URL_PREFIX=/parser`**

3. **Раскомментируйте сервис nginx в `docker-compose.yml`** (уберите `profiles: - production`)

4. **Настройте DNS**: убедитесь, что `test.pbd.space` указывает на IP вашего сервера

5. **Запустите контейнеры**:
   ```bash
   docker-compose up -d
   ```

6. **Проверьте доступность**:
   ```bash
   curl -I https://test.pbd.space/parser/login
   ```

## Мониторинг и логи

### Просмотр логов

```bash
# Все сервисы
docker-compose logs -f

# Только парсер
docker-compose logs -f parser

# Только Selenium
docker-compose logs -f selenium

# Только Nginx
docker-compose logs -f nginx
```

### Проверка здоровья сервисов

```bash
# Статус контейнеров
docker-compose ps

# Проверка парсера
curl http://localhost:8000/login

# Проверка Selenium
curl http://localhost:4444/wd/hub/status
```

## Обновление приложения

### Автоматическое обновление

Просто сделайте push в ветку `main` или `develop` - GitHub Actions автоматически задеплоит изменения.

### Ручное обновление

```bash
cd /opt/unified_parser
git pull
docker-compose down
docker-compose up -d --build
```

## Решение проблем

### Парсер не может подключиться к Selenium

Проверьте:
1. Selenium контейнер запущен: `docker-compose ps`
2. Переменная `SELENIUM_REMOTE_URL` установлена правильно
3. Оба контейнера в одной сети: `docker network ls`

### Ошибки при сборке образа

```bash
# Очистите кеш Docker
docker system prune -a

# Пересоберите образ
docker-compose build --no-cache
```

### Проблемы с правами доступа

```bash
# Убедитесь, что директории output и logs доступны для записи
sudo chown -R $USER:$USER /opt/unified_parser/output
sudo chown -R $USER:$USER /opt/unified_parser/logs
```

## Структура проекта

```
unified_parser_v2/
├── Dockerfile              # Образ для парсера
├── docker-compose.yml      # Конфигурация всех сервисов
├── .dockerignore           # Исключения для Docker
├── .github/
│   └── workflows/
│       └── deploy.yml     # GitHub Actions workflow
├── nginx/                  # Конфигурация Nginx
│   ├── nginx.conf
│   └── conf.d/
│       └── default.conf
├── config/                 # Конфигурация приложения
├── output/                 # Результаты парсинга (volume)
└── logs/                   # Логи приложения (volume)
```

## Дополнительные настройки

### Увеличение лимитов для длительных операций

В `docker-compose.yml` можно добавить:

```yaml
parser:
  deploy:
    resources:
      limits:
        memory: 4G
      reservations:
        memory: 2G
```

### Настройка автоматического перезапуска

Контейнеры уже настроены на `restart: unless-stopped`, что означает автоматический перезапуск при сбое.

## Безопасность

1. **Измените пароль по умолчанию** в `.env` файле
2. **Используйте HTTPS** в продакшене (настройте SSL сертификаты в Nginx)
3. **Ограничьте доступ** к портам 4444 (Selenium) и 7900 (VNC) только для внутренней сети
4. **Регулярно обновляйте** Docker образы для безопасности

## Поддержка

При возникновении проблем проверьте:
1. Логи контейнеров
2. Статус контейнеров (`docker-compose ps`)
3. Доступность портов
4. Переменные окружения в `.env`
