# Чеклист для деплоя на test.pbd.space/parser

## Предварительная подготовка

- [ ] Docker и Docker Compose установлены на сервере
- [ ] DNS запись для `test.pbd.space` настроена и указывает на IP сервера
- [ ] SSH доступ к серверу настроен
- [ ] GitHub Secrets настроены для автоматического деплоя

## Настройка на сервере

### 1. Клонирование репозитория

```bash
sudo mkdir -p /opt/unified_parser
sudo chown $USER:$USER /opt/unified_parser
cd /opt/unified_parser
git clone <your-repo-url> .
```

### 2. Настройка переменных окружения

```bash
cp env.docker.example .env
nano .env
```

**Обязательно установите:**
- `SITE_PASSWORD` - пароль для доступа к веб-интерфейсу
- `SESSION_SECRET_KEY` - случайная строка (можно сгенерировать: `python -c "import secrets; print(secrets.token_urlsafe(32))"`)
- `URL_PREFIX=/parser` - префикс URL

### 3. Настройка SSL сертификатов

```bash
# Создайте директорию для SSL
mkdir -p nginx/ssl

# Установите certbot (если используете Let's Encrypt)
sudo apt-get install -y certbot

# Получите сертификат
sudo certbot certonly --standalone -d test.pbd.space

# Скопируйте сертификаты
sudo cp /etc/letsencrypt/live/test.pbd.space/fullchain.pem nginx/ssl/test.pbd.space.crt
sudo cp /etc/letsencrypt/live/test.pbd.space/privkey.pem nginx/ssl/test.pbd.space.key

# Установите права доступа
sudo chmod 644 nginx/ssl/test.pbd.space.crt
sudo chmod 600 nginx/ssl/test.pbd.space.key
sudo chown $USER:$USER nginx/ssl/*
```

**Или используйте существующие сертификаты** (см. [nginx/SSL_SETUP.md](nginx/SSL_SETUP.md))

### 4. Создание необходимых директорий

```bash
mkdir -p output logs config nginx/conf.d
```

### 5. Первый запуск

```bash
# Убедитесь, что Nginx не заблокирован в docker-compose.yml
# (уберите profiles: - production если он есть)

# Запустите контейнеры
docker-compose up -d

# Проверьте статус
docker-compose ps

# Просмотрите логи
docker-compose logs -f
```

## Проверка работы

- [ ] Проверьте доступность через HTTP (должен быть редирект на HTTPS):
  ```bash
  curl -I http://test.pbd.space/parser/login
  ```

- [ ] Проверьте доступность через HTTPS:
  ```bash
  curl -I https://test.pbd.space/parser/login
  ```

- [ ] Откройте в браузере: `https://test.pbd.space/parser/login`
  - Должна открыться страница входа
  - После ввода пароля должен открыться интерфейс парсера

- [ ] Проверьте работу Selenium:
  ```bash
  curl http://localhost:4444/wd/hub/status
  ```

## Настройка автоматического деплоя

### GitHub Secrets

Добавьте в GitHub → Settings → Secrets and variables → Actions:

- `SERVER_HOST` = IP адрес или домен сервера
- `SERVER_USER` = имя пользователя для SSH
- `SERVER_SSH_KEY` = приватный SSH ключ
- `SERVER_PORT` = 22 (или другой порт SSH)
- `DEPLOY_PATH` = `/opt/unified_parser`

### Тестирование деплоя

```bash
# Сделайте commit и push
git add .
git commit -m "Initial deployment setup"
git push origin main

# Проверьте GitHub Actions - должен запуститься workflow "Deploy to Test Server"
```

## Мониторинг

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

### Проверка здоровья

```bash
# Статус контейнеров
docker-compose ps

# Health check парсера
curl http://localhost:8000/login

# Health check Selenium
curl http://localhost:4444/wd/hub/status
```

## Решение проблем

### Проблема: 404 Not Found при доступе к /parser/login

**Решение:**
1. Проверьте, что в `.env` установлен `URL_PREFIX=/parser`
2. Проверьте логи Nginx: `docker-compose logs nginx`
3. Проверьте, что контейнер parser запущен: `docker-compose ps`

### Проблема: SSL сертификат не работает

**Решение:**
1. Проверьте, что сертификаты находятся в `nginx/ssl/`
2. Проверьте права доступа: `ls -la nginx/ssl/`
3. Проверьте конфигурацию Nginx: `docker-compose exec nginx nginx -t`

### Проблема: Парсер не может подключиться к Selenium

**Решение:**
1. Проверьте, что оба контейнера в одной сети: `docker network ls`
2. Проверьте переменную `SELENIUM_REMOTE_URL` в docker-compose.yml
3. Проверьте логи парсера: `docker-compose logs parser`

## Обновление приложения

### Автоматическое обновление

Просто сделайте `git push` в ветку `main` - GitHub Actions автоматически задеплоит изменения.

### Ручное обновление

```bash
cd /opt/unified_parser
git pull
docker-compose down
docker-compose up -d --build
```

## Безопасность

- [ ] Изменен пароль по умолчанию в `.env`
- [ ] SSL сертификаты настроены и работают
- [ ] Порт 4444 (Selenium) не доступен извне (только через внутреннюю сеть)
- [ ] Порт 7900 (VNC) не доступен извне
- [ ] Регулярно обновляйте Docker образы
