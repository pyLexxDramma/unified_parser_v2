# Инструкция по настройке .env файла для бизнес-аккаунта 2ГИС

## Добавьте в файл `.env` следующие строки:

```env
# Бизнес-аккаунт 2ГИС
GIS_LOGIN=kazan@pbd.space
GIS_PASSWORD=0o-3DjPUu_12

# Или используйте полные имена переменных:
# 2GIS_BUSINESS_ENABLED=true
# 2GIS_BUSINESS_EMAIL=kazan@pbd.space
# 2GIS_BUSINESS_PASSWORD=0o-3DjPUu_12
```

## Варианты настройки:

### Вариант 1: Простые имена (рекомендуется)
```env
GIS_LOGIN=kazan@pbd.space
GIS_PASSWORD=0o-3DjPUu_12
```
Парсер автоматически включит бизнес-аккаунт, если указаны эти переменные.

### Вариант 2: Полные имена
```env
2GIS_BUSINESS_ENABLED=true
2GIS_BUSINESS_EMAIL=kazan@pbd.space
2GIS_BUSINESS_PASSWORD=0o-3DjPUu_12
```

### Вариант 3: Через config.json
Добавьте в `config/config.json`:
```json
{
  "2gis_business": {
    "enabled": true,
    "email": "kazan@pbd.space",
    "password": "0o-3DjPUu_12"
  }
}
```

⚠️ **Важно:** Не коммитьте файл `.env` в git! Он уже добавлен в `.gitignore`.

## После добавления переменных:

1. Перезапустите сервер парсера
2. Парсер автоматически попытается войти в бизнес-аккаунт при запуске
3. В логах вы увидите:
   - `Вход в бизнес-аккаунт 2ГИС...`
   - `Successfully logged into 2GIS business account` (если успешно)
   - `Failed to login to 2GIS business account, using public access` (если не удалось)

## Проверка:

После запуска парсера проверьте логи - там будет видно, успешно ли выполнен вход в бизнес-аккаунт.





