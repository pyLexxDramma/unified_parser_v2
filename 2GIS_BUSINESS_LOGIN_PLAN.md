# План интеграции логина в бизнес-аккаунт 2ГИС

## Цель

Интегрировать логин в бизнес-аккаунт 2ГИС для использования фильтров отзывов:
- **С ответами** - только отзывы с ответами компании
- **Положительные** - только положительные отзывы
- **Отрицательные** - только отрицательные отзывы
- **Все** - все отзывы (по умолчанию)

## Преимущества

1. ✅ **Фильтры уже готовы** - не нужно фильтровать вручную
2. ✅ **Более точная классификация** - 2ГИС сам определяет положительные/отрицательные
3. ✅ **Возможно больше отзывов** - бизнес-аккаунт может показывать больше данных
4. ✅ **Структурированные данные** - данные уже отфильтрованы на стороне 2ГИС

## Структура реализации

### 1. Конфигурация учетных данных

**Добавить в `config/config.json`:**
```json
{
  "2gis_business": {
    "enabled": false,
    "email": "",
    "password": ""
  }
}
```

**Или в `.env` (более безопасно):**
```env
2GIS_BUSINESS_ENABLED=false
2GIS_BUSINESS_EMAIL=
2GIS_BUSINESS_PASSWORD=
```

### 2. Метод логина в GisParser

```python
def _login_to_business_account(self, email: str, password: str) -> bool:
    """
    Логинится в бизнес-аккаунт 2ГИС
    
    Returns:
        True если логин успешен, False иначе
    """
    try:
        # 1. Переходим на страницу входа
        login_url = "https://2gis.ru/login"  # или другой URL входа
        self.driver.navigate(login_url)
        time.sleep(2)
        
        # 2. Находим поля ввода email и password
        # Нужно изучить структуру страницы входа
        
        # 3. Вводим данные
        # email_input = self.driver.find_element(...)
        # password_input = self.driver.find_element(...)
        # email_input.send_keys(email)
        # password_input.send_keys(password)
        
        # 4. Нажимаем кнопку входа
        # login_button = self.driver.find_element(...)
        # login_button.click()
        
        # 5. Ждем перехода на страницу бизнес-аккаунта
        # Проверяем, что логин успешен
        
        return True
    except Exception as e:
        logger.error(f"Error logging into 2GIS business account: {e}")
        return False
```

### 3. Использование фильтров при парсинге отзывов

```python
def _apply_review_filters(self, filter_type: str = "all") -> None:
    """
    Применяет фильтры отзывов на странице бизнес-аккаунта
    
    Args:
        filter_type: "all", "positive", "negative", "with_response"
    """
    try:
        # Селекторы для фильтров (нужно изучить структуру страницы)
        filter_selectors = {
            "all": "button:contains('Все')",
            "positive": "button:contains('Положительные')",
            "negative": "button:contains('Отрицательные')",
            "with_response": "button:contains('С ответами')"
        }
        
        selector = filter_selectors.get(filter_type, filter_selectors["all"])
        
        # Находим и кликаем на фильтр
        filter_button = self.driver.find_element(...)
        filter_button.click()
        time.sleep(2)  # Ждем применения фильтра
        
    except Exception as e:
        logger.warning(f"Could not apply filter {filter_type}: {e}")
```

### 4. Модификация метода получения отзывов

```python
def _get_card_reviews_info_2gis(self, card_url: str, use_business_account: bool = False) -> Dict[str, Any]:
    """
    Получает информацию об отзывах карточки
    
    Args:
        card_url: URL карточки
        use_business_account: Использовать ли бизнес-аккаунт
    """
    if use_business_account:
        # Переходим на страницу карточки в бизнес-аккаунте
        # URL может отличаться, например: https://2gis.ru/business/{firm_id}/reviews
        business_url = self._convert_to_business_url(card_url)
        self.driver.navigate(business_url)
        time.sleep(3)
        
        # Применяем фильтры (если нужно)
        # self._apply_review_filters("all")
        
        # Парсим отзывы (структура может отличаться от публичной страницы)
        return self._parse_business_reviews()
    else:
        # Текущая логика для публичной страницы
        return self._parse_public_reviews(card_url)
```

## Что нужно изучить

### 1. Страница входа в бизнес-аккаунт

**Вопросы:**
- Какой URL страницы входа?
- Какие селекторы для полей email и password?
- Какая кнопка для входа?
- Есть ли капча или двухфакторная аутентификация?

### 2. Страница карточки в бизнес-аккаунте

**Вопросы:**
- Какой формат URL карточки в бизнес-аккаунте?
- Где находятся фильтры отзывов?
- Какие селекторы для фильтров?
- Отличается ли структура отзывов от публичной страницы?

### 3. Структура отзывов в бизнес-аккаунте

**Вопросы:**
- Те же селекторы, что и на публичной странице?
- Или другие классы/структура?
- Есть ли дополнительные данные?

## План действий

### Этап 1: Изучение структуры (с вашей помощью)

1. ✅ Получить учетные данные для бизнес-аккаунта
2. ✅ Изучить страницу входа:
   - URL
   - Селекторы полей
   - Селектор кнопки входа
3. ✅ Изучить страницу карточки:
   - URL формат
   - Селекторы фильтров
   - Структура отзывов

### Этап 2: Реализация логина (1-2 дня)

1. ✅ Добавить конфигурацию для учетных данных
2. ✅ Реализовать метод `_login_to_business_account()`
3. ✅ Протестировать логин

### Этап 3: Интеграция фильтров (1-2 дня)

1. ✅ Реализовать метод `_apply_review_filters()`
2. ✅ Модифицировать `_get_card_reviews_info_2gis()` для использования бизнес-аккаунта
3. ✅ Протестировать фильтры

### Этап 4: Тестирование (1 день)

1. ✅ Протестировать на реальных данных
2. ✅ Сравнить результаты с публичной страницей
3. ✅ Убедиться, что получаем больше/лучше отфильтрованных отзывов

## Безопасность

⚠️ **Важно:** Учетные данные должны храниться безопасно:
- Использовать `.env` файл (не коммитить в git)
- Добавить `.env` в `.gitignore`
- Использовать переменные окружения
- Не логировать пароли

## Пример использования

```python
# В config.json или .env
{
  "2gis_business": {
    "enabled": true,
    "email": "your_email@example.com",
    "password": "your_password"
  }
}

# В коде парсера
if settings.gis_business.enabled:
    if self._login_to_business_account(settings.gis_business.email, settings.gis_business.password):
        # Используем бизнес-аккаунт
        reviews_info = self._get_card_reviews_info_2gis(card_url, use_business_account=True)
    else:
        # Fallback на публичную страницу
        reviews_info = self._get_card_reviews_info_2gis(card_url, use_business_account=False)
```





