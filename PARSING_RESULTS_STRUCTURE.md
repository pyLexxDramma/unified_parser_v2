# Структура данных парсинга и места хранения

## Что возвращается из методов `parse()`

### Яндекс парсер (`YandexParser.parse()`)

Возвращает словарь `Dict[str, Any]` со следующей структурой:

```python
{
    "cards_data": List[Dict[str, Any]],  # Список карточек компаний
    "aggregated_info": Dict[str, Any]    # Агрегированная статистика
}
```

#### Структура `cards_data` (каждая карточка):

```python
{
    "card_name": str,                    # Название компании
    "card_address": str,                 # Адрес
    "card_rating": str,                  # Рейтинг (например, "4.5")
    "card_reviews_count": int,           # Количество отзывов
    "card_website": str,                  # Сайт компании
    "card_phone": str,                   # Телефон
    "card_rubrics": str,                 # Рубрики
    "card_response_status": str,         # Статус ответа (UNKNOWN, ANSWERED, etc.)
    "card_avg_response_time": str,       # Среднее время ответа
    "card_reviews_positive": int,        # Положительных отзывов (4-5⭐)
    "card_reviews_negative": int,        # Отрицательных отзывов (1-2⭐)
    "card_reviews_neutral": int,         # Нейтральных отзывов (3⭐)
    "card_reviews_texts": str,           # Краткие тексты отзывов (через "; ")
    "detailed_reviews": List[Dict],     # Детальная информация по каждому отзыву
    "source": str,                       # "yandex"
    "city": str                          # Город (добавляется в app.py)
}
```

#### Структура `detailed_reviews` (каждый отзыв):

```python
{
    "review_rating": float,              # Оценка (1.0-5.0)
    "review_text": str,                   # Текст отзыва
    "review_author": str,                 # Автор отзыва
    "review_date": str,                   # Дата отзыва
    "has_response": bool,                 # Есть ли ответ компании
    "response_text": str,                 # Текст ответа компании
    "response_date": str                  # Дата ответа компании
}
```

#### Структура `aggregated_info`:

```python
{
    "search_query_name": str,            # Название компании из поискового запроса
    "total_cards_found": int,            # Всего найдено карточек
    "aggregated_rating": float,         # Средний рейтинг по всем карточкам
    "aggregated_reviews_count": int,     # Всего отзывов
    "aggregated_positive_reviews": int,  # Всего положительных отзывов
    "aggregated_negative_reviews": int,  # Всего отрицательных отзывов
    "aggregated_neutral_reviews": int,   # Всего нейтральных отзывов
    "aggregated_answered_reviews_count": int,  # Отзывов с ответами
    "aggregated_unanswered_reviews_count": int,  # Отзывов без ответов
    "aggregated_avg_response_time": float  # Среднее время ответа (дни)
}
```

### 2ГИС парсер (`GisParser.parse()`)

Возвращает словарь `Dict[str, Any]` со следующей структурой:

```python
{
    "cards_data": List[Dict[str, Any]],  # Список карточек компаний
    "aggregated_info": Dict[str, Any]    # Агрегированная статистика
}
```

#### Структура `cards_data` (каждая карточка):

```python
{
    "card_name": str,                    # Название компании
    "card_address": str,                 # Адрес
    "card_rating": str,                  # Рейтинг
    "card_reviews_count": int,           # Количество отзывов
    "card_website": str,                 # Сайт компании
    "card_phone": str,                   # Телефон
    "card_rubrics": str,                 # Рубрики
    "card_response_status": str,         # Статус ответа
    "card_avg_response_time": float,     # Среднее время ответа (дни)
    "card_reviews_positive": int,        # Положительных отзывов
    "card_reviews_negative": int,        # Отрицательных отзывов
    "card_reviews_texts": str,           # Краткие тексты отзывов
    "card_answered_reviews_count": int,   # Отзывов с ответами
    "card_unanswered_reviews_count": int, # Отзывов без ответов
    "detailed_reviews": List[Dict],     # Детальная информация по каждому отзыву
    "source": str,                       # "2gis"
    "city": str                          # Город (добавляется в app.py)
}
```

#### Структура `detailed_reviews` (каждый отзыв):

```python
{
    "review_rating": float,              # Оценка (1.0-5.0)
    "review_text": str,                  # Текст отзыва
    "review_author": str,                # Автор отзыва
    "review_date": str,                  # Дата отзыва
    "has_response": bool,                # Есть ли ответ компании
    "response_text": str,                # Текст ответа компании
    "response_date": str                 # Дата ответа компании
}
```

#### Структура `aggregated_info`:

```python
{
    "search_query_name": str,            # Название компании из поискового запроса
    "total_cards_found": int,            # Всего найдено карточек
    "aggregated_rating": float,         # Средний рейтинг по всем карточкам
    "aggregated_reviews_count": int,     # Всего отзывов
    "aggregated_positive_reviews": int,  # Всего положительных отзывов
    "aggregated_negative_reviews": int,  # Всего отрицательных отзывов
    "aggregated_answered_reviews_count": int,  # Отзывов с ответами
    "aggregated_unanswered_reviews_count": int,  # Отзывов без ответов
    "aggregated_avg_response_time": float  # Среднее время ответа (дни)
}
```

---

## Где хранятся результаты парсинга

### 1. В памяти (во время выполнения)

**Объект задачи (`TaskStatus`):**

Хранится в `src/utils/task_manager.py` в словаре `active_tasks`:

```python
active_tasks[task_id] = TaskStatus(
    task_id: str,
    status: str,                        # PENDING, RUNNING, COMPLETED, FAILED
    progress: str,                      # Текущий статус выполнения
    email: Optional[str],
    source_info: Dict[str, Any],        # Информация о запросе (company_name, company_site, etc.)
    detailed_results: List[Dict],       # Все карточки (all_cards)
    statistics: Dict[str, Any],        # Агрегированная статистика
    result_file: Optional[str],         # Имя файла с результатами
    error: Optional[str],
    timestamp: datetime,
    start_time: Optional[datetime],
    end_time: Optional[datetime]
)
```

**Доступ к задаче:**
- Через API: `GET /tasks/{task_id}/status` - возвращает JSON с информацией о задаче
- Через веб-интерфейс: `GET /tasks/{task_id}` - отображает HTML страницу с результатами

### 2. CSV файл (постоянное хранилище)

**Местоположение:**
- Директория: `output/` (настраивается в `settings.app_config.writer.output_dir`)
- Имя файла: задается пользователем в форме (`output_filename`, по умолчанию `report.csv`)
- Полный путь: `{output_dir}/{output_filename}`

**Формат:**
- CSV файл с заголовками
- Каждая строка = одна карточка компании
- Поле `detailed_reviews` сериализуется в JSON строку

**Класс:** `src/storage/csv_writer.py` - `CSVWriter`

**Пример использования:**
```python
writer = CSVWriter(settings=settings)
writer.set_file_path(os.path.join(results_dir, form_data.output_filename))
with writer:
    for card in all_cards:
        writer.write(card)
```

### 3. PDF отчет (генерируется по запросу)

**Местоположение:**
- Директория: `output/` (та же, что и для CSV)
- Имя файла: `report_{task_id}.pdf`
- Полный путь: `{output_dir}/report_{task_id}.pdf`

**Генерация:**
- По запросу: `GET /tasks/{task_id}/download-pdf`
- Генерируется из `task.detailed_results` и `task.statistics`

**Класс:** `src/storage/pdf_writer.py` - `PDFWriter`

**Содержимое PDF:**
- Агрегированная статистика (рейтинг, количество отзывов, тональность)
- Детальная информация по каждой карточке
- Список всех отзывов с рейтингами и текстами
- Ответы компаний на отзывы

---

## Поток данных

```
1. Пользователь заполняет форму → start_parsing()
   ↓
2. Создается задача → create_task() → active_tasks[task_id]
   ↓
3. Запускается парсинг в отдельном потоке → run_parsing()
   ↓
4. Для каждого города/источника:
   - YandexParser.parse(url) → возвращает {"cards_data": [...], "aggregated_info": {...}}
   - GisParser.parse(url) → возвращает {"cards_data": [...], "aggregated_info": {...}}
   ↓
5. Все карточки собираются в all_cards[]
   ↓
6. Статистика агрегируется в stats{}
   ↓
7. Результаты сохраняются:
   - CSV файл → CSVWriter.write() для каждой карточки
   - В задачу → task.detailed_results = all_cards
   - В задачу → task.statistics = stats
   - В задачу → task.result_file = output_filename
   ↓
8. Задача помечается как COMPLETED
   ↓
9. Пользователь может:
   - Просмотреть результаты → GET /tasks/{task_id}
   - Скачать CSV → GET /tasks/{task_id}/download
   - Скачать PDF → GET /tasks/{task_id}/download-pdf
   - Получить статус → GET /tasks/{task_id}/status
```

---

## API endpoints для доступа к результатам

### 1. Статус задачи (JSON)
```
GET /tasks/{task_id}/status
```
Возвращает:
```json
{
    "task_id": "...",
    "status": "COMPLETED",
    "progress": "...",
    "email": "...",
    "source_info": {...},
    "result_file": "report.csv",
    "error": null,
    "timestamp": "..."
}
```

### 2. Страница с результатами (HTML)
```
GET /tasks/{task_id}
```
Отображает HTML страницу с:
- Статистикой парсинга
- Списком всех карточек
- Группировкой по городам
- Ссылками на скачивание CSV и PDF

### 3. Скачать CSV
```
GET /tasks/{task_id}/download
```
Возвращает CSV файл с результатами

### 4. Скачать PDF
```
GET /tasks/{task_id}/download-pdf
```
Генерирует и возвращает PDF отчет

---

## Важные замечания

1. **Временное хранилище:** `active_tasks` хранится только в памяти процесса. При перезапуске сервера данные теряются.

2. **Постоянное хранилище:** CSV файлы сохраняются на диск и остаются после перезапуска.

3. **PDF генерируется на лету:** PDF файлы создаются при запросе и могут быть пересозданы в любой момент.

4. **Ограничения памяти:** Если карточек очень много, `task.detailed_results` может занимать много памяти. CSV файл не имеет таких ограничений.

5. **JSON в CSV:** Поле `detailed_reviews` сериализуется в JSON строку в CSV файле, что позволяет сохранить всю детальную информацию об отзывах.





