from __future__ import annotations
import uuid
import logging
import threading
import os
import urllib.parse
import re
from fastapi import FastAPI, Request, Depends, HTTPException, Form
from fastapi.responses import RedirectResponse, JSONResponse, FileResponse
from fastapi import status as http_status
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field
from typing import Optional, Dict, Any, List
import secrets
from starlette.middleware.sessions import SessionMiddleware

from src.drivers.selenium_driver import SeleniumDriver
from src.parsers.yandex_parser import YandexParser
from src.parsers.gis_parser import GisParser
from src.storage.csv_writer import CSVWriter
from src.storage.pdf_writer import PDFWriter
from src.utils.task_manager import (
    TaskStatus,
    active_tasks,
    create_task,
    update_task_status,
    pause_task,
    resume_task,
    stop_task,
    is_task_paused,
    is_task_stopped,
    get_task,
)
from src.config.settings import Settings

app = FastAPI()

templates = Jinja2Templates(directory="src/webapp/templates")

os.makedirs("src/webapp/static", exist_ok=True)
app.mount("/static", StaticFiles(directory="src/webapp/static"), name="static")

# ВАЖНО: секрет для сессий должен быть стабильным между воркерами/перезапусками,
# иначе при работе через Docker или несколько процессов сессия «теряется» и
# check_auth начинает возвращать False (Unauthorized).
SESSION_SECRET_KEY = os.environ.get("SESSION_SECRET_KEY") or "change_me_in_production_session_secret"
app.add_middleware(SessionMiddleware, secret_key=SESSION_SECRET_KEY)

logger = logging.getLogger(__name__)

settings = Settings()

# Загружаем пароль: сначала из переменной окружения, потом из config.json, потом дефолтный
SITE_PASSWORD = os.environ.get("SITE_PASSWORD")
if not SITE_PASSWORD:
    if hasattr(settings, "app_config") and settings.app_config:
        SITE_PASSWORD = getattr(settings.app_config, "password", None)
if not SITE_PASSWORD:
    SITE_PASSWORD = "admin123"

logger.info(
    f"Site password loaded: {'*' * len(SITE_PASSWORD) if SITE_PASSWORD else 'NOT SET'}"
)
logger.info(
    "Password source: "
    f"{'ENV' if os.environ.get('SITE_PASSWORD') else 'CONFIG' if hasattr(settings, 'app_config') and getattr(settings.app_config, 'password', None) else 'DEFAULT'}"
)


def get_url_prefix(request: Request | None = None) -> str:
    """
    Определяет префикс корневого пути для работы за reverse-proxy.
    Приоритет:
    1) request.scope['root_path'] (если настроен сервер, передающий root_path)
    2) переменная окружения URL_PREFIX (можно задать в .env, например '/parser')
    3) по умолчанию: пустая строка (приложение висит на корне)
    """
    scope_prefix = ""
    if request is not None and hasattr(request, "scope"):
        scope_prefix = request.scope.get("root_path") or ""

    env_prefix = os.getenv("URL_PREFIX", "").strip()

    prefix = scope_prefix or env_prefix

    if not prefix or prefix == "/":
        return ""

    # Нормализуем: добавляем ведущий слэш, убираем лишний хвост
    if not prefix.startswith("/"):
        prefix = "/" + prefix
    prefix = prefix.rstrip("/")
    return prefix


class ParsingForm(BaseModel):
    company_name: str
    company_site: str
    source: str
    email: str
    output_filename: str = "report.csv"
    search_scope: str = "country"
    location: str = ""
    proxy_server: Optional[str] = ""
    # Список городов (для режима "по стране"), упакованный в одну строку через ; или ,
    cities: str = ""

    @classmethod
    async def as_form(cls, request: Request):
        form_data = await request.form()
        try:
            return cls(**form_data)
        except Exception as e:
            logger.error(f"Error parsing form data: {e}", exc_info=True)
            raise HTTPException(status_code=422, detail=f"Error processing form data: {e}")

def check_auth(request: Request) -> bool:
    return request.session.get("authenticated", False)

@app.get("/login")
async def login_page(request: Request):
    # Префикс корневого пути для работы за reverse-proxy (например, /parser)
    url_prefix = get_url_prefix(request)
    if check_auth(request):
        return RedirectResponse(url=f"{url_prefix}/", status_code=302)
    return templates.TemplateResponse(
        "login.html",
        {
            "request": request,
            "error": None,
            "url_prefix": url_prefix,
        },
    )

@app.post("/login")
async def login(request: Request, password: str = Form(...)):
    # Убираем пробелы в начале и конце пароля
    password = password.strip()
    expected_password = SITE_PASSWORD.strip() if SITE_PASSWORD else ""
    url_prefix = get_url_prefix(request)

    logger.debug(
        f"Login attempt: received password length={len(password)}, expected length={len(expected_password)}"
    )

    if password == expected_password:
        request.session["authenticated"] = True
        logger.info("User authenticated successfully")
        return RedirectResponse(url=f"{url_prefix}/", status_code=302)
    else:
        logger.warning("Failed login attempt: password mismatch")
        return templates.TemplateResponse(
            "login.html",
            {
                "request": request,
                "error": "Неверный пароль",
                "url_prefix": url_prefix,
            },
        )

@app.get("/logout")
async def logout(request: Request):
    request.session.clear()
    url_prefix = get_url_prefix(request)
    return RedirectResponse(url=f"{url_prefix}/login", status_code=302)

@app.get("/debug/password")
async def debug_password(request: Request):
    """Временный эндпоинт для отладки пароля (только для разработки)"""
    if not check_auth(request):
        url_prefix = get_url_prefix(request)
        return RedirectResponse(url=f"{url_prefix}/login", status_code=302)
    password_info = {
        "password_length": len(SITE_PASSWORD) if SITE_PASSWORD else 0,
        "password_source": "ENV" if os.environ.get('SITE_PASSWORD') else ("CONFIG" if hasattr(settings, 'app_config') and getattr(settings.app_config, 'password', None) else "DEFAULT"),
        "password_set": bool(SITE_PASSWORD),
        "config_password": getattr(settings.app_config, 'password', None) if hasattr(settings, 'app_config') else None
    }
    return JSONResponse(password_info)

@app.get("/")
async def read_root(request: Request):
    url_prefix = get_url_prefix(request)
    if not check_auth(request):
        return RedirectResponse(url=f"{url_prefix}/login", status_code=302)
    error = request.query_params.get("error")
    success = request.query_params.get("success")
    last_form = request.session.get("last_form") if request.session else None
    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "error": error,
            "success": success,
            "last_form": last_form,
            "url_prefix": url_prefix,
        },
    )

SUMMARY_FIELDS = [
    ("search_query_name", "Название запроса"),
    ("total_cards_found", "Карточек найдено"),
    ("aggregated_rating", "Средний рейтинг"),
    ("aggregated_reviews_count", "Всего отзывов"),
    ("aggregated_positive_reviews", "Положительных отзывов (4-5⭐)"),
    ("aggregated_negative_reviews", "Отрицательных отзывов (1-3⭐)"),
    ("aggregated_answered_reviews_count", "Отвечено отзывов"),
    ("aggregated_answered_reviews_percent", "Процент отзывов с ответами"),
    ("aggregated_unanswered_reviews_count", "Не отвечено отзывов"),
    ("aggregated_avg_response_time", "Среднее время ответа (дни)"),
]

def _generate_yandex_url(company_name: str, search_scope: str, location: str) -> str:
    encoded_company_name = urllib.parse.quote(company_name)
    if search_scope == "city" and location:
        encoded_location = urllib.parse.quote(location)
        if location.lower() == "москва":
            return f"https://yandex.ru/maps/?text={encoded_company_name}%2C+{encoded_location}&ll=37.617300%2C55.755826&z=12"
        elif location.lower() == "санкт-петербург":
            return f"https://yandex.ru/maps/?text={encoded_company_name}%2C+{encoded_location}&ll=30.315868%2C59.939095&z=11"
        else:
            return f"https://yandex.ru/maps/?text={encoded_company_name}%2C+{encoded_location}"
    else:
        search_text = location if location else "Россия"
        full_search_text = f"{search_text}%20{encoded_company_name}"
        return f"https://yandex.ru/maps/?text={full_search_text}&mode=search&z=3"

def _generate_gis_url(company_name: str, company_site: str, search_scope: str, location: str) -> str:
    """
    Генерирует URL для поиска в 2ГИС.
    
    Для режима "city" использует формат поиска с городом в запросе:
    https://2gis.ru/search/{company_name}+{city}?search_source=main&company_website={company_site}
    
    Это позволяет 2ГИС самому определить правильный городской сегмент (spb, msk и т.д.)
    вместо попытки угадать код города из полного названия.
    """
    encoded_company_name = urllib.parse.quote(company_name, safe='')
    encoded_company_site = urllib.parse.quote(company_site, safe='')
    if search_scope == "city" and location:
        # Добавляем город в поисковый запрос, а не в путь URL
        # Это позволяет 2ГИС корректно определить город и вернуть результаты
        search_query = f"{company_name} {location}"
        encoded_search_query = urllib.parse.quote(search_query, safe='')
        return f"https://2gis.ru/search/{encoded_search_query}?search_source=main&company_website={encoded_company_site}"
    else:
        return f"https://2gis.ru/search/{encoded_company_name}?search_source=main&company_website={encoded_company_site}"


CITY_NAME_RE = re.compile(r"^[А-Яа-яЁё\s\-]+$")
CITY_PLACEHOLDER = "Значение отсутствует"


def _parse_cities(cities_str: str) -> List[str]:
    """
    Преобразует строку с городами (через ; или ,) в список уникальных городов.

    Здесь мы также отбрасываем заведомо «пустые» значения вроде
    служебного текста «Значение отсутствует».
    """
    if not cities_str:
        return []
    parts = re.split(r"[;,]", cities_str)
    cities: List[str] = []
    seen = set()
    for part in parts:
        city = part.strip()
        if not city or city == CITY_PLACEHOLDER:
            continue
        if city not in seen:
            cities.append(city)
            seen.add(city)
    return cities


def _is_valid_city_name(city: str) -> bool:
    """
    Серверная валидация названия города:
    - только кириллица + пробел + дефис
    - разумная длина
    - исключаем placeholder «Значение отсутствует»
    """
    city = (city or "").strip()
    if not city or city == CITY_PLACEHOLDER:
        return False
    if len(city) < 2 or len(city) > 64:
        return False
    return bool(CITY_NAME_RE.match(city))


def _is_valid_email(email: str) -> bool:
    """
    Простая проверка email:
    - есть одна @
    - после @ есть точка
    """
    email = (email or "").strip()
    if not email:
        return False
    # Очень упрощённая, но достаточная валидация
    return bool(re.match(r"^[^@\s]+@[^@\s]+\.[^@\s]+$", email))


def _normalize_company_site(raw_site: str) -> str:
    """
    Нормализует и валидирует сайт компании.
    Разрешаем формат без протокола (example.ru) и с протоколом (http(s)://example.ru).
    Если протокола нет — дописываем http://.
    Если передан полный URL с путём/параметрами/якорем, автоматически обрезаем до origin:
    scheme + host (например, https://example.com/path?a=1 -> https://example.com).
    При некорректном адресе поднимаем ValueError с человекочитаемым сообщением.
    """
    site = (raw_site or "").strip()
    if not site:
        raise ValueError("Укажите сайт компании")

    # Явно запрещаем protocol-relative URL вида //example.ru
    if site.startswith("//"):
        raise ValueError("Укажите сайт в формате example.ru или с протоколом, например: https://example.ru")

    # Запрещаем протоколы, отличные от http/https, в частности ftp://
    if re.match(r"^ftp://", site, flags=re.IGNORECASE):
        raise ValueError("Поддерживаются только сайты с протоколом http или https (пример: https://example.ru)")

    # Если пользователь ввёл только домен/хост без протокола — дописываем http://
    if not re.match(r"^https?://", site, flags=re.IGNORECASE):
        site = f"http://{site}"

    parsed = urllib.parse.urlparse(site)

    # Должен быть хостнейм вида example.ru или sub.example.ru
    if not parsed.netloc or "." not in parsed.netloc:
        raise ValueError("Некорректный адрес сайта. Пример: example.ru или https://example.ru")

    # Запрещаем указание порта в адресе (например, :8080)
    if ":" in parsed.netloc:
        raise ValueError("Адрес сайта не должен содержать номер порта (например, :8080)")

    # Дополнительно исключаем пробелы и явно битые варианты
    if " " in parsed.netloc:
        raise ValueError("Адрес сайта не должен содержать пробелы")

    # Дополнительно проверяем доменные метки: не должны начинаться или заканчиваться дефисом,
    # домен верхнего уровня и домен второго уровня не должны быть пустыми
    host = parsed.hostname or parsed.netloc
    # Хост не должен заканчиваться точкой (sberbank. — недопустимо)
    if host.endswith("."):
        raise ValueError("Укажите корректный домен верхнего уровня (пример: sberbank.ru, example.com)")

    labels = host.split(".")
    non_empty_labels = [label for label in labels if label]
    if len(non_empty_labels) < 2:
        raise ValueError("Адрес сайта должен содержать домен второго уровня (пример: sberbank.ru, example.com)")

    for label in labels:
        if not label:
            continue
        if label.startswith("-") or label.endswith("-"):
            raise ValueError("Доменное имя не должно начинаться или заканчиваться дефисом (например: -example.ru, example-.ru)")

    # Нормализуем до origin: схема + хост, без путей, параметров и якорей
    scheme = parsed.scheme or "http"
    normalized = f"{scheme}://{parsed.netloc}"
    return normalized


def _normalize_company_name(raw_name: str) -> str:
    """
    Нормализует название компании:
    - обрезает пробелы по краям;
    - добавляет один пробел между ОПФ (ООО, ЗАО, ОАО, ПАО, АО, ИП) и остальной частью, если он отсутствует.
    """
    name = (raw_name or "").strip()
    if not name:
        return name

    # Поддерживаемые (актуальные) ОПФ; при необходимости список можно расширить
    opf_list = ("ООО", "ПАО", "АО", "ИП")
    upper = name.upper()

    for opf in opf_list:
        if upper.startswith(opf):
            # Берём «хвост» после ОПФ и убираем из него лишние пробелы слева
            rest = name[len(opf):].lstrip()
            if rest:
                return f"{opf} {rest}"
            # Если после ОПФ ничего нет, просто возвращаем само ОПФ
            return opf

    return name

def _run_parser_task(parser_class, url: str, task_id: str, source: str):
    driver = None
    try:
        logger.info(f"Task {task_id} ({source}): Starting parser task for URL: {url}")
        update_task_status(task_id, "RUNNING", f"{source}: Инициализация драйвера...")
        driver = SeleniumDriver(settings=settings)
        logger.info(f"Task {task_id} ({source}): Driver created, starting...")
        try:
            driver.start()
            logger.info(f"Task {task_id} ({source}): Driver started successfully")
        except Exception as driver_error:
            logger.error(f"Task {task_id} ({source}): Failed to start driver: {driver_error}", exc_info=True)
            update_task_status(task_id, "FAILED", f"{source}: Ошибка запуска драйвера: {str(driver_error)}", error=str(driver_error))
            return None, str(driver_error)

        update_task_status(task_id, "RUNNING", f"{source}: Запуск парсера...")
        logger.info(f"Task {task_id} ({source}): Creating parser instance...")
        parser = parser_class(driver=driver, settings=settings)
        # Пробрасываем task_id в парсер, чтобы он мог реагировать на паузу/остановку
        try:
            setattr(parser, "task_id", task_id)
        except Exception:
            logger.debug("Could not set task_id attribute on parser instance")

        def update_progress(msg: str):
            # Формируем сообщение с префиксом источника (как в старом проекте)
            if source == "Yandex":
                progress_message = f"Yandex: {msg}" if not msg.startswith("Yandex:") else msg
            elif source == "2GIS":
                progress_message = f"2GIS: {msg}" if not msg.startswith("2GIS:") else msg
            else:
                progress_message = msg
            
            # Обновляем напрямую через active_tasks (как в старом проекте)
            if task_id in active_tasks:
                active_tasks[task_id].progress = progress_message
            update_task_status(task_id, "RUNNING", progress_message)
            logger.info(f"Task {task_id}: {progress_message}")
            import sys
            sys.stdout.flush()

        parser.set_progress_callback(update_progress)
        # Callback, чтобы парсер мог в любой момент проверить, не остановлена ли задача
        try:
            parser.set_stop_check_callback(lambda: is_task_stopped(task_id))
        except Exception:
            logger.debug("Could not set stop_check_callback on parser instance")

        logger.info(f"Task {task_id} ({source}): Starting parse for URL: {url}")
        try:
            result = parser.parse(url=url)
        except Exception as parse_error:
            logger.error(f"Task {task_id} ({source}): Parse failed: {parse_error}", exc_info=True)
            update_task_status(task_id, "FAILED", f"{source}: Ошибка парсинга: {str(parse_error)}", error=str(parse_error))
            return None, str(parse_error)

        logger.info(
            f"Task {task_id} ({source}): Parse completed, result keys: {list(result.keys()) if result else 'None'}"
        )

        # Если задачу остановили пользователем, помечаем это явно в прогрессе
        if is_task_stopped(task_id):
            cards_count = len(result.get('cards_data', [])) if result and isinstance(result, dict) else 0
            update_task_status(
                task_id,
                "COMPLETED",
                f"{source}: Остановлено пользователем. Найдено карточек: {cards_count}",
            )
        else:
            update_task_status(task_id, "RUNNING", f"{source}: Парсинг завершен")

        return result, None
    except Exception as e:
        logger.error(f"Error in parser task {task_id} ({source}): {e}", exc_info=True)
        update_task_status(task_id, "FAILED", f"{source}: Ошибка: {str(e)}", error=str(e))
        return None, str(e)
    finally:
        if driver:
            try:
                logger.info(f"Task {task_id} ({source}): Stopping driver...")
                driver.stop()
                logger.info(f"Task {task_id} ({source}): Driver stopped")
            except Exception as stop_error:
                logger.warning(f"Error stopping driver for task {task_id} ({source}): {stop_error}")

@app.post("/start_parsing")
async def start_parsing(request: Request, form_data: ParsingForm = Depends(ParsingForm.as_form)):
    url_prefix = get_url_prefix(request)
    if not check_auth(request):
        return RedirectResponse(url=f"{url_prefix}/login", status_code=302)

    # Нормализуем часть текстовых полей (обрезаем пробелы по краям, приводим формат названия компании)
    if form_data.company_name:
        form_data.company_name = _normalize_company_name(form_data.company_name)
    if form_data.company_site:
        form_data.company_site = form_data.company_site.strip()
    if form_data.email:
        form_data.email = form_data.email.strip()

    # Сохраняем последние введённые значения формы в сессии,
    # чтобы при ошибке пользователь не заполнял её заново
    try:
        request.session["last_form"] = form_data.dict()
    except Exception as e:
        logger.warning(f"Could not store last_form in session: {e}")

    errors: list[str] = []

    # Базовая проверка заполненности
    if not form_data.company_name or not form_data.company_site or not form_data.source or not form_data.email:
        errors.append("Заполните все обязательные поля")

    # ОПФ не обязателен - пользователь может ввести любое название компании

    # Проверка: в названии не должно быть латинских букв
    if re.search(r"[A-Za-z]", form_data.company_name):
        errors.append("Название компании должно быть полностью на кириллице, без латинских букв (например: ООО Ромашка)")

    # Проверка: в названии не должно быть запрещённых спецсимволов (@, /, \ и т.п.)
    # Разрешаем: буквы, цифры, пробел, дефис, точку, запятую, кавычки, скобки и №
    if re.search(r"[^А-Яа-яЁё0-9\s\-\.,«»\"'()№]", form_data.company_name):
        errors.append("Название компании содержит недопустимые спецсимволы. Уберите символы вроде @, / и другие лишние знаки.")

    # Валидация email
    if not _is_valid_email(form_data.email):
        errors.append("Укажите корректный email (например: user@example.com)")

    # Дополнительная серверная валидация сайта
    if form_data.company_site:
        try:
            normalized_site = _normalize_company_site(form_data.company_site)
            form_data.company_site = normalized_site
        except ValueError as e:
            errors.append(str(e))

    # Валидация списка городов (режим "Страна / общий поиск")
    raw_cities_str = getattr(form_data, "cities", "") or ""
    if form_data.search_scope == "country" and raw_cities_str:
        parsed_cities = _parse_cities(raw_cities_str)
        invalid_cities = [c for c in parsed_cities if not _is_valid_city_name(c)]
        if invalid_cities:
            msg = "Некорректные названия городов: " + ", ".join(invalid_cities[:5])
            errors.append(msg)

    # Если есть ошибки валидации — возвращаем их одним ответом
    if errors:
        full_msg = "Исправьте ошибки: " + "; ".join(errors)
        return RedirectResponse(
            url=f"{url_prefix}/?error=" + urllib.parse.quote(full_msg),
            status_code=302,
        )

    task_id = create_task(
        email=form_data.email,
        source_info={
            "company_name": form_data.company_name,
            "company_site": form_data.company_site,
            "source": form_data.source,
            "search_scope": form_data.search_scope,
            "location": form_data.location,
            # Для режима "по стране" сохраняем список выбранных городов
            "cities": getattr(form_data, "cities", ""),
        },
    )
    logger.info(f"Created task {task_id} for company: {form_data.company_name}, source: {form_data.source}")

    def run_parsing():
        logger.info(f"Starting parsing thread for task {task_id}")
        try:
            # Разбираем список городов, если включён режим "по стране" и переданы города
            cities_list: List[str] = []
            if form_data.search_scope == 'country' and getattr(form_data, "cities", ""):
                cities_list = _parse_cities(form_data.cities)

            if form_data.source == 'both':
                update_task_status(task_id, "RUNNING", "Запуск парсинга обоих источников...")
                all_cards: List[Dict[str, Any]] = []
                statistics: Dict[str, Any] = {}
                yandex_error = None
                gis_error = None

                # Если передан список городов, запускаем парсинг поочерёдно по каждому городу
                if cities_list:
                    yandex_stats_list: List[Dict[str, Any]] = []
                    gis_stats_list: List[Dict[str, Any]] = []

                    for city in cities_list:
                        # Позволяем пользователю остановить задачу посреди обхода городов
                        if is_task_stopped(task_id):
                            logger.info(f"Task {task_id}: stop flag detected before processing city '{city}', breaking city loop (both sources)")
                            break
                        # Yandex
                        yandex_url = _generate_yandex_url(form_data.company_name, "city", city)
                        update_task_status(task_id, "RUNNING", f"Yandex: Парсинг города {city}...")
                        logger.info(f"Task {task_id}: Starting Yandex parser for city {city}...")
                        yandex_result, yandex_error = _run_parser_task(YandexParser, yandex_url, task_id, "Yandex")

                        if yandex_result:
                            cards = yandex_result.get("cards_data", [])
                            for card in cards:
                                card["source"] = "yandex"
                                card["city"] = city
                            all_cards.extend(cards)

                            if yandex_result.get("aggregated_info"):
                                yandex_stats_list.append(yandex_result["aggregated_info"])

                        # 2GIS
                        if is_task_stopped(task_id):
                            logger.info(f"Task {task_id}: stop flag detected after Yandex for city '{city}', skipping 2GIS and remaining cities (both sources)")
                            break

                        gis_url = _generate_gis_url(
                            form_data.company_name,
                            form_data.company_site,
                            "city",
                            city,
                        )
                        update_task_status(task_id, "RUNNING", f"2GIS: Парсинг города {city}...")
                        logger.info(f"Task {task_id}: Starting 2GIS parser for city {city}...")
                        gis_result, gis_error = _run_parser_task(GisParser, gis_url, task_id, "2GIS")

                        if gis_result:
                            cards = gis_result.get("cards_data", [])
                            for card in cards:
                                card["source"] = "2gis"
                                card["city"] = city
                            all_cards.extend(cards)

                            if gis_result.get("aggregated_info"):
                                gis_stats_list.append(gis_result["aggregated_info"])

                    # Формируем агрегированную статистику по каждому источнику на основе списка городов
                    def _combine_stats(stats_list: List[Dict[str, Any]]) -> Dict[str, Any]:
                        combined = {
                            "search_query_name": form_data.company_name,
                            "total_cards_found": 0,
                            "aggregated_rating": 0.0,
                            "aggregated_reviews_count": 0,
                            "aggregated_positive_reviews": 0,
                            "aggregated_negative_reviews": 0,
                            "aggregated_answered_reviews_count": 0,
                            "aggregated_unanswered_reviews_count": 0,
                            "aggregated_avg_response_time": 0.0,
                            "aggregated_answered_reviews_percent": 0.0,
                        }
                        total_rating_sum = 0.0
                        total_rating_weight = 0
                        total_response_time_sum = 0.0
                        total_response_time_weight = 0

                        for s in stats_list:
                            combined["total_cards_found"] += s.get("total_cards_found", 0) or 0
                            reviews_cnt = s.get("aggregated_reviews_count", 0) or 0
                            if reviews_cnt > 0:
                                total_rating_sum += (s.get("aggregated_rating", 0.0) or 0.0) * reviews_cnt
                                total_rating_weight += reviews_cnt

                            combined["aggregated_reviews_count"] += reviews_cnt
                            combined["aggregated_positive_reviews"] += s.get("aggregated_positive_reviews", 0) or 0
                            combined["aggregated_negative_reviews"] += s.get("aggregated_negative_reviews", 0) or 0
                            answered = s.get("aggregated_answered_reviews_count", 0) or 0
                            unanswered = s.get("aggregated_unanswered_reviews_count", 0) or 0
                            combined["aggregated_answered_reviews_count"] += answered
                            combined["aggregated_unanswered_reviews_count"] += unanswered

                            resp_time = s.get("aggregated_avg_response_time", 0.0) or 0.0
                            if resp_time > 0 and answered > 0:
                                total_response_time_sum += resp_time * answered
                                total_response_time_weight += answered

                        if total_rating_weight > 0:
                            combined["aggregated_rating"] = round(total_rating_sum / total_rating_weight, 2)

                        if combined["aggregated_reviews_count"] > 0:
                            combined["aggregated_answered_reviews_percent"] = round(
                                (combined["aggregated_answered_reviews_count"] / combined["aggregated_reviews_count"]) * 100,
                                2,
                            )

                        if total_response_time_weight > 0:
                            combined["aggregated_avg_response_time"] = round(
                                total_response_time_sum / total_response_time_weight, 2
                            )

                        return combined

                    if yandex_stats_list:
                        statistics["yandex"] = _combine_stats(yandex_stats_list)
                    if gis_stats_list:
                        statistics["2gis"] = _combine_stats(gis_stats_list)

                else:
                    # Старое поведение: один общий поиск по стране или городу
                    yandex_url = _generate_yandex_url(
                        form_data.company_name, form_data.search_scope, form_data.location
                    )
                    gis_url = _generate_gis_url(
                        form_data.company_name,
                        form_data.company_site,
                        form_data.search_scope,
                        form_data.location,
                    )

                    update_task_status(task_id, "RUNNING", "Запуск парсера Яндекс...")
                    logger.info(
                        f"Task {task_id}: Starting Yandex parser first (sequential execution)..."
                    )
                    yandex_result, yandex_error = _run_parser_task(
                        YandexParser, yandex_url, task_id, "Yandex"
                    )

                    if is_task_stopped(task_id):
                        logger.info(f"Task {task_id}: stop flag detected after Yandex in 'both' mode, skipping 2GIS")
                        gis_result, gis_error = None, None
                    else:
                        update_task_status(task_id, "RUNNING", "Запуск парсера 2GIS...")
                        logger.info(
                            f"Task {task_id}: Starting 2GIS parser after Yandex (sequential execution)..."
                        )
                        gis_result, gis_error = _run_parser_task(
                            GisParser, gis_url, task_id, "2GIS"
                        )

                    # Собираем детальные карточки по обоим источникам
                    if yandex_result:
                        cards = yandex_result.get("cards_data", [])
                        for card in cards:
                            card["source"] = "yandex"
                        all_cards.extend(cards)

                        if yandex_result.get("aggregated_info"):
                            statistics["yandex"] = yandex_result["aggregated_info"]

                    if gis_result:
                        cards = gis_result.get("cards_data", [])
                        for card in cards:
                            card["source"] = "2gis"
                        all_cards.extend(cards)

                        if gis_result.get("aggregated_info"):
                            statistics["2gis"] = gis_result["aggregated_info"]

                # Формируем объединённую статистику по обоим источникам (для PDF и при необходимости)
                if statistics:
                    combined: Dict[str, Any] = {
                        'search_query_name': form_data.company_name,
                        'total_cards_found': 0,
                        'aggregated_rating': 0.0,
                        'aggregated_reviews_count': 0,
                        'aggregated_positive_reviews': 0,
                        'aggregated_negative_reviews': 0,
                        'aggregated_answered_reviews_count': 0,
                        'aggregated_unanswered_reviews_count': 0,
                        'aggregated_avg_response_time': 0.0,
                        'aggregated_answered_reviews_percent': 0.0,
                    }

                    total_rating_sum = 0.0
                    total_rating_weight = 0

                    for src_key in ['yandex', '2gis']:
                        src_stats = statistics.get(src_key)
                        if not src_stats:
                            continue

                        combined['total_cards_found'] += src_stats.get('total_cards_found', 0) or 0

                        reviews_cnt = src_stats.get('aggregated_reviews_count', 0) or 0
                        if reviews_cnt > 0:
                            total_rating_sum += (src_stats.get('aggregated_rating', 0.0) or 0.0) * reviews_cnt
                            total_rating_weight += reviews_cnt

                        combined['aggregated_reviews_count'] += reviews_cnt
                        combined['aggregated_positive_reviews'] += src_stats.get('aggregated_positive_reviews', 0) or 0
                        combined['aggregated_negative_reviews'] += src_stats.get('aggregated_negative_reviews', 0) or 0
                        combined['aggregated_answered_reviews_count'] += src_stats.get('aggregated_answered_reviews_count', 0) or 0
                        combined['aggregated_unanswered_reviews_count'] += src_stats.get('aggregated_unanswered_reviews_count', 0) or 0

                    if total_rating_weight > 0:
                        combined['aggregated_rating'] = round(total_rating_sum / total_rating_weight, 2)

                    if combined['aggregated_reviews_count'] > 0:
                        combined['aggregated_answered_reviews_percent'] = round(
                            (combined['aggregated_answered_reviews_count'] / combined['aggregated_reviews_count']) * 100,
                            2
                        )

                    statistics['combined'] = combined

                if all_cards:
                    writer = CSVWriter(settings=settings)
                    results_dir = settings.app_config.writer.output_dir
                    os.makedirs(results_dir, exist_ok=True)
                    output_path = os.path.join(results_dir, form_data.output_filename)
                    writer.set_file_path(output_path)

                    with writer:
                        for card in all_cards:
                            writer.write(card)

                    task = active_tasks[task_id]
                    task.result_file = form_data.output_filename
                    task.detailed_results = all_cards
                    task.statistics = statistics

                # Финальный статус с учётом возможной остановки пользователем
                cards_count = len(all_cards)
                if is_task_stopped(task_id):
                    update_task_status(
                        task_id,
                        TaskStatus.COMPLETED,
                        f"Парсинг остановлен пользователем. Найдено карточек: {cards_count}",
                    )
                elif yandex_error or gis_error:
                    update_task_status(
                        task_id,
                        TaskStatus.COMPLETED,
                        f"Завершено с ошибками: Yandex={bool(yandex_error)}, 2GIS={bool(gis_error)}",
                    )
                else:
                    update_task_status(
                        task_id,
                        TaskStatus.COMPLETED,
                        f"Парсинг завершен. Найдено карточек: {cards_count}",
                    )
            elif form_data.source == 'yandex':
                all_cards: List[Dict[str, Any]] = []
                stats: Dict[str, Any] = {}

                if cities_list:
                    # Парсинг поочерёдно по каждому городу
                    yandex_stats_list: List[Dict[str, Any]] = []
                    error: Optional[str] = None

                    for city in cities_list:
                        if is_task_stopped(task_id):
                            logger.info(f"Task {task_id}: stop flag detected before Yandex city '{city}' (single source), breaking city loop")
                            break
                        url = _generate_yandex_url(form_data.company_name, "city", city)
                        update_task_status(task_id, "RUNNING", f"Yandex: Парсинг города {city}...")
                        logger.info(f"Task {task_id}: Starting Yandex parser for city {city} (single source)...")
                        result, error = _run_parser_task(YandexParser, url, task_id, "Yandex")

                        if result and result.get("cards_data"):
                            for card in result["cards_data"]:
                                card["city"] = city
                            all_cards.extend(result["cards_data"])

                        if result and result.get("aggregated_info"):
                            yandex_stats_list.append(result["aggregated_info"])

                    if yandex_stats_list:
                        # Используем тот же помощник, что и выше, для объединения статистики
                        def _combine_stats_single(stats_list: List[Dict[str, Any]]) -> Dict[str, Any]:
                            combined = {
                                "search_query_name": form_data.company_name,
                                "total_cards_found": 0,
                                "aggregated_rating": 0.0,
                                "aggregated_reviews_count": 0,
                                "aggregated_positive_reviews": 0,
                                "aggregated_negative_reviews": 0,
                                "aggregated_answered_reviews_count": 0,
                                "aggregated_unanswered_reviews_count": 0,
                                "aggregated_avg_response_time": 0.0,
                                "aggregated_answered_reviews_percent": 0.0,
                            }
                            total_rating_sum = 0.0
                            total_rating_weight = 0
                            total_response_time_sum = 0.0
                            total_response_time_weight = 0

                            for s in stats_list:
                                combined["total_cards_found"] += s.get("total_cards_found", 0) or 0
                                reviews_cnt = s.get("aggregated_reviews_count", 0) or 0
                                if reviews_cnt > 0:
                                    total_rating_sum += (s.get("aggregated_rating", 0.0) or 0.0) * reviews_cnt
                                    total_rating_weight += reviews_cnt

                                combined["aggregated_reviews_count"] += reviews_cnt
                                combined["aggregated_positive_reviews"] += s.get("aggregated_positive_reviews", 0) or 0
                                combined["aggregated_negative_reviews"] += s.get("aggregated_negative_reviews", 0) or 0
                                answered = s.get("aggregated_answered_reviews_count", 0) or 0
                                unanswered = s.get("aggregated_unanswered_reviews_count", 0) or 0
                                combined["aggregated_answered_reviews_count"] += answered
                                combined["aggregated_unanswered_reviews_count"] += unanswered

                                resp_time = s.get("aggregated_avg_response_time", 0.0) or 0.0
                                if resp_time > 0 and answered > 0:
                                    total_response_time_sum += resp_time * answered
                                    total_response_time_weight += answered

                            if total_rating_weight > 0:
                                combined["aggregated_rating"] = round(total_rating_sum / total_rating_weight, 2)

                            if combined["aggregated_reviews_count"] > 0:
                                combined["aggregated_answered_reviews_percent"] = round(
                                    (combined["aggregated_answered_reviews_count"] / combined["aggregated_reviews_count"]) * 100,
                                    2,
                                )

                            if total_response_time_weight > 0:
                                combined["aggregated_avg_response_time"] = round(
                                    total_response_time_sum / total_response_time_weight, 2
                                )

                            return combined

                        stats["yandex"] = _combine_stats_single(yandex_stats_list)
                        stats["combined"] = stats["yandex"]

                    if all_cards:
                        writer = CSVWriter(settings=settings)
                        results_dir = settings.app_config.writer.output_dir
                        os.makedirs(results_dir, exist_ok=True)
                        writer.set_file_path(os.path.join(results_dir, form_data.output_filename))

                        with writer:
                            for card in all_cards:
                                writer.write(card)

                        task = active_tasks[task_id]
                        task.result_file = form_data.output_filename
                        task.detailed_results = all_cards
                        task.statistics = stats

                        msg = (
                            f"Парсинг остановлен пользователем. Найдено карточек: {len(all_cards)}"
                            if is_task_stopped(task_id)
                            else f"Парсинг завершен. Найдено карточек: {len(all_cards)}"
                        )
                        update_task_status(task_id, TaskStatus.COMPLETED, msg)
                    else:
                        update_task_status(
                            task_id, "COMPLETED", "Парсинг завершен. Карточки не найдены"
                        )
                else:
                    # Старое поведение для одного города / общего поиска
                    url = _generate_yandex_url(
                        form_data.company_name, form_data.search_scope, form_data.location
                    )
                    result, error = _run_parser_task(YandexParser, url, task_id, "Yandex")

                    if error:
                        update_task_status(task_id, "FAILED", f"Ошибка: {error}", error=error)
                    elif result and result.get("cards_data"):
                        writer = CSVWriter(settings=settings)
                        results_dir = settings.app_config.writer.output_dir
                        os.makedirs(results_dir, exist_ok=True)
                        writer.set_file_path(os.path.join(results_dir, form_data.output_filename))

                        with writer:
                            for card in result["cards_data"]:
                                writer.write(card)

                        task = active_tasks[task_id]
                        task.result_file = form_data.output_filename
                        task.detailed_results = result["cards_data"]

                        # Сохраняем агрегированную информацию, чтобы она отображалась в веб-отчёте и PDF
                        stats = {}
                        if result.get("aggregated_info"):
                            stats["yandex"] = result["aggregated_info"]
                            stats["combined"] = result["aggregated_info"]
                        task.statistics = stats

                        msg = (
                            f"Парсинг остановлен пользователем. Найдено карточек: {len(result['cards_data'])}"
                            if is_task_stopped(task_id)
                            else f"Парсинг завершен. Найдено карточек: {len(result['cards_data'])}"
                        )
                        update_task_status(task_id, TaskStatus.COMPLETED, msg)
                    else:
                        update_task_status(
                            task_id, "COMPLETED", "Парсинг завершен. Карточки не найдены"
                        )
            elif form_data.source == '2gis':
                all_cards: List[Dict[str, Any]] = []
                stats: Dict[str, Any] = {}

                if cities_list:
                    gis_stats_list: List[Dict[str, Any]] = []
                    error: Optional[str] = None

                    for city in cities_list:
                        if is_task_stopped(task_id):
                            logger.info(f"Task {task_id}: stop flag detected before 2GIS city '{city}' (single source), breaking city loop")
                            break
                        url = _generate_gis_url(
                            form_data.company_name,
                            form_data.company_site,
                            "city",
                            city,
                        )
                        update_task_status(task_id, "RUNNING", f"2GIS: Парсинг города {city}...")
                        logger.info(f"Task {task_id}: Starting 2GIS parser for city {city} (single source)...")
                        result, error = _run_parser_task(GisParser, url, task_id, "2GIS")

                        if result and result.get("cards_data"):
                            for card in result["cards_data"]:
                                card["city"] = city
                            all_cards.extend(result["cards_data"])

                        if result and result.get("aggregated_info"):
                            gis_stats_list.append(result["aggregated_info"])

                    if gis_stats_list:
                        def _combine_stats_single_gis(stats_list: List[Dict[str, Any]]) -> Dict[str, Any]:
                            combined = {
                                "search_query_name": form_data.company_name,
                                "total_cards_found": 0,
                                "aggregated_rating": 0.0,
                                "aggregated_reviews_count": 0,
                                "aggregated_positive_reviews": 0,
                                "aggregated_negative_reviews": 0,
                                "aggregated_answered_reviews_count": 0,
                                "aggregated_unanswered_reviews_count": 0,
                                "aggregated_avg_response_time": 0.0,
                                "aggregated_answered_reviews_percent": 0.0,
                            }
                            total_rating_sum = 0.0
                            total_rating_weight = 0
                            total_response_time_sum = 0.0
                            total_response_time_weight = 0

                            for s in stats_list:
                                combined["total_cards_found"] += s.get("total_cards_found", 0) or 0
                                reviews_cnt = s.get("aggregated_reviews_count", 0) or 0
                                if reviews_cnt > 0:
                                    total_rating_sum += (s.get("aggregated_rating", 0.0) or 0.0) * reviews_cnt
                                    total_rating_weight += reviews_cnt

                                combined["aggregated_reviews_count"] += reviews_cnt
                                combined["aggregated_positive_reviews"] += s.get("aggregated_positive_reviews", 0) or 0
                                combined["aggregated_negative_reviews"] += s.get("aggregated_negative_reviews", 0) or 0
                                answered = s.get("aggregated_answered_reviews_count", 0) or 0
                                unanswered = s.get("aggregated_unanswered_reviews_count", 0) or 0
                                combined["aggregated_answered_reviews_count"] += answered
                                combined["aggregated_unanswered_reviews_count"] += unanswered

                                resp_time = s.get("aggregated_avg_response_time", 0.0) or 0.0
                                if resp_time > 0 and answered > 0:
                                    total_response_time_sum += resp_time * answered
                                    total_response_time_weight += answered

                            if total_rating_weight > 0:
                                combined["aggregated_rating"] = round(total_rating_sum / total_rating_weight, 2)

                            if combined["aggregated_reviews_count"] > 0:
                                combined["aggregated_answered_reviews_percent"] = round(
                                    (combined["aggregated_answered_reviews_count"] / combined["aggregated_reviews_count"]) * 100,
                                    2,
                                )

                            if total_response_time_weight > 0:
                                combined["aggregated_avg_response_time"] = round(
                                    total_response_time_sum / total_response_time_weight, 2
                                )

                            return combined

                        stats["2gis"] = _combine_stats_single_gis(gis_stats_list)
                        stats["combined"] = stats["2gis"]

                    if all_cards:
                        writer = CSVWriter(settings=settings)
                        results_dir = settings.app_config.writer.output_dir
                        os.makedirs(results_dir, exist_ok=True)
                        writer.set_file_path(os.path.join(results_dir, form_data.output_filename))

                        with writer:
                            for card in all_cards:
                                writer.write(card)

                        task = active_tasks[task_id]
                        task.result_file = form_data.output_filename
                        task.detailed_results = all_cards
                        task.statistics = stats

                        update_task_status(
                            task_id,
                            "COMPLETED",
                            f"Парсинг завершен. Найдено карточек: {len(all_cards)}",
                        )
                    else:
                        update_task_status(
                            task_id, "COMPLETED", "Парсинг завершен. Карточки не найдены"
                        )
                else:
                    url = _generate_gis_url(
                        form_data.company_name,
                        form_data.company_site,
                        form_data.search_scope,
                        form_data.location,
                    )
                    result, error = _run_parser_task(GisParser, url, task_id, "2GIS")

                    if error:
                        update_task_status(task_id, "FAILED", f"Ошибка: {error}", error=error)
                    elif result and result.get("cards_data"):
                        writer = CSVWriter(settings=settings)
                        results_dir = settings.app_config.writer.output_dir
                        os.makedirs(results_dir, exist_ok=True)
                        writer.set_file_path(os.path.join(results_dir, form_data.output_filename))

                        with writer:
                            for card in result["cards_data"]:
                                writer.write(card)

                        task = active_tasks[task_id]
                        task.result_file = form_data.output_filename
                        task.detailed_results = result["cards_data"]

                        stats = {}
                        if result.get("aggregated_info"):
                            stats["2gis"] = result["aggregated_info"]
                            stats["combined"] = result["aggregated_info"]
                        task.statistics = stats

                        update_task_status(
                            task_id,
                            "COMPLETED",
                            f"Парсинг завершен. Найдено карточек: {len(result['cards_data'])}",
                        )
                    else:
                        update_task_status(
                            task_id, "COMPLETED", "Парсинг завершен. Карточки не найдены"
                        )
        except Exception as e:
            logger.error(f"Error in parsing task {task_id}: {e}", exc_info=True)
            update_task_status(task_id, "FAILED", f"Критическая ошибка: {str(e)}", error=str(e))
        finally:
            logger.info(f"Parsing thread finished for task {task_id}")

    thread = threading.Thread(target=run_parsing, daemon=True)
    thread.start()
    logger.info(f"Started parsing thread for task {task_id}")

    # Редиректим с учетом возможного префикса (например, /parser)
    url_prefix = get_url_prefix(request)
    return RedirectResponse(url=f"{url_prefix}/tasks/{task_id}", status_code=302)

@app.get("/tasks/{task_id}")
async def get_task(request: Request, task_id: str):
    try:
        url_prefix = get_url_prefix(request)

        if not check_auth(request):
            return RedirectResponse(url=f"{url_prefix}/login", status_code=302)

        task = active_tasks.get(task_id)
        if not task:
            return templates.TemplateResponse(
                "task_status.html",
                {
                    "request": request,
                    "task": None,
                    "error": "Task not found",
                    "url_prefix": url_prefix,
                },
            )

        # Флаг показа «проблемных карточек» и карточек без ответов
        show_problem_param = request.query_params.get("show_problem_cards", "").lower()
        show_problem_cards = show_problem_param in ("1", "true", "on", "yes")

        task_dict = {
            "task_id": task.task_id,
            "status": task.status,
            "progress": task.progress or "",
            "email": task.email or "",
            "source_info": task.source_info or {},
            "result_file": task.result_file or "",
            "error": task.error or "",
            # Передаём реальные datetime-объекты, чтобы шаблон мог использовать strftime
            "timestamp": task.timestamp if hasattr(task, 'timestamp') else None,
            "start_time": task.start_time if hasattr(task, 'start_time') else None,
            "end_time": task.end_time if hasattr(task, 'end_time') else None,
            "total_paused_duration": getattr(task, 'total_paused_duration', 0.0),
            "statistics": task.statistics if hasattr(task, 'statistics') else {},
            "detailed_results": task.detailed_results if hasattr(task, 'detailed_results') else []
        }

        cards = task.detailed_results if hasattr(task, 'detailed_results') and task.detailed_results else []
        statistics = task.statistics if hasattr(task, 'statistics') and task.statistics else {}

        # Гарантируем наличие поля "city" у каждой карточки для корректной работы
        # groupby('city') в шаблоне task_status.html. Без этого при строгих настройках
        # Jinja возможна ошибка вида "'dict object' has no attribute 'city'".
        normalized_cards = []
        for card in cards:
            if isinstance(card, dict):
                if "city" not in card or card.get("city") in (None, ""):
                    card = {**card, "city": "Город не указан"}
            normalized_cards.append(card)
        cards = normalized_cards
        output_dir = getattr(settings.app_config.writer, 'output_dir', './output') if hasattr(settings, 'app_config') and hasattr(settings.app_config, 'writer') else './output'

        return templates.TemplateResponse(
            "task_status.html",
            {
                "request": request,
                "task": task_dict,
                "statistics": statistics
                if (task.status == "COMPLETED" or task.status == "FAILED")
                else None,
                "cards": cards
                if (task.status == "COMPLETED" or task.status == "FAILED")
                else None,
                "summary_fields": SUMMARY_FIELDS,
                "output_dir": output_dir,
                "show_problem_cards": show_problem_cards,
                # Префикс корневого пути (для работы за reverse-proxy, например /parser)
                "url_prefix": url_prefix,
            },
        )
    except Exception as e:
        logger.error(f"Error in get_task endpoint for task {task_id}: {e}", exc_info=True)
        return templates.TemplateResponse(
            "task_status.html",
            {
                "request": request,
                "task": None,
                "error": f"Internal Server Error: {str(e)}",
                "show_problem_cards": False,
            },
        )

@app.get("/tasks/{task_id}/status")
async def get_task_status(task_id: str):
    task = active_tasks.get(task_id)
    if not task:
        return JSONResponse({"error": "Task not found"}, status_code=404)

    task_dict = {
        "task_id": task.task_id,
        "status": task.status,
        "progress": task.progress,
        "email": task.email,
        "source_info": task.source_info,
        "result_file": task.result_file,
        "error": task.error,
        "timestamp": str(task.timestamp),
        "statistics": task.statistics
    }
    return JSONResponse(task_dict)

@app.get("/api/task_status/{task_id}")
async def get_task_status_api(task_id: str):
    task = active_tasks.get(task_id)
    if not task:
        return JSONResponse({"error": "Task not found"}, status_code=404)

    task_dict = {
        "task_id": task.task_id,
        "status": task.status,
        "progress": task.progress,
        "email": task.email,
        "source_info": task.source_info,
        "result_file": task.result_file,
        "error": task.error,
        "timestamp": str(task.timestamp),
        "statistics": task.statistics
    }
    return JSONResponse(task_dict)

@app.get("/tasks")
async def get_all_tasks():
    tasks_list = []
    for task in active_tasks.values():
        task_dict = {
            "task_id": task.task_id,
            "status": task.status,
            "progress": task.progress,
            "email": task.email,
            "source_info": task.source_info,
            "result_file": task.result_file,
            "error": task.error,
            "timestamp": str(task.timestamp)
        }
        tasks_list.append(task_dict)
    return {"tasks": tasks_list}

@app.get("/tasks/{task_id}/download-pdf")
async def download_pdf_report(request: Request, task_id: str):
    if not check_auth(request):
        return RedirectResponse(url="/login", status_code=302)

    task = active_tasks.get(task_id)
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")

    if task.status != 'COMPLETED':
        raise HTTPException(status_code=400, detail="Task is not completed yet")

    try:
        results_dir = settings.app_config.writer.output_dir
        os.makedirs(results_dir, exist_ok=True)

        pdf_filename = f"report_{task_id}.pdf"
        pdf_path = os.path.join(results_dir, pdf_filename)

        pdf_writer = PDFWriter(settings=settings)
        company_name = task.source_info.get('company_name', 'Unknown')
        company_site = task.source_info.get('company_site', '')

        # Для PDF берём "плоскую" статистику:
        # 1) combined, если есть (оба источника)
        # 2) иначе yandex или 2gis, если есть только один
        # 3) иначе — то, что лежит в task.statistics как есть
        stats = task.statistics or {}
        if isinstance(stats, dict) and ('yandex' in stats or '2gis' in stats or 'combined' in stats):
            if stats.get('combined'):
                pdf_stats = stats['combined']
            elif stats.get('yandex') and not stats.get('2gis'):
                pdf_stats = stats['yandex']
            elif stats.get('2gis') and not stats.get('yandex'):
                pdf_stats = stats['2gis']
            else:
                pdf_stats = stats.get('combined') or {}
        else:
            pdf_stats = stats

        pdf_writer.generate_report(
            output_path=pdf_path,
            aggregated_data=pdf_stats or {},
            detailed_cards=task.detailed_results or [],
            company_name=company_name,
            company_site=company_site
        )

        return FileResponse(
            pdf_path,
            media_type='application/pdf',
            filename=pdf_filename,
            headers={"Content-Disposition": f"attachment; filename={pdf_filename}"}
        )
    except Exception as e:
        logger.error(f"Error generating PDF for task {task_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Error generating PDF: {str(e)}")


@app.post("/api/tasks/{task_id}/pause")
async def api_pause_task(request: Request, task_id: str):
    if not check_auth(request):
        return JSONResponse({"success": False, "error": "Unauthorized"}, status_code=401)

    success = pause_task(task_id)
    if success:
        return JSONResponse({"success": True})
    return JSONResponse({"success": False, "error": "Cannot pause this task"}, status_code=400)


@app.post("/api/tasks/{task_id}/resume")
async def api_resume_task(request: Request, task_id: str):
    if not check_auth(request):
        return JSONResponse({"success": False, "error": "Unauthorized"}, status_code=401)

    success = resume_task(task_id)
    if success:
        return JSONResponse({"success": True})
    return JSONResponse({"success": False, "error": "Cannot resume this task"}, status_code=400)


@app.post("/api/tasks/{task_id}/stop")
async def api_stop_task(request: Request, task_id: str):
    if not check_auth(request):
        return JSONResponse({"success": False, "error": "Unauthorized"}, status_code=401)

    success = stop_task(task_id)
    if success:
        # Флаг остановки выставлен; парсер завершит работу и сохранит частичный результат,
        # а фронт через polling дождётся статуса COMPLETED/FAILED и перезагрузит страницу.
        return JSONResponse({"success": True})
    return JSONResponse({"success": False, "error": "Cannot stop this task"}, status_code=400)


@app.post("/api/tasks/{task_id}/restart")
async def api_restart_task(request: Request, task_id: str):
    """
    Перезапуск последнего парсинга:
    - находит существующую задачу;
    - создаёт новую задачу с теми же параметрами (company_name, site, source, scope, location, cities, email);
    - запускает её в фоне и возвращает ID новой задачи.
    """
    if not check_auth(request):
        return JSONResponse({"success": False, "error": "Unauthorized"}, status_code=401)

    old_task = active_tasks.get(task_id)
    if not old_task:
        return JSONResponse({"success": False, "error": "Task not found"}, status_code=404)

    source_info = old_task.source_info or {}

    try:
        form_data = ParsingForm(
            company_name=source_info.get("company_name", ""),
            company_site=source_info.get("company_site", ""),
            source=source_info.get("source", ""),
            email=old_task.email or "",
            output_filename="report.csv",
            search_scope=source_info.get("search_scope", "country") or "country",
            location=source_info.get("location", "") or "",
            cities=source_info.get("cities", "") or "",
        )
    except Exception as e:
        logger.error(f"Failed to reconstruct form data for restart from task {task_id}: {e}", exc_info=True)
        return JSONResponse(
            {"success": False, "error": "Невозможно восстановить параметры задачи для перезапуска"},
            status_code=400,
        )

    # Создаём новую задачу и запускаем для неё тот же сценарий, что и при обычном старте
    new_task_id = create_task(
        email=form_data.email,
        source_info={
            "company_name": form_data.company_name,
            "company_site": form_data.company_site,
            "source": form_data.source,
            "search_scope": form_data.search_scope,
            "location": form_data.location,
            "cities": getattr(form_data, "cities", ""),
        },
    )
    logger.info(f"Restart requested: old task {task_id}, new task {new_task_id}")

    # Воспользуемся существующей логикой: запустим тот же run_parsing, что и в /start_parsing,
    # но с теми же параметрами form_data, просто под другим task_id.
    def run_parsing_restart():
        # Лёгкий способ — вызвать существующий эндпоинт start_parsing "как функцию",
        # но нам нужен новый task_id, поэтому минимально повторяем его логику:
        from copy import deepcopy
        # Создаём временный клон form_data, чтобы не трогать оригинал
        cloned_form = deepcopy(form_data)
        # Переиспользуем глобальный код старта: просто вызываем внутреннюю функцию,
        # имитируя тот же путь, что и в start_parsing.
        # Здесь мы делаем упрощённый путь: повторно вызываем _run_parser_task
        # ровно с теми же URL, что и в start_parsing, но для new_task_id.
        try:
            # Разбираем список городов для country-режима
            cities_list: List[str] = []
            if cloned_form.search_scope == 'country' and getattr(cloned_form, "cities", ""):
                cities_list = _parse_cities(cloned_form.cities)

            # Чтобы не тащить весь сложный код сюда, просто дергаем /start_parsing
            # через внутренний вызов, но это потребовало бы Request. Поэтому для
            # перезапуска поддерживаем только базовый сценарий: один общий поиск.
            if cloned_form.source == 'both':
                yandex_url = _generate_yandex_url(
                    cloned_form.company_name, cloned_form.search_scope, cloned_form.location
                )
                gis_url = _generate_gis_url(
                    cloned_form.company_name,
                    cloned_form.company_site,
                    cloned_form.search_scope,
                    cloned_form.location,
                )

                all_cards: List[Dict[str, Any]] = []
                statistics: Dict[str, Any] = {}

                yandex_result, yandex_error = _run_parser_task(YandexParser, yandex_url, new_task_id, "Yandex")
                if yandex_result:
                    cards = yandex_result.get("cards_data", [])
                    for card in cards:
                        card["source"] = "yandex"
                    all_cards.extend(cards)
                    if yandex_result.get("aggregated_info"):
                        statistics["yandex"] = yandex_result["aggregated_info"]

                gis_result, gis_error = _run_parser_task(GisParser, gis_url, new_task_id, "2GIS")
                if gis_result:
                    cards = gis_result.get("cards_data", [])
                    for card in cards:
                        card["source"] = "2gis"
                    all_cards.extend(cards)
                    if gis_result.get("aggregated_info"):
                        statistics["2gis"] = gis_result["aggregated_info"]

                if all_cards:
                    writer = CSVWriter(settings=settings)
                    results_dir = settings.app_config.writer.output_dir
                    os.makedirs(results_dir, exist_ok=True)
                    output_path = os.path.join(results_dir, cloned_form.output_filename)
                    writer.set_file_path(output_path)
                    with writer:
                        for card in all_cards:
                            writer.write(card)

                    task = active_tasks[new_task_id]
                    task.result_file = cloned_form.output_filename
                    task.detailed_results = all_cards
                    task.statistics = statistics

                    update_task_status(new_task_id, "COMPLETED", f"Парсинг завершен. Найдено карточек: {len(all_cards)}")
                else:
                    update_task_status(new_task_id, "COMPLETED", "Парсинг завершен. Карточки не найдены")
            else:
                # Один источник: повторно запускаем его так же, как в исходном коде
                source_name = "Yandex" if cloned_form.source.lower() == "yandex" else "2GIS"
                update_task_status(new_task_id, "RUNNING", f"{source_name}: Запуск парсера...")

                if cloned_form.source.lower() == "yandex":
                    url = _generate_yandex_url(cloned_form.company_name, cloned_form.search_scope, cloned_form.location)
                    parser_class = YandexParser
                else:
                    url = _generate_gis_url(
                        cloned_form.company_name,
                        cloned_form.company_site,
                        cloned_form.search_scope,
                        cloned_form.location,
                    )
                    parser_class = GisParser

                result, error = _run_parser_task(parser_class, url, new_task_id, source_name)

                if result and isinstance(result, dict):
                    cards = result.get("cards_data", [])
                    for card in cards:
                        card["source"] = cloned_form.source.lower()

                    writer = CSVWriter(settings=settings)
                    results_dir = settings.app_config.writer.output_dir
                    os.makedirs(results_dir, exist_ok=True)
                    output_path = os.path.join(results_dir, cloned_form.output_filename)
                    writer.set_file_path(output_path)
                    with writer:
                        for card in cards:
                            writer.write(card)

                    task = active_tasks[new_task_id]
                    task.result_file = cloned_form.output_filename
                    task.detailed_results = cards
                    task.statistics = {
                        cloned_form.source.lower(): result.get("aggregated_info", {})
                    }

                    update_task_status(
                        new_task_id,
                        "COMPLETED",
                        f"Парсинг завершен. Найдено карточек: {len(cards)}",
                    )
                else:
                    if error:
                        update_task_status(
                            new_task_id,
                            "FAILED",
                            f"{source_name}: Ошибка парсинга: {str(error)}",
                            error=str(error),
                        )
                    else:
                        update_task_status(
                            new_task_id,
                            "COMPLETED",
                            "Парсинг завершен. Карточки не найдены",
                        )
        except Exception as e:
            logger.error(f"Error in restart parsing task {new_task_id}: {e}", exc_info=True)
            update_task_status(new_task_id, "FAILED", f"Критическая ошибка: {str(e)}", error=str(e))

    thread = threading.Thread(target=run_parsing_restart, daemon=True)
    thread.start()
    logger.info(f"Started restart parsing thread for new task {new_task_id}")

    return JSONResponse({"success": True, "new_task_id": new_task_id})

