from __future__ import annotations
import json
import os
import re
import logging
import time
import urllib.parse
import hashlib
from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime, timedelta

from bs4 import BeautifulSoup, Tag
from pydantic import BaseModel, Field
from selenium.webdriver.remote.webelement import WebElement as SeleniumWebElement

from src.drivers.base_driver import BaseDriver, DOMNode
from src.config.settings import Settings
from src.parsers.base_parser import BaseParser
from src.parsers.date_parser import parse_russian_date, format_russian_date

logger = logging.getLogger(__name__)


class YandexParser(BaseParser):
    def __init__(self, driver: BaseDriver, settings: Settings):
        if not isinstance(driver, BaseDriver):
            raise TypeError("YandexParser requires a BaseDriver instance.")

        super().__init__(driver, settings)
        self._url: str = ""

        self._captcha_wait_time: int = getattr(self._settings.parser, 'yandex_captcha_wait', 20)
        self._reviews_scroll_step: int = getattr(self._settings.parser, 'yandex_reviews_scroll_step', 500)
        self._reviews_scroll_iterations_max: int = getattr(self._settings.parser, 'yandex_reviews_scroll_max_iter', 100)
        self._reviews_scroll_iterations_min: int = getattr(self._settings.parser, 'yandex_reviews_scroll_min_iter', 30)
        self._max_records: int = getattr(self._settings.parser, 'max_records', 1000)

        self._card_selectors: List[str] = getattr(self._settings.parser, 'yandex_card_selectors', [
            "a[href*='/maps/org/']:not([href*='/gallery/'])",
            "a[href*='/org/']:not([href*='/gallery/'])",
            "div.search-business-snippet-view",
            "div.search-snippet-view__body._type_business",
            "li[class*='search'] a[href*='/org/']",
        ])
        self._pagination_selectors: List[str] = getattr(self._settings.parser, 'yandex_pagination_selectors', [
            "a[href*='/search/'][href*='page=']",
            "a[href*='page=']"
        ])
        self._scroll_container: str = getattr(self._settings.parser, 'yandex_scroll_container', 
                                               ".scroll__container, .scroll__content, .search-list-view__list")
        self._scrollable_element_selector: str = getattr(self._settings.parser, 'yandex_scrollable_element_selector',
                                                         ".scroll__container, .scroll__content, [class*='search-list-view'], [class*='scroll']")
        self._scroll_step: int = getattr(self._settings.parser, 'yandex_scroll_step', 400)
        self._scroll_max_iter: int = getattr(self._settings.parser, 'yandex_scroll_max_iter', 200)
        self._scroll_wait_time: float = getattr(self._settings.parser, 'yandex_scroll_wait_time', 1.5)
        self._min_cards_threshold: int = getattr(self._settings.parser, 'yandex_min_cards_threshold', 500)

        self._data_mapping: Dict[str, str] = {
            'search_query_name': 'Название поиска',
            'total_cards_found': 'Всего карточек найдено',
            'aggregated_rating': 'Общий рейтинг',
            'aggregated_reviews_count': 'Всего отзывов',
            'aggregated_positive_reviews': 'Всего положительных отзывов (4–5⭐)',
            'aggregated_negative_reviews': 'Всего отрицательных отзывов (1–2⭐)',
            'aggregated_avg_response_time': 'Среднее время ответа (дни)',

            'card_name': 'Название карточки',
            'card_address': 'Адрес карточки',
            'card_rating': 'Рейтинг карточки',
            'card_reviews_count': 'Отзывов по карточке',
            'card_website': 'Сайт карточки',
            'card_phone': 'Телефон карточки',
            'card_rubrics': 'Рубрики карточки',
            'card_response_status': 'Статус ответа (карточка)',
            'card_avg_response_time': 'Среднее время ответа (дни, карточка)',
            'card_reviews_positive': 'Положительных отзывов (карточка, 4–5⭐)',
            'card_reviews_negative': 'Отрицательных отзывов (карточка, 1–2⭐)',
            'card_reviews_neutral': 'Нейтральных отзывов (карточка, 3⭐)',
            'card_reviews_texts': 'Тексты отзывов (карточка)',
            'review_rating': 'Оценка отзыва',
            'review_text': 'Текст отзыва',
        }

        self._current_page_number: int = 1
        self._aggregated_data: Dict[str, Any] = {
            'total_cards': 0,
            'total_rating_sum': 0.0,
            'total_reviews_count': 0,
            'total_positive_reviews': 0,
            'total_negative_reviews': 0,
            'total_neutral_reviews': 0,
            'total_answered_count': 0,
            'total_answered_reviews_count': 0,
            'total_unanswered_reviews_count': 0,
            'total_response_time_sum_days': 0.0,
            'total_response_time_calculated_count': 0,
        }
        self._collected_card_data: List[Dict[str, Any]] = []
        self._search_query_name: str = ""

    def _estimate_sentiment(self, text: str) -> int:
        """
        Грубая эвристика для оценки «смысла» отзыва по тексту:
        - возвращает 1 для явно положительных текстов
        - -1 для явно отрицательных
        - 0, если сигнал слабый/смешанный.
        Используется только как корректировка к звёздам, если они
        явно противоречат содержимому отзыва.
        """
        if not text:
            return 0

        t = text.lower()

        positive_markers = [
            "очень хороший", "очень хорошо", "весьма удачно", "весьма хороший",
            "рекомендую", "советую", "могу порекомендовать", "буду рекомендовать",
            "понравил", "нравит", "нравится", "понравилось",
            "отличн", "классн", "замечательн", "прекрасн", "шикарн",
            "доволен", "довольн", "приятн", "порадовал", "порадовали",
            "милые продавцы", "мило встретили", "добрые продавцы",
            "положительное впечатление", "положительные впечатления",
            "приятное впечатление", "осталось впечатление", "бренд приятно удивляет",
        ]

        negative_markers = [
            "ужасн", "отвратительн", "отвратит", "кошмар", "худший",
            "плох", "ничего хорошего", "разочаров", "разочарован",
            "агрессивн", "хамств", "груб", "грубо", "нагл", "хамы",
            "обман", "обманули", "обманщик", "мошенник",
            "помощи ноль", "не помогли", "не помог", "игнорир",
            "отвратительное отношение", "ужасное отношение",
        ]

        score = 0
        for kw in positive_markers:
            if kw in t:
                score += 1
        for kw in negative_markers:
            if kw in t:
                score -= 1

        if score >= 2:
            return 1
        if score <= -2:
            return -1
        return 0

    def get_url_pattern(self) -> str:
        return r'https?://yandex\.ru/maps/\?.*'

    def _get_page_source_and_soup(self) -> Tuple[str, BeautifulSoup]:
        page_source = self.driver.get_page_source()
        soup = BeautifulSoup(page_source, "lxml")
        return page_source, soup

    def check_captcha(self) -> None:
        page_source, soup = self._get_page_source_and_soup()

        is_captcha = soup.find("div", {"class": "CheckboxCaptcha"}) or \
                     soup.find("div", {"class": "AdvancedCaptcha"})

        if is_captcha:
            logger.warning(f"Captcha detected. Waiting for {self._captcha_wait_time} seconds.")
            time.sleep(self._captcha_wait_time)
            self.check_captcha()

    def _get_card_snippet_data(self, card_element: Tag) -> Optional[Dict[str, Any]]:
        try:
            name_selectors = [
                'h1.card-title-view__title',
                '.search-business-snippet-view__title',
                'a.search-business-snippet-view__title',
                'a.catalogue-snippet-view__title',
                'a[class*="title"]',
                'h2[class*="title"]',
                'h3[class*="title"]',
            ]
            name = ''
            for selector in name_selectors:
                name_element = card_element.select_one(selector)
                if name_element:
                    name = name_element.get_text(strip=True)
                    if name:
                        break

            address_selectors = [
                'div.business-contacts-view__address-link',
                '.search-business-snippet-view__address',
                'div[class*="address"]',
                'span[class*="address"]',
            ]
            address = ''
            for selector in address_selectors:
                address_element = card_element.select_one(selector)
                if address_element:
                    address = address_element.get_text(strip=True)
                    if address:
                        break

            rating_selectors = [
                'span.business-rating-badge-view__rating-text',
                '.search-business-snippet-view__rating-text',
                'span[class*="rating"]',
                'div[class*="rating"]',
            ]
            rating = ''
            for selector in rating_selectors:
                rating_element = card_element.select_one(selector)
                if rating_element:
                    rating = rating_element.get_text(strip=True)
                    if rating:
                        break

            reviews_selectors = [
                'a.business-review-view__rating',
                '.search-business-snippet-view__link-reviews',
                'a[class*="review"]',
                'span[class*="review"]',
            ]
            reviews_count = 0
            for selector in reviews_selectors:
                reviews_element = card_element.select_one(selector)
                if reviews_element:
                    reviews_count_text = reviews_element.get_text(strip=True)
                    if reviews_count_text:
                        match = re.search(r'(\d+)', reviews_count_text)
                        if match:
                            reviews_count = int(match.group(0))
                            break
                if reviews_count > 0:
                    break

            website_selectors = [
                'a[itemprop="url"]',
                'a[class*="website"]',
                'a[href^="http"]',
            ]
            website = ''
            for selector in website_selectors:
                website_element = card_element.select_one(selector)
                if website_element:
                    website = website_element.get('href', '')
                    if website and 'yandex.ru' not in website:
                        break

            phone_selectors = [
                'span.business-contacts-view__phone-number',
                'a[href^="tel:"]',
                'span[class*="phone"]',
            ]
            phone = ''
            for selector in phone_selectors:
                phone_element = card_element.select_one(selector)
                if phone_element:
                    phone = phone_element.get_text(strip=True)
                    if not phone and phone_element.get('href'):
                        phone = phone_element.get('href').replace('tel:', '').strip()
                    if phone:
                        phone = phone.replace('Показать телефон', '').replace('показать телефон', '').strip()
                        break

            rubrics_elements = card_element.select('a.rubric-view__title, a[class*="rubric"], a[href*="/rubric/"]')
            rubrics = "; ".join([r.get_text(strip=True) for r in rubrics_elements]) if rubrics_elements else ''

            normalized_name = self._normalize_card_name(name)

            return {
                'card_name': normalized_name,
                'card_address': address,
                'card_rating': rating,
                'card_reviews_count': reviews_count,
                'card_website': website,
                'card_phone': phone,
                'card_rubrics': rubrics,
                'card_response_status': "UNKNOWN",
                'card_avg_response_time': "",
                'card_reviews_positive': 0,
                'card_reviews_negative': 0,
                'card_reviews_texts': "",
                'card_answered_reviews_count': 0,
                'card_unanswered_reviews_count': reviews_count,
                'detailed_reviews': [],
                'review_rating': None,
                'review_text': None,
            }
        except Exception as e:
            logger.error(f"Error processing Yandex card snippet: {e}")
            return None

    def _extract_card_data_from_detail_page(self, card_details_soup: BeautifulSoup) -> Optional[Dict[str, Any]]:
        try:
            card_snippet = {
                'card_name': '',
                'card_address': '',
                'card_rating': '',
                'card_reviews_count': 0,
                'card_website': '',
                'card_phone': '',
                'card_rubrics': '',
                'card_response_status': "UNKNOWN",
                'card_avg_response_time': "",
                'card_reviews_positive': 0,
                'card_reviews_negative': 0,
                'card_reviews_texts': "",
                'card_answered_reviews_count': 0,
                'card_unanswered_reviews_count': 0,
                'detailed_reviews': [],
            }
            

            name_selectors = [
                'div.search-placemark-view__title',
                'h1.card-title-view__title',
                'h1[class*="title"]',
                'h1[class*="card-title"]',
                'h1.business-card-title-view__title',
                'h1',
                'div[class*="title"]',
                'span[class*="title"]',
            ]
            
            name_detail = None
            for selector in name_selectors:
                name_detail = card_details_soup.select_one(selector)
                if name_detail:
                    name_text = name_detail.get_text(strip=True)
                    if name_text:
                        normalized_name = self._normalize_card_name(name_text)
                        card_snippet['card_name'] = normalized_name
                        logger.debug(f"Found card name using selector '{selector}': {normalized_name[:50]}")
                        break
            
            if not card_snippet.get('card_name'):
                logger.warning(f"Could not find card name on detail page. Available h1 tags: {[h.get_text(strip=True)[:50] for h in card_details_soup.select('h1')]}")


            address_selectors = [
                'a.orgpage-header-view__address',
                'a[href*="/house/"]',
                'div.business-contacts-view__address-link',
                'div[class*="address"]',
                'span[class*="address"]',
                'div[class*="location"]',
                'span[class*="location"]',
                '[itemprop="address"]',
                'div[data-test="address"]',
            ]
            
            address_detail = None
            for selector in address_selectors:
                address_detail = card_details_soup.select_one(selector)
                if address_detail:
                    address_text = address_detail.get_text(strip=True)
                    if address_text and len(address_text) > 5:
                        card_snippet['card_address'] = address_text
                        logger.debug(f"Found card address using selector '{selector}': {address_text[:50]}")
                        break
            

            if card_snippet.get('card_address'):
                card_snippet['card_address'] = self._normalize_address(card_snippet['card_address'])
            
            if not card_snippet.get('card_address') or len(card_snippet.get('card_address', '').strip()) < 5:
                logger.warning(f"Card address not found for card: {card_snippet.get('card_name', 'Unknown')[:50]}")
                card_snippet['card_address'] = ''

            rating_selectors = [
                'span.business-rating-badge-view__rating-text',
                'div.search-placemark-view__rating',
                'div[class*="business-rating-view"]',
                'span[class*="rating"]',
            ]
            rating_detail = None
            for selector in rating_selectors:
                rating_detail = card_details_soup.select_one(selector)
                if rating_detail:
                    rating_text = rating_detail.get_text(strip=True)
                    if rating_text:
                        card_snippet['card_rating'] = rating_text
                        break
            if not card_snippet.get('card_rating'):
                card_snippet['card_rating'] = ''

            website_detail = card_details_soup.select_one('a[itemprop="url"], .business-website-view__link')
            card_snippet['card_website'] = website_detail.get('href') if website_detail else ''


            phone_selectors = [
                'div.orgpage-phones-view',
                'a[href^="tel:"]',
                'span.business-contacts-view__phone-number',
                'span[class*="phone"]',
                'div[class*="phone"]',
                'span[itemprop="telephone"]',
                'a.business-contacts-view__phone-link',
            ]
            
            phone_text = ""
            for selector in phone_selectors:
                phone_elements = card_details_soup.select(selector)
                if phone_elements:
                    for phone_elem in phone_elements:
                        phone_text = phone_elem.get_text(strip=True)
                        if not phone_text and phone_elem.get('href'):
                            href = phone_elem.get('href', '')
                            if href.startswith('tel:'):
                                phone_text = href.replace('tel:', '').strip()
                        if phone_text:
                            phone_text = phone_text.replace('Показать телефон', '').replace('показать телефон', '').strip()
                            break
                if phone_text:
                    break
            
            card_snippet['card_phone'] = phone_text


            rubric_selectors = [
                'a.rubric-view__title',
                'a[class*="rubric"]',
                'span[class*="rubric"]',
                'div[class*="rubric"]',
                'a[href*="/rubric/"]',
            ]
            
            rubrics_list = []
            for selector in rubric_selectors:
                rubrics_detail = card_details_soup.select(selector)
                if rubrics_detail:
                    for r in rubrics_detail:
                        rubric_text = r.get_text(strip=True)
                        if rubric_text and rubric_text not in rubrics_list:
                            rubrics_list.append(rubric_text)
                    if rubrics_list:
                        break
            
            card_snippet['card_rubrics'] = "; ".join(rubrics_list) if rubrics_list else ""


            response_selectors = [
                '.business-header-view__quick-response-badge',
                'div[class*="response"]',
                'span[class*="response"]',
                'div.business-response-view',
            ]
            
            response_status = "UNKNOWN"
            for selector in response_selectors:
                response_status_element = card_details_soup.select_one(selector)
                if response_status_element:
                    response_text = response_status_element.get_text(strip=True)
                    if response_text:
                        response_status = response_text
                        break
            
            card_snippet['card_response_status'] = response_status
            
            time_selectors = [
                '.business-header-view__avg-response-time',
                'div[class*="response-time"]',
                'span[class*="response-time"]',
            ]
            
            avg_response_time_text = ""
            for selector in time_selectors:
                avg_response_time_element = card_details_soup.select_one(selector)
                if avg_response_time_element:
                    avg_response_time_text = avg_response_time_element.get_text(strip=True)
                    if avg_response_time_text:
                        break
            
            if avg_response_time_text:
                if "час" in avg_response_time_text.lower() or "hour" in avg_response_time_text.lower():
                    match = re.search(r'(\d+(\.\d+)?)\s*(час|hour)', avg_response_time_text, re.IGNORECASE)
                    if match:
                        hours = float(match.group(1))
                        card_snippet['card_avg_response_time'] = round(hours / 24, 2)
                elif "день" in avg_response_time_text.lower() or "day" in avg_response_time_text.lower():
                    match = re.search(r'(\d+(\.\d+)?)\s*(день|day)', avg_response_time_text, re.IGNORECASE)
                    if match:
                        card_snippet['card_avg_response_time'] = float(match.group(1))
                elif "недел" in avg_response_time_text.lower() or "week" in avg_response_time_text.lower():
                    match = re.search(r'(\d+(\.\d+)?)\s*(недел|week)', avg_response_time_text, re.IGNORECASE)
                    if match:
                        weeks = float(match.group(1))
                        card_snippet['card_avg_response_time'] = weeks * 7
                elif "месяц" in avg_response_time_text.lower() or "month" in avg_response_time_text.lower():
                    match = re.search(r'(\d+(\.\d+)?)\s*(месяц|month)', avg_response_time_text, re.IGNORECASE)
                    if match:
                        months = float(match.group(1))
                        card_snippet['card_avg_response_time'] = months * 30
                else:
                    card_snippet['card_avg_response_time'] = ""
            else:
                card_snippet['card_avg_response_time'] = ""

            reviews_data = self._get_card_reviews_info()
            details = reviews_data.get('details', [])

            # Базовые счётчики по отзывам
            # Используем фактическое количество найденных отзывов (details), а не значение из snippet
            actual_reviews_count = len(details) if details else reviews_data.get('reviews_count', 0)
            card_snippet['card_reviews_count'] = actual_reviews_count
            card_snippet['card_reviews_positive'] = reviews_data.get('positive_reviews', 0)
            card_snippet['card_reviews_negative'] = reviews_data.get('negative_reviews', 0)
            card_snippet['card_reviews_neutral'] = reviews_data.get('neutral_reviews', 0)

            # Формируем краткое поле с текстами отзывов, убирая служебные подписи
            review_texts = []
            for detail in details:
                txt = (detail.get('review_text') or "").strip()
                if not txt:
                    continue
                # Удаляем служебные подписи типа "Подписаться" из краткого поля
                txt_clean = re.sub(r'\bПодписаться\b', '', txt, flags=re.IGNORECASE).strip()
                if txt_clean:
                    review_texts.append(txt_clean)
            card_snippet['card_reviews_texts'] = "; ".join(review_texts)
            card_snippet['detailed_reviews'] = details

            # Считаем количество отвеченных/неотвеченных отзывов по детальным данным
            answered_reviews_count = sum(1 for d in details if d.get('has_response'))
            card_snippet['card_answered_reviews_count'] = answered_reviews_count
            card_snippet['card_unanswered_reviews_count'] = max(
                0, card_snippet['card_reviews_count'] - answered_reviews_count
            )

            # Среднее время ответа по карточке (в днях) по детальным отзывам
            deltas: List[float] = []
            for d in details:
                if d.get('has_response') and d.get('review_date') and d.get('response_date'):
                    try:
                        rd = parse_russian_date(str(d.get('review_date')))
                        respd = parse_russian_date(str(d.get('response_date')))
                        if rd and respd and respd >= rd:
                            delta_days = (respd - rd).days
                            if delta_days >= 0:
                                deltas.append(float(delta_days))
                    except Exception:
                        continue
            if deltas:
                card_snippet['card_avg_response_time'] = round(sum(deltas) / len(deltas), 2)
            else:
                # если не удалось посчитать по детальным данным, оставляем как есть (возможен парсинг из UI)
                if 'card_avg_response_time' not in card_snippet:
                    card_snippet['card_avg_response_time'] = ""

            if not card_snippet.get('card_name'):
                logger.warning(f"Card name is empty. Card snippet keys: {list(card_snippet.keys())}")
                try:
                    debug_html_path = os.path.join('output', f'debug_card_no_name_{int(time.time())}.html')
                    os.makedirs('output', exist_ok=True)
                    with open(debug_html_path, 'w', encoding='utf-8') as f:
                        f.write(str(card_details_soup))
                    logger.info(f"Saved debug HTML to {debug_html_path}")
                except Exception as e:
                    logger.error(f"Could not save debug HTML: {e}")
                return None

            card_snippet['source'] = 'yandex'
            
            logger.debug(f"Successfully extracted card data: name='{card_snippet.get('card_name', '')[:50]}', address='{card_snippet.get('card_address', '')[:50]}'")
            return card_snippet
        except Exception as e:
            logger.error(f"Error extracting card data from detail page: {e}", exc_info=True)
            return None

    def _update_aggregated_data(self, card_snippet: Dict[str, Any]) -> None:
        try:
            rating_str = str(card_snippet.get('card_rating', '')).replace(',', '.').strip()
            try:
                card_rating_float = float(rating_str) if rating_str and rating_str.replace('.', '', 1).isdigit() else 0.0
            except (ValueError, TypeError):
                card_rating_float = 0.0

            self._aggregated_data['total_rating_sum'] += card_rating_float

            reviews_count = card_snippet.get('card_reviews_count', 0) or 0
            positive_reviews = card_snippet.get('card_reviews_positive', 0) or 0
            negative_reviews = card_snippet.get('card_reviews_negative', 0) or 0
            neutral_reviews = card_snippet.get('card_reviews_neutral', 0) or 0
            answered_reviews = card_snippet.get('card_answered_reviews_count', 0) or 0

            self._aggregated_data['total_reviews_count'] += reviews_count
            self._aggregated_data['total_positive_reviews'] += positive_reviews
            self._aggregated_data['total_negative_reviews'] += negative_reviews
            self._aggregated_data['total_neutral_reviews'] += neutral_reviews
            self._aggregated_data['total_answered_reviews_count'] += answered_reviews
            self._aggregated_data['total_unanswered_reviews_count'] += max(0, reviews_count - answered_reviews)

            if card_snippet.get('card_response_status') != 'UNKNOWN' or answered_reviews > 0:
                self._aggregated_data['total_answered_count'] += 1

            if card_snippet.get('card_avg_response_time'):
                try:
                    response_time_str = str(card_snippet['card_avg_response_time']).strip()
                    if response_time_str:
                        response_time_days = float(response_time_str)
                        if response_time_days > 0:
                            self._aggregated_data['total_response_time_sum_days'] += response_time_days
                            self._aggregated_data['total_response_time_calculated_count'] += 1
                except (ValueError, TypeError):
                    logger.warning(
                        f"Could not convert response time to float for card '{card_snippet.get('card_name', 'Unknown')}': {card_snippet.get('card_avg_response_time')}")

            logger.info(f"Aggregated data updated for '{card_snippet.get('card_name', 'Unknown')}': "
                       f"rating={card_rating_float}, reviews={reviews_count}, "
                       f"positive={positive_reviews}, negative={negative_reviews}")
        except Exception as e:
            logger.warning(
                f"Could not parse rating or other data for aggregation for card '{card_snippet.get('card_name', 'Unknown')}': {e}", exc_info=True)

    def _get_card_reviews_info(self) -> Dict[str, Any]:
        reviews_info = {
            'reviews_count': 0,
            'positive_reviews': 0,
            'negative_reviews': 0,
            'neutral_reviews': 0,
            'texts': [],
            'details': [],
        }

        try:
            page_source, soup_content = self._get_page_source_and_soup()
        except Exception as e:
            logger.warning(f"Failed to get page source before handling reviews: {e}")
            return reviews_info

        reviews_count_total = 0
        reviews_url = ""
        try:
            reviews_link = soup_content.select_one('a.tabs-select-view__label[href*="/reviews/"], a[href*="/reviews/"]')
            if reviews_link:
                reviews_url = reviews_link.get('href')
                if reviews_url:
                    if not reviews_url.startswith('http'):
                        if reviews_url.startswith('/'):
                            reviews_url = urllib.parse.urljoin("https://yandex.ru", reviews_url)
                        else:
                            current_url = self.driver.current_url if hasattr(self.driver, 'current_url') else ''
                            if current_url:
                                base_url = '/'.join(current_url.split('/')[:4])
                                reviews_url = f"{base_url}/reviews/"
                    logger.info(f"Navigating to reviews page: {reviews_url}")
                    try:
                        self.driver.navigate(reviews_url)
                        time.sleep(3)
                        page_source, soup_content = self._get_page_source_and_soup()

                        # Сохраняем HTML вкладки отзывов для отладки извлечения рейтинга/текста
                        try:
                            import os
                            debug_dir = os.path.join("debug", "yandex_reviews")
                            os.makedirs(debug_dir, exist_ok=True)

                            org_id_match = re.search(r'/org/[^/]+/(\d+)', reviews_url)
                            org_id = org_id_match.group(1) if org_id_match else hashlib.md5(
                                reviews_url.encode("utf-8", errors="ignore")
                            ).hexdigest()[:8]

                            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
                            debug_path = os.path.join(debug_dir, f"reviews_{org_id}_{ts}.html")
                            with open(debug_path, "w", encoding="utf-8") as f:
                                f.write(page_source)
                            logger.info(f"Saved Yandex reviews debug HTML to {debug_path}")
                        except Exception as dump_error:
                            logger.warning(f"Could not save Yandex reviews debug HTML: {dump_error}")

                    except Exception as nav_error:
                        logger.warning(f"Could not navigate to reviews page: {nav_error}")
            else:
                current_url = self.driver.current_url if hasattr(self.driver, 'current_url') else ''
                if current_url and '/maps/org/' in current_url:
                    reviews_url = current_url.rstrip('/') + '/reviews/'
                    logger.info(f"Constructing reviews URL from current URL: {reviews_url}")
                    try:
                        self.driver.navigate(reviews_url)
                        time.sleep(3)
                        page_source, soup_content = self._get_page_source_and_soup()
                    except Exception as nav_error:
                        logger.warning(f"Could not navigate to constructed reviews page: {nav_error}")
        except Exception as e:
            logger.warning(f"Error trying to navigate to reviews: {e}")
            
        if not reviews_url:
            current_url = self.driver.current_url if hasattr(self.driver, 'current_url') else ''
            if current_url and '/maps/org/' in current_url:
                reviews_url = current_url.rstrip('/') + '/reviews/'
            elif current_url and '/reviews/' in current_url:
                reviews_url = current_url

        try:
            count_selectors = [
                'div.tabs-select-view__counter',
                '.search-business-snippet-view__link-reviews',
                'a[href*="/reviews/"]',
                'span.business-rating-badge-view__reviews-count',
                'div.business-header-view__reviews-count',
                'a.business-review-view__rating',
            ]

            for selector in count_selectors:
                count_elements = soup_content.select(selector)
                if count_elements:
                    for elem in count_elements:
                        reviews_count_text = elem.get_text(strip=True)
                        matches = re.findall(r'(\d+)', reviews_count_text)
                        if matches:
                            potential_count = max([int(m) for m in matches])
                            if potential_count > reviews_count_total:
                                reviews_count_total = potential_count
                                logger.info(f"Found reviews count {reviews_count_total} using selector: {selector}")

            if reviews_count_total > 0:
                logger.info(f"Total reviews found on page: {reviews_count_total}")
            else:
                logger.warning("Could not find reviews count element. Trying to navigate to reviews tab...")
                try:
                    reviews_tab = soup_content.select_one('a[href*="/reviews/"], button[data-tab="reviews"]')
                    if reviews_tab:
                        reviews_url = reviews_tab.get('href')
                        if reviews_url:
                            if not reviews_url.startswith('http'):
                                reviews_url = urllib.parse.urljoin("https://yandex.ru", reviews_url)
                            logger.info(f"Navigating to reviews page: {reviews_url}")
                            self.driver.navigate(reviews_url)
                            time.sleep(3)
                            page_source, soup_content = self._get_page_source_and_soup()
                            for selector in count_selectors:
                                count_elements = soup_content.select(selector)
                                if count_elements:
                                    for elem in count_elements:
                                        reviews_count_text = elem.get_text(strip=True)
                                        matches = re.findall(r'(\d+)', reviews_count_text)
                                        if matches:
                                            potential_count = max([int(m) for m in matches])
                                            if potential_count > reviews_count_total:
                                                reviews_count_total = potential_count
                except Exception as nav_error:
                    logger.warning(f"Could not navigate to reviews tab: {nav_error}")
        except (ValueError, AttributeError, IndexError) as e:
            logger.warning(f"Could not determine review count: {e}")
        except Exception as e:
            logger.error(f"Unexpected error getting review count: {e}")
            return reviews_info

        if reviews_count_total == 0:
            logger.warning("No reviews found or reviews count is 0")
            return reviews_info

        scroll_iterations = 0
        max_scroll_iterations = self._reviews_scroll_iterations_max
        min_scroll_iterations = self._reviews_scroll_iterations_min
        scroll_step = self._reviews_scroll_step

        scroll_container_script = """
        var containers = document.querySelectorAll('.scroll__container, [class*="scroll"], [class*="reviews"]');
        for (var i = 0; i < containers.length; i++) {
            var container = containers[i];
            if (container.scrollHeight > container.clientHeight && container.scrollHeight > 500) {
                return container;
            }
        }
        return null;
        """

        try:
            scroll_container = self.driver.execute_script(scroll_container_script)
            if scroll_container:
                for _ in range(max_scroll_iterations):
                    if self._is_stopped():
                        logger.info("Yandex reviews scroll: stop flag detected, breaking scroll loop")
                        break
                    try:
                        self.driver.execute_script(f"arguments[0].scrollTop += {scroll_step};", scroll_container)
                        time.sleep(0.3)
                        scroll_iterations += 1
                        
                        if scroll_iterations >= min_scroll_iterations:
                            current_height = self.driver.execute_script("return arguments[0].scrollHeight;", scroll_container)
                            scroll_top = self.driver.execute_script("return arguments[0].scrollTop;", scroll_container)
                            client_height = self.driver.execute_script("return arguments[0].clientHeight;", scroll_container)
                            
                            if scroll_top + client_height >= current_height - 10:
                                break
                        
                        if scroll_iterations >= max_scroll_iterations:
                            break
                    except Exception as scroll_error:
                        logger.warning(f"Error during scroll iteration: {scroll_error}")
                        break
        except Exception as e:
            logger.warning(f"Error scrolling reviews: {e}")

        page_source, soup_content = self._get_page_source_and_soup()
        
        if not reviews_url:
            current_url = self.driver.current_url if hasattr(self.driver, 'current_url') else ''
            if current_url and '/maps/org/' in current_url:
                reviews_url = current_url.rstrip('/') + '/reviews/'
            elif current_url and '/reviews/' in current_url:
                reviews_url = current_url
        
        pagination_links = soup_content.select('a[href*="/reviews/?page="], a[href*="/reviews?page="]')
        all_pages_urls = set()
        current_url = reviews_url if reviews_url else (self.driver.current_url if hasattr(self.driver, 'current_url') else '')
        for link in pagination_links:
            href = link.get('href', '')
            if href and 'page=' in href:
                if not href.startswith('http'):
                    href = urllib.parse.urljoin("https://yandex.ru", href)
                all_pages_urls.add(href)
        
        all_reviews = []
        pages_to_process = [current_url] if current_url and '/reviews/' in current_url else []
        if all_pages_urls:
            pages_to_process.extend(sorted(all_pages_urls)[:10])
        
        if not pages_to_process and current_url:
            pages_to_process = [current_url]
        
        seen_review_keys = set()
        
        for page_url in pages_to_process:
            if self._is_stopped():
                logger.info(f"Yandex reviews: stop flag detected before processing reviews page {page_url}, breaking pages loop")
                break
            try:
                if page_url != current_url:
                    logger.info(f"Processing reviews page: {page_url}")
                    self.driver.navigate(page_url)
                    time.sleep(2)

                # Перед парсингом отзывов разворачиваем все ответы организации,
                # чтобы в DOM появились блоки с текстом и датой ответа.
                try:
                    expand_script = r"""
                    var buttons = Array.from(document.querySelectorAll('button, a, span'))
                        .filter(el => el.textContent && el.textContent.indexOf('Посмотреть ответ организации') !== -1);
                    buttons.forEach(btn => { try { btn.click(); } catch(e) {} });
                    return buttons.length;
                    """
                    expanded_count = self.driver.execute_script(expand_script)
                    if expanded_count and expanded_count > 0:
                        time.sleep(2);
                except Exception as expand_err:
                    logger.warning(f"Could not expand Yandex answer blocks: {expand_err}")

                page_source, soup_content = self._get_page_source_and_soup()

                review_elements = soup_content.select('li, div.business-review-view, div.review-item-view')
                
                for review_elem in review_elements:
                    if self._is_stopped():
                        logger.info("Yandex reviews: stop flag detected inside reviews loop, breaking")
                        break
                    author_name = ""
                    author_elem = review_elem.select_one('a[href*="/user/"][class*="business-review-view__link"], a[href*="/user/"]')
                    if author_elem:
                        author_name = author_elem.get_text(strip=True)
                    
                    if not author_name:
                        author_name_elem = review_elem.select_one('[class*="author"], [class*="user"]')
                        if author_name_elem:
                            author_name = author_name_elem.get_text(strip=True)
                    
                    if not author_name:
                        all_text_elem = review_elem.get_text()
                        name_match = re.search(r'([А-ЯЁ][а-яё]+\s+[А-ЯЁ][а-яё]+|[А-ЯЁ][а-яё]+)', all_text_elem)
                        if name_match and len(name_match.group(1)) > 2:
                            author_name = name_match.group(1)
                    
                    if author_name == "Анонимный отзыв":
                        author_name = "Аноним"
                    
                    date_elem = None
                    date_selectors = [
                        'time[datetime]',
                        'time',
                        '[class*="date"]',
                        '[class*="time"]',
                    ]
                    for selector in date_selectors:
                        date_elem = review_elem.select_one(selector)
                        if date_elem:
                            break
                    
                    review_date = None
                    date_text = ""
                    if date_elem:
                        date_text = date_elem.get_text(strip=True)
                        datetime_attr = date_elem.get('datetime', '')
                        if datetime_attr:
                            try:
                                review_date = datetime.fromisoformat(datetime_attr.replace('Z', '+00:00'))
                            except:
                                review_date = parse_russian_date(date_text)
                        else:
                            review_date = parse_russian_date(date_text)
                    else:
                        all_text = review_elem.get_text()
                        date_match = re.search(r'(\d{1,2}\s+[а-яё]+\s+\d{4}|\d{1,2}\s+[а-яё]+)', all_text, re.IGNORECASE)
                        if date_match:
                            date_text = date_match.group(1)
                            review_date = parse_russian_date(date_text)
                    
                    rating_value = 0.0

                    # 1) Пытаемся достать рейтинг из явного текстового/aria-элемента
                    rating_elem = review_elem.select_one(
                        '[aria-label*="Оценка"], [aria-label*="оценка"], [class*="Оценка"], [class*="оценка"], [class*="rating"]'
                    )
                    if rating_elem:
                        rating_text = rating_elem.get_text(strip=True)
                        rating_match = re.search(
                            r'Оценка\s+(\d+)\s+Из\s+5|(\d+)\s+Из\s+5|(\d+)\s+из\s+5|(\d+(?:\.\d+)?)',
                            rating_text,
                            re.IGNORECASE,
                        )
                        if rating_match:
                            for grp in rating_match.groups():
                                if grp:
                                    rating_value = float(grp)
                                    break
                    
                    # 2) Пытаемся по количеству "активных" звёзд
                    if not rating_value:
                        stars = review_elem.select(
                            '[class*="star"][class*="active"], '
                            '[class*="star"][class*="fill"], '
                            '[class*="star"][class*="filled"], '
                            '[aria-label*="звезд"], [aria-label*="звезды"], '
                            '[data-rating], [data-score]'
                        )
                        if stars:
                            active_count = len(
                                [
                                    s
                                    for s in stars
                                    if 'active' in str(s.get('class', []))
                                    or 'fill' in str(s.get('class', []))
                                    or 'filled' in str(s.get('class', []))
                                    or 'full' in str(s.get('class', []))
                                ]
                            )
                            if active_count > 0:
                                rating_value = float(active_count)
                    
                    # 2.5) Пытаемся извлечь рейтинг из data-атрибутов
                    if not rating_value:
                        rating_attr = review_elem.get('data-rating') or review_elem.get('data-score')
                        if rating_attr:
                            try:
                                rating_value = float(rating_attr)
                                if 1 <= rating_value <= 5:
                                    pass  # Используем найденное значение
                                else:
                                    rating_value = 0.0
                            except (ValueError, TypeError):
                                pass
                    
                    # 2.6) Пытаемся найти заполненные SVG звезды (fill="#ffb81c" или похожие)
                    if not rating_value:
                        svg_stars = review_elem.select('svg[class*="star"], svg path[fill*="#ff"], svg path[fill*="#FF"]')
                        if svg_stars:
                            filled_count = 0
                            for svg in svg_stars:
                                # Проверяем fill атрибут в path внутри svg
                                paths = svg.select('path[fill]')
                                for path in paths:
                                    fill_attr = path.get('fill', '').lower()
                                    # Золотой/желтый цвет обычно означает заполненную звезду
                                    if '#ff' in fill_attr or '#fdb' in fill_attr or '#fc' in fill_attr:
                                        filled_count += 1
                                        break
                            if 1 <= filled_count <= 5:
                                rating_value = float(filled_count)

                    # 3) Фолбэк: ищем паттерн "N из 5" или "N/5" в общем тексте отзыва
                    if not rating_value:
                        all_text_for_rating = review_elem.get_text(separator=' ', strip=True)
                        m = re.search(r'\b([1-5])\s*(?:из|/)\s*5\b', all_text_for_rating, re.IGNORECASE)
                        if m:
                            rating_value = float(m.group(1))

                    # 4) Дополнительный фолбэк специально под текущую верстку Яндекс.Карт:
                    # во многих кейсах рейтинг хранится как одиночная цифра "1".."5"
                    # в "шапке" отзыва, до даты вида "10 ноября 2025".
                    #
                    # Чтобы не путать такие цифры с днями/годами, сначала отрезаем
                    # кусок текста ДО первой найденной даты, и уже в нем ищем
                    # одиночную цифру 1–5.
                    if not rating_value:
                        header_text = all_text_for_rating

                        try:
                            date_pattern = r'\d{1,2}\s+[а-яё]+\s+\d{4}'
                            date_match_for_cut = re.search(date_pattern, header_text, re.IGNORECASE)
                            if date_match_for_cut:
                                header_text = header_text[: date_match_for_cut.start()]
                        except Exception:
                            # В случае любой ошибки просто используем весь текст
                            header_text = all_text_for_rating

                        m_simple_digit = re.search(r'\b([1-5])\b', header_text)
                        if m_simple_digit:
                            try:
                                rating_value = float(m_simple_digit.group(1))
                            except Exception:
                                pass
                    
                    review_text_selectors = [
                        'div.business-review-view__body-text',
                        '.review-item-view__comment-text',
                        'div[class*="review-text"]',
                        'div[class*="comment-text"]',
                        'div[class*="body-text"]',
                        'p[class*="review-text"]',
                        'p[class*="comment"]',
                        'div[class*="text"][class*="review"]',
                        'div[class*="content"][class*="review"]',
                        'p[class*="review"]',
                        # намеренно НЕ используем голые [class*="text"]/content/comment,
                        # чтобы не цеплять служебные подписи типа "Подписаться"
                    ]
                    
                    review_text = ""
                    for text_selector in review_text_selectors:
                        text_elements = review_elem.select(text_selector)
                        for text_element in text_elements:
                            review_text = text_element.get_text(separator=' ', strip=True)
                            review_text = ' '.join(review_text.split())
                            if review_text and len(review_text) > 10:
                                break
                        if review_text and len(review_text) > 10:
                            break
                    
                    if not review_text or len(review_text) < 10:
                        all_text = review_elem.get_text(separator=' ', strip=True)
                        cleaned_text = re.sub(r'\d+[.,]\d+\s*(звезд|star|⭐)', '', all_text, flags=re.IGNORECASE)
                        cleaned_text = re.sub(r'\d{1,2}\s*(янв|фев|мар|апр|май|июн|июл|авг|сен|окт|ноя|дек)\s*\d{4}?', '', cleaned_text, flags=re.IGNORECASE)
                        cleaned_text = re.sub(r'\d{1,2}\s*(день|дня|дней|недел|недели|недель|месяц|месяца|месяцев|год|года|лет)\s*(назад)?', '', cleaned_text, flags=re.IGNORECASE)
                        cleaned_text = re.sub(r'Оцените это место', '', cleaned_text, flags=re.IGNORECASE)
                        cleaned_text = re.sub(r'Качество лечения.*?положительный', '', cleaned_text, flags=re.IGNORECASE)
                        cleaned_text = re.sub(r'Персонал.*?положительный', '', cleaned_text, flags=re.IGNORECASE)
                        cleaned_text = ' '.join(cleaned_text.split())
                        cleaned_text = cleaned_text.strip()
                        if len(cleaned_text) > 20:
                            review_text = cleaned_text  # Убрано ограничение в 1000 символов для полного текста отзыва

                    # Убираем чисто служебные тексты вида "Подписаться", "Полезно?" и т.п.
                    if review_text:
                        rt_lower = review_text.strip().lower()
                        service_texts = {
                            'подписаться',
                            'полезно?',
                            'полезно',
                            'подписаться полезно?',
                        }
                        if rt_lower in service_texts or ('подписаться' in rt_lower and len(rt_lower) <= 20):
                            review_text = ""

                    # Обрезаем префикс "Имя Знаток города N уровня Подписаться ДД месяц YYYY"
                    if review_text:
                        text = review_text.strip()
                        idx = text.lower().find('подписаться')
                        if idx != -1 and idx + len('подписаться') < len(text):
                            text = text[idx + len('подписаться'):].strip(' ,.-')
                        # Убираем ведущую дату "12 ноября 2025" или "12 ноября"
                        text = re.sub(r'^\d{1,2}\s+[А-Яа-яЁё]+\s+\d{4}\s*', '', text)
                        text = re.sub(r'^\d{1,2}\s+[А-Яа-яЁё]+\s*', '', text)
                        review_text = text.strip()
                    
                    answer_elem = review_elem.select_one('[class*="answer"], [class*="reply"], [class*="response"]')
                    has_response = False
                    response_text = ""
                    response_date = None
                    
                    if answer_elem or 'ответ организации' in review_elem.get_text().lower():
                        has_response = True
                        response_elem = review_elem.select_one('[class*="answer"], [class*="reply"], [class*="response"]')
                        if response_elem:
                            response_text_elem = response_elem.select_one('[class*="text"], [class*="content"]')
                            if response_text_elem:
                                response_text = response_text_elem.get_text(strip=True)
                            else:
                                response_text = response_elem.get_text(strip=True)
                            
                            response_date_elem = response_elem.select_one('[class*="date"], time')
                            if response_date_elem:
                                response_date_text = response_date_elem.get_text(strip=True)
                                response_date = parse_russian_date(response_date_text)
                    
                    review_key = f"{author_name}_{date_text}_{rating_value}_{hashlib.md5(review_text[:50].encode()).hexdigest()[:10]}"
                    if review_key in seen_review_keys:
                        continue
                    seen_review_keys.add(review_key)
                    
                    if review_text or rating_value > 0:
                        all_reviews.append({
                            'review_rating': rating_value,
                            'review_text': review_text or "",
                            'review_author': author_name or "Аноним",
                            'review_date': format_russian_date(review_date) if review_date else (date_text or ""),
                            'has_response': has_response,
                            'response_text': response_text,
                            'response_date': format_russian_date(response_date) if response_date else "",
                        })
                        
                        # Классификация: учитываем и звёзды, и смысл текста.
                        # Базовое правило: 1–2★ — негатив, 3★ — нейтрально, 4–5★ — позитив.
                        # Если звёзды явно противоречат тексту (по эвристике _estimate_sentiment),
                        # даём приоритет смыслу текста.
                        sentiment = self._estimate_sentiment(review_text or "")

                        if sentiment > 0 and rating_value <= 3:
                            # Низкие звёзды, но явный позитивный текст — считаем позитивом.
                            reviews_info['positive_reviews'] += 1
                        elif sentiment < 0 and rating_value >= 3:
                            # Высокие звёзды, но явно негативный текст — считаем негативом.
                            reviews_info['negative_reviews'] += 1
                        else:
                            # Обычный путь: классифицируем по звёздам.
                            if rating_value >= 4:
                                reviews_info['positive_reviews'] += 1
                            elif rating_value in (1, 2):
                                reviews_info['negative_reviews'] += 1
                            elif rating_value == 3:
                                reviews_info['neutral_reviews'] += 1
            except Exception as page_error:
                logger.warning(f"Error processing reviews page {page_url}: {page_error}", exc_info=True)
                continue
        
        reviews_info['details'] = all_reviews[:500]
        reviews_info['reviews_count'] = len(all_reviews) if all_reviews else reviews_count_total
        return reviews_info

    def _normalize_address(self, address: str) -> str:
        if not address:
            return ""
        
        address = address.strip()
        address = re.sub(r'\s+', ' ', address)
        
        return address

    def _normalize_card_name(self, name: str) -> str:
        """
        Нормализуем название карточки:
        - убираем лишние пробелы
        - отрезаем всё, что прилипло после первых цифр/символов рейтинга (например, "Апрель4,8Аптека" -> "Апрель")
        """
        if not name:
            return ""

        name = re.sub(r'\s+', ' ', name).strip()

        # Обрезаем по первой цифре или символу "•" (где часто начинается рейтинг/метки)
        m = re.match(r'^([^0-9•]+)', name)
        if m:
            base = m.group(1).strip()
        else:
            base = name

        # Дополнительно убираем в конце склеенное слово типа "Аптека", если оно прилипло без пробела
        base = re.sub(r'(Аптека|Клиника|До\s*\d{1,2}[:.]\d{2}|С\s*\d{1,2}[:.]\d{2})$', '', base, flags=re.IGNORECASE).strip()

        return base or name.strip()

    def _normalize_for_comparison(self, text: str) -> str:
        """
        Нормализует текст для сравнения:
        - приводит к нижнему регистру
        - убирает лишние пробелы
        - убирает ОПФ (ООО, ПАО, АО, ИП и т.д.) для более точного сравнения
        - убирает общие слова, которые могут мешать сравнению
        """
        if not text:
            return ""
        text = text.lower().strip()
        text = re.sub(r'\s+', ' ', text)
        # Убираем ОПФ
        opf_patterns = [
            r'^ооо\s+', r'^пао\s+', r'^ао\s+', r'^ип\s+', r'^зао\s+', r'^оао\s+',
            r'^чп\s+', r'^гк\s+', r'^ичп\s+'
        ]
        for pattern in opf_patterns:
            text = re.sub(pattern, '', text, flags=re.IGNORECASE)
        # Убираем общие слова, которые могут быть в разных компаниях
        # Например, "телеком" может быть в разных названиях
        common_words = ['телеком', 'телекоммуникации', 'связь', 'интернет', 'провайдер']
        words = text.split()
        words = [w for w in words if w not in common_words]
        text = ' '.join(words)
        return text.strip()

    def _calculate_name_similarity(self, card_name: str, search_name: str) -> float:
        """
        Вычисляет оценку схожести названия карточки с поисковым запросом.
        Возвращает значение от 0.0 до 1.0, где 1.0 - полное совпадение.
        """
        if not card_name or not search_name:
            return 0.0
        
        card_normalized = self._normalize_for_comparison(card_name)
        search_normalized = self._normalize_for_comparison(search_name)
        
        if not card_normalized or not search_normalized:
            return 0.0
        
        # Полное совпадение
        if card_normalized == search_normalized:
            return 1.0
        
        # Одно название содержит другое
        if search_normalized in card_normalized:
            # Если поисковый запрос полностью содержится в названии карточки
            return 0.9
        if card_normalized in search_normalized:
            # Если название карточки полностью содержится в поисковом запросе
            return 0.8
        
        # Проверяем совпадение по словам
        card_words = set(card_normalized.split())
        search_words = set(search_normalized.split())
        
        if not card_words or not search_words:
            return 0.0
        
        # Вычисляем долю совпадающих слов
        common_words = card_words & search_words
        if not common_words:
            return 0.0
        
        # Если все слова из поискового запроса есть в названии карточки
        if search_words.issubset(card_words):
            return 0.7
        
        # Частичное совпадение по словам
        # Используем более строгую метрику: доля совпадающих слов от минимального количества слов
        # Это даст большее значение для карточек, где больше слов совпадает
        similarity = len(common_words) / min(len(card_words), len(search_words))
        
        # Дополнительный бонус, если первое слово совпадает (важно для названий компаний)
        card_first_word = list(card_words)[0] if card_words else ""
        search_first_word = list(search_words)[0] if search_words else ""
        if card_first_word and search_first_word and card_first_word == search_first_word:
            similarity = min(1.0, similarity + 0.15)
        
        # Штраф, если в названии карточки есть слова, которых нет в поисковом запросе
        # (например, "телеком" в "Смарт Телеком" при поиске "Смарт Хоум")
        card_only_words = card_words - search_words
        search_only_words = search_words - card_words
        if card_only_words and search_only_words:
            # Если есть слова, которые есть только в карточке и только в запросе,
            # это указывает на разные компании
            penalty = min(0.3, len(card_only_words) * 0.1)
            similarity = max(0.0, similarity - penalty)
        
        return similarity

    def _filter_cards_by_name(self, cards: List[Dict[str, Any]], search_name: str) -> List[Dict[str, Any]]:
        """
        Фильтрует карточки по названию компании, оставляя только те, которые лучше всего совпадают.
        Если найдено несколько карточек с одинаковым высоким совпадением, возвращает все такие карточки.
        Если все совпадения низкие, возвращает карточку с наилучшим совпадением.
        """
        if not cards or not search_name:
            return cards
        
        # Вычисляем оценку схожести для каждой карточки
        cards_with_scores = []
        for card in cards:
            card_name = card.get('card_name', '')
            if not card_name:
                continue
            similarity = self._calculate_name_similarity(card_name, search_name)
            cards_with_scores.append((card, similarity, card_name))
            logger.debug(f"Card '{card_name}' similarity with '{search_name}': {similarity:.2f}")
        
        if not cards_with_scores:
            return cards
        
        # Сортируем по убыванию схожести
        cards_with_scores.sort(key=lambda x: x[1], reverse=True)
        
        best_score = cards_with_scores[0][1]
        best_card_name = cards_with_scores[0][2]
        logger.info(f"Best name similarity score: {best_score:.2f} for card '{best_card_name}' (search: '{search_name}')")
        
        # Если есть несколько карточек, выводим топ-3 для отладки
        if len(cards_with_scores) > 1:
            logger.info(f"Top 3 cards by similarity:")
            for i, (card, score, name) in enumerate(cards_with_scores[:3], 1):
                logger.info(f"  {i}. '{name}' - {score:.2f}")
        
        # Если лучшее совпадение достаточно хорошее (>= 0.6), оставляем только карточки с таким же или близким совпадением
        # Повысили порог с 0.5 до 0.6 для более строгой фильтрации
        if best_score >= 0.6:
            # Оставляем карточки с совпадением >= 0.6 или в пределах 0.15 от лучшего
            threshold = max(0.6, best_score - 0.15)
            filtered = [card for card, score, name in cards_with_scores if score >= threshold]
            logger.info(f"Filtered to {len(filtered)} card(s) with similarity >= {threshold:.2f}")
            return filtered
        else:
            # Если все совпадения низкие, возвращаем только лучшую карточку
            logger.warning(f"Low name similarity scores (best: {best_score:.2f}). Returning only the best matching card '{best_card_name}'.")
            return [cards_with_scores[0][0]]

    def _scroll_to_load_all_cards(
        self,
        max_scrolls: Optional[int] = None,
        scroll_step: Optional[int] = None,
        max_cards_to_fetch: Optional[int] = None,
        max_no_change_scrolls: int = 5,
    ) -> int:
        logger.info("Starting scroll to load all cards on Yandex search page")
        
        scroll_iterations = 0
        max_card_count = 0
        
        if max_scrolls is None:
            max_scrolls = self._scroll_max_iter
        if scroll_step is None:
            scroll_step = self._scroll_step
        if max_cards_to_fetch is None:
            max_cards_to_fetch = self._max_records
        
        logger.info(
            f"Scroll parameters: Max iterations={max_scrolls}, "
            f"Scroll step={scroll_step}px, Wait time={self._scroll_wait_time}s, "
            f"Target cards={max_cards_to_fetch}, Max no-change={max_no_change_scrolls}"
        )
        
        scrollable_element_selector = None
        try:
            selector_json = json.dumps(self._scrollable_element_selector)
            find_scrollable_script = f"""
            var selectorStr = {selector_json};
            var selectors = selectorStr.split(',').map(s => s.trim());
            for (var i = 0; i < selectors.length; i++) {{
                var els = document.querySelectorAll(selectors[i]);
                for (var j = 0; j < els.length; j++) {{
                    var el = els[j];
                    if (el && el.scrollHeight > el.clientHeight && el.scrollHeight > 500) {{
                        return {{
                            'selector': selectors[i],
                            'scrollHeight': el.scrollHeight,
                            'clientHeight': el.clientHeight
                        }};
                    }}
                }}
            }}
            return null;
            """
            scrollable_info = self.driver.execute_script(find_scrollable_script)
            if scrollable_info and isinstance(scrollable_info, dict):
                scrollable_element_selector = scrollable_info.get('selector')
                logger.info(f"Found scrollable element: {scrollable_element_selector}")
        except Exception as e:
            logger.warning(f"Error finding scrollable element: {e}")
        
        last_card_count = 0
        stable_count = 0
        
        while scroll_iterations < max_scrolls:
            if self._is_stopped():
                logger.info("Yandex scroll: stop flag detected, breaking scroll loop")
                break
            try:
                page_source, soup = self._get_page_source_and_soup()
                
                current_card_count = 0
                for selector in self._card_selectors:
                    found = soup.select(selector)
                    current_card_count = max(current_card_count, len(found))
                
                if scrollable_element_selector:
                    escaped_selector = json.dumps(scrollable_element_selector)
                    scroll_info_script = f"""
                    var selector = {escaped_selector};
                    var container = document.querySelector(selector);
                    if (container) {{
                        var oldScrollTop = container.scrollTop;
                        var oldScrollHeight = container.scrollHeight;
                        var oldClientHeight = container.clientHeight;
                        container.scrollTop = container.scrollHeight;
                        var newScrollTop = container.scrollTop;
                        var newScrollHeight = container.scrollHeight;
                        var newClientHeight = container.clientHeight;
                        var isAtBottom = newScrollTop + newClientHeight >= newScrollHeight - 10;
                        return {{
                            'oldScrollTop': oldScrollTop,
                            'oldScrollHeight': oldScrollHeight,
                            'oldClientHeight': oldClientHeight,
                            'newScrollTop': newScrollTop,
                            'newScrollHeight': newScrollHeight,
                            'newClientHeight': newClientHeight,
                            'isAtBottom': isAtBottom,
                            'hasGrown': newScrollHeight > oldScrollHeight
                        }};
                    }}
                    return {{'error': 'Container not found'}};
                    """
                    scroll_info = self.driver.execute_script(scroll_info_script)
                    
                    if scroll_info and isinstance(scroll_info, dict):
                        if scroll_info.get('error'):
                            logger.warning(f"Scroll container error: {scroll_info.get('error')}")
                            break
                        
                        current_scroll_height = scroll_info.get('newScrollHeight', 0)
                        has_grown = scroll_info.get('hasGrown', False)
                        
                        if current_card_count > last_card_count or has_grown:
                            last_card_count = current_card_count
                            stable_count = 0
                            max_card_count = max(max_card_count, current_card_count)
                            logger.info(f"Cards found: {current_card_count}, scroll height: {current_scroll_height}px (iteration {scroll_iterations + 1})")
                        else:
                            stable_count += 1
                            if stable_count >= max_no_change_scrolls:
                                logger.info(
                                    f"Card count stable for {stable_count} iterations. "
                                    f"Stopping scroll (no new cards)."
                                )
                                break
                            
                        if scroll_info.get('isAtBottom') and not has_grown:
                            logger.info("Reached bottom of scrollable container without growth, stopping.")
                            break
                else:
                    scroll_info_script = """
                    var oldScrollTop = window.pageYOffset || document.documentElement.scrollTop || 0;
                    var oldScrollHeight = Math.max(document.body.scrollHeight, document.documentElement.scrollHeight);
                    window.scrollTo(0, document.body.scrollHeight);
                    var newScrollTop = window.pageYOffset || document.documentElement.scrollTop || 0;
                    var newScrollHeight = Math.max(document.body.scrollHeight, document.documentElement.scrollHeight);
                    var isAtBottom = newScrollTop + window.innerHeight >= newScrollHeight - 10;
                    return {
                        'oldScrollTop': oldScrollTop,
                        'oldScrollHeight': oldScrollHeight,
                        'newScrollTop': newScrollTop,
                        'newScrollHeight': newScrollHeight,
                        'isAtBottom': isAtBottom,
                        'hasGrown': newScrollHeight > oldScrollHeight
                    };
                    """
                    scroll_info = self.driver.execute_script(scroll_info_script)
                    
                    if scroll_info and isinstance(scroll_info, dict):
                        current_scroll_height = scroll_info.get('newScrollHeight', 0)
                        has_grown = scroll_info.get('hasGrown', False)
                        
                        if current_card_count > last_card_count or has_grown:
                            last_card_count = current_card_count
                            stable_count = 0
                            max_card_count = max(max_card_count, current_card_count)
                            logger.info(f"Cards found: {current_card_count}, scroll height: {current_scroll_height}px (iteration {scroll_iterations + 1})")
                        else:
                            stable_count += 1
                            if stable_count >= max_no_change_scrolls:
                                logger.info(
                                    f"Card count stable for {stable_count} iterations. "
                                    f"Stopping scroll (no new cards)."
                                )
                                break
                        
                        if scroll_info.get('isAtBottom') and not has_grown:
                            logger.info("Reached bottom of page without growth, stopping.")
                            break
                
                # даём время Яндексу подгрузить новые карточки
                time.sleep(self._scroll_wait_time)
                scroll_iterations += 1

                # если достигли целевого количества карточек — выходим
                if max_card_count >= max_cards_to_fetch:
                    logger.info(
                        f"Reached target card count ({max_cards_to_fetch}). "
                        f"Stopping scroll after {scroll_iterations} iterations."
                    )
                    break
                
            except Exception as e:
                logger.error(f"Error during scroll iteration {scroll_iterations + 1}: {e}")
                break
        
        logger.info(f"Scroll completed: {scroll_iterations} iterations, found {max_card_count} cards")
        return max_card_count

    def _parse_cards(self, search_query_url: str) -> List[Dict[str, Any]]:
        self._collected_card_data.clear()
        self._aggregated_data = {
            'total_cards': 0,
            'total_rating_sum': 0.0,
            'total_reviews_count': 0,
            'total_positive_reviews': 0,
            'total_negative_reviews': 0,
            'total_neutral_reviews': 0,
            'total_answered_count': 0,
            'total_answered_reviews_count': 0,
            'total_unanswered_reviews_count': 0,
            'total_response_time_sum_days': 0.0,
            'total_response_time_calculated_count': 0,
        }

        try:
            self.driver.navigate(search_query_url)
            time.sleep(3)
            self.check_captcha()
            
            self._update_progress("Поиск карточек...")
            
            page_source, soup = self._get_page_source_and_soup()
            
            # Пагинация: на практике Яндекс сейчас отдаёт ссылки вида
            # https://yandex.ru/maps/.../chain/.../?page=2
            # поэтому нельзя жёстко фильтровать только по "/search/".
            pagination_links = soup.select('a[href*="page="]')
            all_pages_urls = set()
            for link in pagination_links:
                href = link.get('href', '')
                if not href or 'page=' not in href:
                    continue

                # Отбрасываем заведомо нерелевантные ссылки (якоря и т.п.)
                if href.startswith('#'):
                    continue

                if not href.startswith('http'):
                    href = urllib.parse.urljoin("https://yandex.ru", href)

                all_pages_urls.add(href)
            
            all_card_urls = set()
            pages_to_process = [search_query_url]
            if all_pages_urls:
                pages_to_process.extend(sorted(all_pages_urls)[:20])
            
            for page_num, page_url in enumerate(pages_to_process, start=1):
                if self._is_stopped():
                    logger.info(f"Yandex cards: stop flag detected before processing search page {page_num}, breaking pages loop")
                    break
                try:
                    if page_url != search_query_url:
                        logger.info(f"Processing search page {page_num}/{len(pages_to_process)}: {page_url}")
                        self._update_progress(f"Поиск карточек: обработка страницы {page_num}/{len(pages_to_process)}, найдено {len(all_card_urls)} карточек")
                        self.driver.navigate(page_url)
                        time.sleep(3)
                        self.check_captcha()
                        page_source, soup = self._get_page_source_and_soup()
                    
                    initial_card_count = len(all_card_urls)
                    logger.info(f"Initial card count on page {page_num}: {initial_card_count}")
                    
                    self._update_progress(f"Поиск карточек: прокрутка страницы {page_num} для загрузки всех карточек...")
                    # Дозированная прокрутка: имитируем поведение пользователя
                    # Цель — попытаться получить до self._max_records карточек (ограничение из настроек).
                    target_cards = self._max_records
                    cards_count_after_scroll = self._scroll_to_load_all_cards(
                        max_scrolls=self._scroll_max_iter,
                        scroll_step=self._scroll_step,
                        max_cards_to_fetch=target_cards,
                        max_no_change_scrolls=5,
                    )
                    logger.info(f"Scroll completed for page {page_num}. Found {cards_count_after_scroll} cards after scrolling.")
                    time.sleep(3)
                    
                    page_source, soup = self._get_page_source_and_soup()
                    
                    for selector in self._card_selectors:
                        elements = soup.select(selector)
                        for elem in elements:
                            href = elem.get('href', '')
                            if href and ('/maps/org/' in href or '/org/' in href):
                                if '/gallery/' in href:
                                    continue
                                if not href.startswith('http'):
                                    href = urllib.parse.urljoin("https://yandex.ru", href)
                                all_card_urls.add(href)
                    
                    new_cards = len(all_card_urls) - initial_card_count
                    logger.info(f"Found {new_cards} new cards on page {page_num}. Total: {len(all_card_urls)}")
                    
                    if len(all_card_urls) >= self._max_records:
                        logger.info(f"Reached max records limit ({self._max_records}). Stopping pagination.")
                        break
                except Exception as page_error:
                    logger.warning(f"Error processing search page {page_url}: {page_error}", exc_info=True)
                    continue
            
            if not all_card_urls:
                logger.warning("No card URLs found on any page")
                return []
            
            logger.info(f"Found {len(all_card_urls)} unique card URLs from {len(pages_to_process)} pages")
            
            # Собираем все карточки с данными
            all_cards_data = []
            for idx, card_url in enumerate(list(all_card_urls)[:self._max_records]):
                if self._is_stopped():
                    logger.info(f"Yandex cards: stop flag detected before processing card index {idx}, breaking cards loop")
                    break
                if len(all_cards_data) >= self._max_records:
                    break
                
                try:
                    self._update_progress(f"Сканирование карточек: {idx + 1}/{min(len(all_card_urls), self._max_records)}")
                    self.driver.navigate(card_url)
                    time.sleep(2)
                    self.check_captcha()
                    
                    page_source, card_soup = self._get_page_source_and_soup()
                    card_data = self._extract_card_data_from_detail_page(card_soup)
                    
                    if card_data and card_data.get('card_name'):
                        all_cards_data.append(card_data)
                except Exception as e:
                    logger.error(f"Error processing card {card_url}: {e}")
                    continue
            
            # Фильтруем карточки по названию компании, оставляя только лучшую
            if all_cards_data and self._search_query_name:
                filtered_cards = self._filter_cards_by_name(all_cards_data, self._search_query_name)
                logger.info(f"Filtered {len(all_cards_data)} cards to {len(filtered_cards)} card(s) matching company name '{self._search_query_name}'")
                all_cards_data = filtered_cards
            
            # Добавляем отфильтрованные карточки в коллекцию и обновляем агрегацию
            for card_data in all_cards_data:
                self._collected_card_data.append(card_data)
                self._update_aggregated_data(card_data)
            
            logger.info(f"Parsed {len(self._collected_card_data)} cards")
            return self._collected_card_data
            
        except Exception as e:
            logger.error(f"Error in _parse_cards: {e}", exc_info=True)
            return self._collected_card_data

    def parse(self, url: str) -> Dict[str, Any]:
        self._update_progress("Инициализация парсера Yandex...")
        self._url = url
        parsed_url = urllib.parse.urlparse(url)
        query_params = urllib.parse.parse_qs(parsed_url.query)

        search_text_param = query_params.get('text')
        if search_text_param:
            search_text_value = search_text_param[0]
            if ',' in search_text_value:
                parts = search_text_value.split(',', 1)
                self._search_query_name = parts[1].strip()
            else:
                self._search_query_name = search_text_value
        else:
            self._search_query_name = "YandexMapsSearch"

        logger.info(f"Starting Yandex Parser for URL: {url}. Search query name extracted as: {self._search_query_name}")

        try:
            collected_cards_data = self._parse_cards(url)
            logger.info(f"_parse_cards returned {len(collected_cards_data)} cards")
        except Exception as e:
            logger.error(f"Error in _parse_cards: {e}", exc_info=True)
            collected_cards_data = []

        total_cards = len(collected_cards_data)
        aggregated_rating = 0.0
        # Считаем рейтинг только по карточкам, у которых есть рейтинг
        cards_with_rating = sum(1 for card in collected_cards_data if card.get('card_rating') and str(card.get('card_rating', '')).strip())
        if cards_with_rating > 0 and self._aggregated_data['total_rating_sum'] > 0:
            aggregated_rating = round(self._aggregated_data['total_rating_sum'] / cards_with_rating, 2)
        elif total_cards > 0 and self._aggregated_data['total_rating_sum'] > 0:
            # Если нет карточек с рейтингом, но есть сумма рейтингов, используем общее количество карточек
            aggregated_rating = round(self._aggregated_data['total_rating_sum'] / total_cards, 2)

        aggregated_avg_response_time = 0.0
        if self._aggregated_data['total_response_time_calculated_count'] > 0:
            aggregated_avg_response_time = round(
                self._aggregated_data['total_response_time_sum_days'] / self._aggregated_data['total_response_time_calculated_count'],
                2
            )

        aggregated_answered_reviews_percent = 0.0
        if self._aggregated_data['total_reviews_count'] > 0:
            aggregated_answered_reviews_percent = round(
                (self._aggregated_data['total_answered_reviews_count'] / self._aggregated_data['total_reviews_count']) * 100,
                2
            )

        aggregated_info = {
            'search_query_name': self._search_query_name,
            'total_cards_found': total_cards,
            'aggregated_rating': aggregated_rating,
            'aggregated_reviews_count': self._aggregated_data['total_reviews_count'],
            'aggregated_positive_reviews': self._aggregated_data['total_positive_reviews'],
            'aggregated_negative_reviews': self._aggregated_data['total_negative_reviews'],
            'aggregated_neutral_reviews': self._aggregated_data['total_neutral_reviews'],
            'aggregated_answered_reviews_count': self._aggregated_data['total_answered_reviews_count'],
            'aggregated_answered_reviews_percent': aggregated_answered_reviews_percent,
            'aggregated_unanswered_reviews_count': self._aggregated_data['total_unanswered_reviews_count'],
            'aggregated_avg_response_time': aggregated_avg_response_time,
        }

        self._update_progress(f"Агрегация результатов: найдено {total_cards} карточек")

        return {
            'cards_data': collected_cards_data,
            'aggregated_info': aggregated_info
        }