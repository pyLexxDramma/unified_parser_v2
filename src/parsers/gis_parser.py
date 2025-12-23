from __future__ import annotations
import json
import re
import logging
import time
import urllib.parse
import hashlib
import os
import base64
from typing import Any, Dict, List, Optional, Tuple
import datetime as dt_module
from datetime import timedelta
from bs4 import BeautifulSoup, Tag

from src.drivers.base_driver import BaseDriver
from src.config.settings import Settings
from src.parsers.base_parser import BaseParser
from src.parsers.date_parser import parse_russian_date, format_russian_date

logger = logging.getLogger(__name__)


class GisParser(BaseParser):
    def __init__(self, driver: BaseDriver, settings: Settings):
        super().__init__(driver, settings)
        self._url: str = ""
        self._target_website: Optional[str] = None  # Целевой сайт для фильтрации
        self._target_address: Optional[str] = None  # Целевой адрес для фильтрации

        self._scroll_step: int = getattr(self._settings.parser, 'gis_scroll_step', 500)
        self._scroll_max_iter: int = getattr(self._settings.parser, 'gis_scroll_max_iter', 100)
        self._scroll_wait_time: float = getattr(self._settings.parser, 'gis_scroll_wait_time', 0.5)
        self._reviews_scroll_step: int = getattr(self._settings.parser, 'gis_reviews_scroll_step', 500)
        self._reviews_scroll_iterations_max: int = getattr(self._settings.parser, 'gis_reviews_scroll_max_iter', 200)  # Увеличено для загрузки всех отзывов
        self._reviews_scroll_iterations_min: int = getattr(self._settings.parser, 'gis_reviews_scroll_min_iter', 30)
        self._max_records: int = getattr(self._settings.parser, 'max_records', 1000)

        self._card_selectors: List[str] = getattr(self._settings.parser, 'gis_card_selectors', [
            'a[href*="/firm/"]',
            'a[href*="/station/"]',
            'link[href*="/firm/"]',
        ])
        self._pagination_selectors: List[str] = getattr(self._settings.parser, 'gis_pagination_selectors', [
            'a[href*="/search/"][href*="page="]',
            'a[href*="page="]',
        ])

        self._scrollable_element_selector: str = getattr(self._settings.parser, 'gis_scroll_container', 
                                                       '[class*="_1rkbbi0x"], [class*="scroll"], [class*="list"], [class*="results"]')

        # Агрегированные данные по карточкам и отзывам (как в YandexParser)
        self._aggregated_data: Dict[str, Any] = {
            'total_cards': 0,
            'total_rating_sum': 0.0,
            'total_reviews_count': 0,
            'total_positive_reviews': 0,
            'total_negative_reviews': 0,
            'total_answered_count': 0,
            'total_answered_reviews_count': 0,
            'total_unanswered_reviews_count': 0,
            'total_response_time_sum_days': 0.0,
            'total_response_time_calculated_count': 0,
        }

    def get_url_pattern(self) -> str:
        return r"https://2gis\.ru/.*"

    def _add_xhr_counter_script(self) -> str:
        xhr_script = r'''
            (function() {
                var oldOpen = XMLHttpRequest.prototype.open;
                XMLHttpRequest.prototype.open = function(method, url, async, user, pass) {
                    if (url.match(/^https?\:\/\/[^\/]*2gis\.[a-z]+/i)) {
                        if (window.openHTTPs === undefined) {
                            window.openHTTPs = 1;
                        } else {
                            window.openHTTPs++;
                        }
                    }
                    oldOpen.call(this, method, url, async, user, pass);
                }
            })();
        '''
        return xhr_script

    def _get_page_source_and_soup(self) -> Tuple[str, BeautifulSoup]:
        page_source = self.driver.get_page_source()
        soup = BeautifulSoup(page_source, "lxml")
        return page_source, soup

    def _normalize_address(self, address: str) -> str:
        if not address:
            return ""
        address = address.strip()
        address = re.sub(r'\s+', ' ', address)
        return address

    def _scroll_to_load_all_cards(self, max_scrolls: Optional[int] = None, scroll_step: Optional[int] = None) -> int:
        logger.info("Starting scroll to load all cards on 2GIS search page")
        
        last_card_count = 0
        last_scroll_height = 0
        stable_count = 0
        max_stable_iterations = 5
        scroll_iterations = 0
        max_card_count = 0
        
        if max_scrolls is None:
            max_scrolls = self._scroll_max_iter
        if scroll_step is None:
            scroll_step = self._scroll_step
        
        logger.info(f"Scroll parameters: Max iterations={max_scrolls}, Wait time={self._scroll_wait_time}s")
        
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
                        var cardsInside = el.querySelectorAll('a[href*="/firm/"], a[href*="/station/"]');
                        if (cardsInside.length > 0) {{
                            return {{
                                'selector': selectors[i],
                                'scrollHeight': el.scrollHeight,
                                'clientHeight': el.clientHeight,
                                'cardsInside': cardsInside.length
                            }};
                        }}
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
        
        while scroll_iterations < max_scrolls:
            if self._is_stopped():
                logger.info("2GIS scroll: stop flag detected, breaking scroll loop")
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
                        container.scrollTop = container.scrollHeight;
                        var newScrollTop = container.scrollTop;
                        var newScrollHeight = container.scrollHeight;
                        var isAtBottom = newScrollTop + container.clientHeight >= newScrollHeight - 10;
                        return {{
                            'oldScrollHeight': oldScrollHeight,
                            'newScrollHeight': newScrollHeight,
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
                            last_scroll_height = current_scroll_height
                            stable_count = 0
                            max_card_count = max(max_card_count, current_card_count)
                            logger.info(f"Cards found: {current_card_count}, scroll height: {current_scroll_height}px (iteration {scroll_iterations + 1})")
                        else:
                            stable_count += 1
                            if stable_count >= max_stable_iterations:
                                logger.info(f"Scroll height and card count stable for {stable_count} iterations. Reached bottom.")
                                break
                            
                        if scroll_info.get('isAtBottom') and not has_grown:
                            time.sleep(2)
                            scroll_info = self.driver.execute_script(scroll_info_script)
                            if scroll_info and scroll_info.get('newScrollHeight') == last_scroll_height:
                                logger.info("Confirmed at bottom of scrollable container")
                                break
                else:
                    scroll_info_script = """
                    var oldScrollHeight = Math.max(document.body.scrollHeight, document.documentElement.scrollHeight);
                    window.scrollTo(0, document.body.scrollHeight);
                    var newScrollHeight = Math.max(document.body.scrollHeight, document.documentElement.scrollHeight);
                    var newScrollTop = window.pageYOffset || document.documentElement.scrollTop || 0;
                    var isAtBottom = newScrollTop + window.innerHeight >= newScrollHeight - 10;
                    return {
                        'oldScrollHeight': oldScrollHeight,
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
                            last_scroll_height = current_scroll_height
                            stable_count = 0
                            max_card_count = max(max_card_count, current_card_count)
                            logger.info(f"Cards found: {current_card_count}, scroll height: {current_scroll_height}px (iteration {scroll_iterations + 1})")
                        else:
                            stable_count += 1
                            if stable_count >= max_stable_iterations:
                                logger.info(f"Scroll height and card count stable for {stable_count} iterations. Reached bottom.")
                                break
                        
                        if scroll_info.get('isAtBottom') and not has_grown:
                            time.sleep(2)
                            scroll_info = self.driver.execute_script(scroll_info_script)
                            if scroll_info and scroll_info.get('newScrollHeight') == last_scroll_height:
                                logger.info("Confirmed at bottom of page")
                                break
                
                time.sleep(self._scroll_wait_time)
                scroll_iterations += 1
                
            except Exception as e:
                logger.error(f"Error during scroll iteration {scroll_iterations + 1}: {e}", exc_info=True)
                break
        
        logger.info(f"Scroll completed: {scroll_iterations} iterations, found {max_card_count} cards")
        return max_card_count

    def _get_links(self) -> List[str]:
        try:
            page_source, soup = self._get_page_source_and_soup()
            valid_urls = set()
            
            def _normalize_firm_station_url(url: str) -> str:
                """
                Приводим URL карточки 2ГИС к каноническому виду:
                https://2gis.ru/{city}/{firm|station}/{id}
                Убираем /search/..., координаты и query-параметры.
                """
                if not url:
                    return url

                # Убираем query-параметры
                base = url.split('?', 1)[0]

                # Пытаемся вытащить host, город и ID организации
                m = re.search(r'^(https?://[^/]+)/([^/]+)/.*?(firm|station)/(\d+)', base)
                if m:
                    host, city, kind, ident = m.groups()
                    return f"{host}/{city}/{kind}/{ident}"

                return base

            card_links = soup.select('a[href*="/firm/"], a[href*="/station/"]')
            logger.info(f"Found {len(card_links)} links with /firm/ or /station/ in href")
            
            for link in card_links:
                href = link.get('href', '')
                if href:
                    if not href.startswith('http'):
                        href = urllib.parse.urljoin("https://2gis.ru", href)
                    
                    if re.match(r'.*/(firm|station)/\d+', href):
                        normalized_url = _normalize_firm_station_url(href)
                        valid_urls.add(normalized_url)
            
            logger.info(f"Method found {len(valid_urls)} unique card URLs")
            
            cards_on_page = []
            seen_ids = set()
            for selector in self._card_selectors:
                found = soup.select(selector)
                for card in found:
                    card_id = id(card)
                    if card_id not in seen_ids:
                        seen_ids.add(card_id)
                        cards_on_page.append(card)
                        
                        link_elem = card.select_one('a[href*="/firm/"], a[href*="/station/"]')
                        if link_elem:
                            href = link_elem.get('href', '')
                            if href:
                                if not href.startswith('http'):
                                    href = urllib.parse.urljoin("https://2gis.ru", href)
                                if re.match(r'.*/(firm|station)/\d+', href):
                                    normalized_url = _normalize_firm_station_url(href)
                                    valid_urls.add(normalized_url)
            
            logger.info(f"Total found {len(valid_urls)} unique card URLs")
            return list(valid_urls)
            
        except Exception as e:
            logger.error(f"Error getting links: {e}", exc_info=True)
            return []

    def _get_pagination_links(self, soup: BeautifulSoup, current_url: str) -> List[str]:
        """
        Возвращает список URL страниц пагинации для результатов поиска 2GIS.
        Ищем ссылки вида /search/.../page/N, при этом отбрасываем ссылки на сами карточки фирм/станций.
        """
        pagination_urls: List[str] = []
        try:
            page_links = soup.select('a[href*="/page/"]')
            logger.debug(f"Found {len(page_links)} links with /page/ in href")

            for link in page_links:
                href = link.get('href', '')
                if not href:
                    continue

                if '/page/' not in href:
                    continue

                # Игнорируем ссылки на карточки
                if '/firm/' in href or '/station/' in href:
                    continue

                if not href.startswith('http'):
                    href = urllib.parse.urljoin("https://2gis.ru", href)

                if '/search/' in href and href not in pagination_urls:
                    pagination_urls.append(href)

            def extract_page_number(url: str) -> int:
                match = re.search(r'/page/(\d+)', url)
                if match:
                    return int(match.group(1))
                return 0

            pagination_urls = sorted(set(pagination_urls), key=extract_page_number)
            logger.info(
                f"✓ Found {len(pagination_urls)} unique pagination pages: "
                f"{[extract_page_number(u) for u in pagination_urls]}"
            )

        except Exception as e:
            logger.error(f"Error getting pagination links: {e}", exc_info=True)

        return pagination_urls

    def _wait_requests_finished(self, timeout: int = 10) -> bool:
        try:
            wait_script = """
            if (window.XMLHttpRequest) {
                return window.XMLHttpRequest.prototype.open && window.XHR_COUNTER !== undefined && window.XHR_COUNTER.active === 0;
            }
            return true;
            """
            start_time = time.time()
            while time.time() - start_time < timeout:
                requests_finished = self.driver.execute_script(wait_script)
                if requests_finished:
                    time.sleep(0.5)
                    requests_finished = self.driver.execute_script(wait_script)
                    if requests_finished:
                        return True
                time.sleep(0.5)
            return False
        except Exception as e:
            logger.warning(f"Error waiting for requests: {e}")
            return False

    def _find_and_click_pagination_button(self) -> bool:
        try:
            page_source, soup = self._get_page_source_and_soup()
            
            next_page_button_selectors = [
                'a[href*="/page/"]',
                'button[aria-label*="Следующ"]',
                'button[aria-label*="Next"]',
                'a[class*="next"]',
                'button[class*="next"]',
            ]
            
            for selector in next_page_button_selectors:
                try:
                    buttons = soup.select(selector)
                    for button in buttons:
                        href = button.get('href', '')
                        button_classes = ' '.join(button.get('class', []))
                        
                        if 'disabled' in button_classes.lower() or 'current' in button_classes.lower() or 'active' in button_classes.lower():
                            continue
                        
                        if href and '/page/' in href and '/search/' in href:
                            if not href.startswith('http'):
                                href = urllib.parse.urljoin("https://2gis.ru", href)
                            
                            logger.info(f"Found pagination button with URL: {href}")
                            
                            try:
                                escaped_href = json.dumps(href)
                                click_script = f"""
                                var href = {escaped_href};
                                var button = document.querySelector('a[href="' + href + '"]');
                                if (button && !button.classList.contains('disabled')) {{
                                    button.scrollIntoView({{behavior: 'smooth', block: 'center'}});
                                    setTimeout(function() {{
                                        button.click();
                                    }}, 500);
                                    return true;
                                }}
                                return false;
                                """
                                time.sleep(1)
                                clicked = self.driver.execute_script(click_script)
                                time.sleep(2)
                                
                                if clicked:
                                    logger.info(f"Successfully clicked pagination button to: {href}")
                                    self._wait_requests_finished()
                                    return True
                                else:
                                    logger.warning(f"Could not click button via script, trying navigate")
                                    self.driver.navigate(href)
                                    time.sleep(3)
                                    self._wait_requests_finished()
                                    return True
                            except Exception as click_error:
                                logger.warning(f"Error clicking pagination button: {click_error}, trying navigate to URL")
                                self.driver.navigate(href)
                                time.sleep(3)
                                self._wait_requests_finished()
                                return True
                except Exception as select_error:
                    continue
            
            logger.debug("No pagination button found")
            return False
        except Exception as e:
            logger.warning(f"Error finding pagination button: {e}")
            return False

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
            return 0.9
        if card_normalized in search_normalized:
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
        similarity = len(common_words) / min(len(card_words), len(search_words))
        
        # Дополнительный бонус, если первое слово совпадает
        card_first_word = list(card_words)[0] if card_words else ""
        search_first_word = list(search_words)[0] if search_words else ""
        if card_first_word and search_first_word and card_first_word == search_first_word:
            similarity = min(1.0, similarity + 0.15)
        
        # Штраф, если в названии карточки есть слова, которых нет в поисковом запросе
        card_only_words = card_words - search_words
        search_only_words = search_words - card_words
        if card_only_words and search_only_words:
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
            logger.debug(f"2GIS card '{card_name}' similarity with '{search_name}': {similarity:.2f}")
        
        if not cards_with_scores:
            return cards
        
        # Сортируем по убыванию схожести
        cards_with_scores.sort(key=lambda x: x[1], reverse=True)
        
        best_score = cards_with_scores[0][1]
        best_card_name = cards_with_scores[0][2]
        logger.info(f"2GIS best name similarity score: {best_score:.2f} for card '{best_card_name}' (search: '{search_name}')")
        
        # Оставляем карточки с оценкой >= 0.6 или в пределах 0.1 от лучшей оценки
        threshold = max(0.6, best_score - 0.1)
        filtered = [card for card, score, _ in cards_with_scores if score >= threshold]
        
        if not filtered:
            # Если ничего не прошло порог, возвращаем хотя бы лучшую карточку
            filtered = [cards_with_scores[0][0]]
        
        return filtered

    def _get_card_reviews_info_2gis(self, card_url: str) -> Dict[str, Any]:
        """
        Получение информации об отзывах по карточке 2GIS.
        Возвращает количество отзывов, распределение по рейтингу, ответы и детальные данные.
        """
        reviews_info: Dict[str, Any] = {
            'reviews_count': 0,
            'ratings_count': 0,  # Количество оценок из структуры страницы
            'positive_reviews': 0,
            'negative_reviews': 0,
            'neutral_reviews': 0,  # Нейтральные отзывы (3⭐)
            'answered_count': 0,
            'unanswered_count': 0,
            'texts': [],
            'details': [],
        }

        try:
            current_url = self.driver.current_url if hasattr(self.driver, 'current_url') else card_url

            # Нормализуем URL карточки к виду https://2gis.ru/{city}/{firm|station}/{id}/tab/reviews
            base_url = current_url or card_url
            base_url = base_url.split('#', 1)[0]

            m = re.search(r'^(https?://[^/]+)/([^/]+)/.*?(firm|station)/(\d+)', base_url)
            if m:
                host, city, kind, ident = m.groups()
                reviews_url = f"{host}/{city}/{kind}/{ident}/tab/reviews"
            else:
                # Фоллбэк: просто добавляем /tab/reviews к URL без query/хвостов
                trimmed = base_url.split('?', 1)[0]
                if '/tab/reviews' in trimmed:
                    reviews_url = trimmed
                else:
                    reviews_url = trimmed.rstrip('/') + '/tab/reviews'

            logger.info(f"Navigating to reviews page: {reviews_url}")
            self.driver.navigate(reviews_url)
            # Ждем загрузки отзывов через JavaScript
            time.sleep(5)
            
            # Пытаемся дождаться появления отзывов на странице
            max_wait_attempts = 10
            for attempt in range(max_wait_attempts):
                page_source, soup_content = self._get_page_source_and_soup()
                # Проверяем наличие отзывов различными способами
                review_elements_test = soup_content.select("div._1k5soqfl, [class*='review'], [data-review-id]")
                if review_elements_test or 'отзыв' in page_source.lower()[:5000]:
                    logger.info(f"Reviews loaded after {attempt + 1} attempts")
                    break
                time.sleep(1)
            else:
                logger.warning("Reviews may not have loaded properly, continuing anyway")
            
            page_source, soup_content = self._get_page_source_and_soup()
            
            # Извлекаем рейтинг из структуры страницы карточки (до парсинга отзывов)
            # Структура: <div class="_1tam240">5</div>
            card_rating_from_page = 0.0
            rating_selectors_page = [
                'div._1tam240',  # Точный селектор для рейтинга
                'div[class*="_1tam240"]',
                '[class*="_1tam240"]',
                '[class*="rating"]',
                '[class*="star"]',
                '[data-rating]',
            ]
            for selector in rating_selectors_page:
                try:
                    rating_elems = soup_content.select(selector)
                    for rating_elem in rating_elems:
                        rating_text = rating_elem.get_text(strip=True)
                        # Если это число напрямую (как в _1tam240)
                        if rating_text.replace('.', '', 1).isdigit():
                            potential_rating = float(rating_text)
                            if 1.0 <= potential_rating <= 5.0:
                                card_rating_from_page = potential_rating
                                logger.info(f"Found rating from card page via selector {selector}: {card_rating_from_page}")
                                break
                        # Также ищем число в тексте
                        rating_match = re.search(r'([1-5](?:\.\d+)?)', rating_text)
                        if rating_match:
                            potential_rating = float(rating_match.group(1))
                            if 1.0 <= potential_rating <= 5.0:
                                card_rating_from_page = potential_rating
                                logger.info(f"Found rating from card page via selector {selector}: {card_rating_from_page}")
                                break
                        # Также проверяем data-атрибуты
                        if rating_elem.get('data-rating'):
                            try:
                                card_rating_from_page = float(rating_elem.get('data-rating'))
                                if 1.0 <= card_rating_from_page <= 5.0:
                                    logger.info(f"Found rating from card page data-rating: {card_rating_from_page}")
                                    break
                            except (ValueError, TypeError):
                                pass
                    if card_rating_from_page > 0:
                        break
                except Exception as e:
                    logger.debug(f"Error with rating selector {selector}: {e}")
                    continue

            # Сохраняем HTML вкладки отзывов для отладки селекторов
            try:
                debug_dir = os.path.join("debug", "2gis_reviews")
                os.makedirs(debug_dir, exist_ok=True)

                firm_id = locals().get("ident")
                if not firm_id:
                    m_id = re.search(r"/firm/(\d+)|/station/(\d+)", reviews_url)
                    if m_id:
                        firm_id = m_id.group(1) or m_id.group(2)

                if not firm_id:
                    firm_id = hashlib.md5(reviews_url.encode("utf-8")).hexdigest()[:8]

                ts = dt_module.datetime.now().strftime("%Y%m%d_%H%M%S")
                debug_path = os.path.join(debug_dir, f"reviews_{firm_id}_{ts}.html")
                with open(debug_path, "w", encoding="utf-8") as f:
                    f.write(page_source)

                logger.info(f"Saved 2GIS reviews debug HTML to {debug_path}")
            except Exception as dump_error:
                logger.warning(f"Could not save 2GIS reviews debug HTML: {dump_error}")

            # Извлекаем точное количество отзывов из структуры страницы карточки
            # Структура: <h2 class="_12jewu69"><a href="/spb/firm/70000001030294479/tab/reviews" class="_rdxuhv3">Отзывы<span class="_1xhlznaa">25</span></a></h2>
            reviews_count_total = 0
            
            # ПРИОРИТЕТ 1: Точный селектор из структуры карточки
            reviews_count_elem = soup_content.select_one('h2._12jewu69 a._rdxuhv3 span._1xhlznaa, h2[class*="_12jewu69"] a[class*="_rdxuhv3"] span._1xhlznaa, span._1xhlznaa')
            if reviews_count_elem:
                reviews_text = reviews_count_elem.get_text(strip=True)
                if reviews_text.isdigit():
                    reviews_count_total = int(reviews_text)
                    logger.info(f"Found reviews count from card page structure (span._1xhlznaa): {reviews_count_total}")
            
            # ПРИОРИТЕТ 2: Альтернативные селекторы для span._1xhlznaa
            if reviews_count_total == 0:
                for selector in ['span._1xhlznaa', 'span[class*="_1xhlznaa"]', '[class*="_1xhlznaa"]']:
                    reviews_count_elems = soup_content.select(selector)
                    for elem in reviews_count_elems:
                        reviews_text = elem.get_text(strip=True)
                        if reviews_text.isdigit():
                            potential_count = int(reviews_text)
                            if 0 < potential_count < 10000:  # Разумные пределы
                                reviews_count_total = potential_count
                                logger.info(f"Found reviews count via selector {selector}: {reviews_count_total}")
                                break
                    if reviews_count_total > 0:
                        break
            
            # ПРИОРИТЕТ 3: Поиск в ссылке на вкладку отзывов
            if reviews_count_total == 0:
                count_selectors = [
                    'a[href*="/tab/reviews"]',
                    '[class*="review"][class*="tab"]',
                ]
                for selector in count_selectors:
                    for elem in soup_content.select(selector):
                        text = elem.get_text(strip=True) if hasattr(elem, 'get_text') else (elem.get('content', '') if hasattr(elem, 'get') else str(elem))
                        matches = re.findall(r'(\d+)', text)
                        if matches:
                            potential_count = max(int(m) for m in matches)
                            if potential_count > reviews_count_total and potential_count < 10000:  # Фильтруем слишком большие числа
                                reviews_count_total = potential_count
                                logger.info(f"Found reviews count via selector {selector}: {reviews_count_total}")
            
            # ПРИОРИТЕТ 4: Из meta description
            if reviews_count_total == 0:
                meta_desc = soup_content.select_one('meta[name="description"]')
                if meta_desc:
                    desc_content = meta_desc.get('content', '')
                    desc_matches = re.findall(r'(\d+)\s+отзыв', desc_content, re.IGNORECASE)
                    if desc_matches:
                        potential_count = max(int(m) for m in desc_matches)
                        if potential_count > reviews_count_total:
                            reviews_count_total = potential_count
                            logger.info(f"Found reviews count from meta description: {reviews_count_total}")
            
            logger.info(f"Expected total reviews count from card page: {reviews_count_total}")
            
            # Обновляем прогресс с общим количеством отзывов
            if reviews_count_total > 0:
                self._update_progress(f"Парсинг отзывов: найдено {reviews_count_total} отзывов, начинаю обработку...")
            
            # Извлекаем количество оценок из структуры страницы
            # Структура: <div class="_jspzdm">Количество оценок</div>
            ratings_count_total = 0
            ratings_count_selectors = [
                'div._jspzdm',  # Точный селектор для количества оценок
                'div[class*="_jspzdm"]',
                '[class*="_jspzdm"]',
            ]
            for selector in ratings_count_selectors:
                try:
                    ratings_count_elems = soup_content.select(selector)
                    for elem in ratings_count_elems:
                        ratings_text = elem.get_text(strip=True)
                        # Ищем число в тексте элемента
                        if ratings_text.isdigit():
                            ratings_count_total = int(ratings_text)
                            logger.info(f"Found ratings count from card page structure (div._jspzdm): {ratings_count_total}")
                            break
                        # Ищем число в тексте элемента (может быть "123 оценок" или просто "123")
                        ratings_match = re.search(r'(\d+)', ratings_text)
                        if ratings_match:
                            potential_count = int(ratings_match.group(1))
                            if 0 < potential_count < 100000:  # Разумные пределы
                                ratings_count_total = potential_count
                                logger.info(f"Found ratings count via selector {selector}: {ratings_count_total}")
                                break
                    if ratings_count_total > 0:
                        break
                except Exception as e:
                    logger.debug(f"Error with ratings count selector {selector}: {e}")
                    continue
            
            logger.info(f"Found ratings count from card page: {ratings_count_total}")
            
            # Извлекаем количество отвеченных отзывов из структуры страницы
            # Структура: <span class="_1iurgbx">С ответами</span>
            answered_reviews_count = 0
            answered_selectors = [
                'span._1iurgbx',  # Точный селектор
                'span[class*="_1iurgbx"]',
            ]
            for selector in answered_selectors:
                try:
                    answered_elems = soup_content.select(selector)
                    for elem in answered_elems:
                        elem_text = elem.get_text(strip=True)
                        # Ищем элемент с текстом "С ответами" или "с ответами"
                        if 'ответами' in elem_text.lower() or 'ответ' in elem_text.lower():
                            # Ищем число в родительском элементе или в соседних элементах
                            parent = elem.find_parent()
                            if parent:
                                # Ищем число в тексте родителя
                                parent_text = parent.get_text(strip=True)
                                # Ищем паттерн типа "16 С ответами" или "С ответами 16"
                                answered_match = re.search(r'(\d+)\s*(?:с\s+ответами|ответами)|(?:с\s+ответами|ответами)\s*(\d+)', parent_text, re.IGNORECASE)
                                if answered_match:
                                    answered_reviews_count = int(answered_match.group(1) or answered_match.group(2))
                                    logger.info(f"Found answered reviews count via selector {selector}: {answered_reviews_count}")
                                    break
                                # Если не нашли паттерн, ищем любое число в родителе
                                numbers = re.findall(r'\b(\d+)\b', parent_text)
                                if numbers:
                                    # Берем первое число, которое может быть количеством отвеченных
                                    for num_str in numbers:
                                        num = int(num_str)
                                        if 1 <= num <= reviews_count_total:  # Разумные пределы
                                            answered_reviews_count = num
                                            logger.info(f"Found answered reviews count via selector {selector} (from parent numbers): {answered_reviews_count}")
                                            break
                                    if answered_reviews_count > 0:
                                        break
                    if answered_reviews_count > 0:
                        break
                except Exception as e:
                    logger.debug(f"Error with answered selector {selector}: {e}")
                    continue
            
            # ПРИОРИТЕТ 3: Подсчитываем количество элементов с div._1wk3bjs (блок ответа организации)
            # Это точный способ определения количества отвеченных отзывов
            if answered_reviews_count == 0:
                official_response_selectors = [
                    'div._1wk3bjs',  # Точный селектор для блока ответа организации
                    'div[class*="_1wk3bjs"]',
                ]
                for selector in official_response_selectors:
                    try:
                        official_response_elems = soup_content.select(selector)
                        # Подсчитываем уникальные элементы с ответами
                        # Фильтруем элементы, которые явно содержат ответ (не пустые и содержат текст)
                        valid_response_elems = [
                            elem for elem in official_response_elems
                            if elem.get_text(strip=True) and len(elem.get_text(strip=True)) >= 3
                        ]
                        if len(valid_response_elems) > 0:
                            answered_reviews_count = len(valid_response_elems)
                            logger.info(f"Found answered reviews count via selector {selector} (counting div._1wk3bjs elements): {answered_reviews_count}")
                            break
                    except Exception as e:
                        logger.debug(f"Error with official response selector {selector}: {e}")
                        continue
            
            # ПРИОРИТЕТ 4: Если не нашли через селекторы, ищем в тексте страницы
            if answered_reviews_count == 0:
                page_text = soup_content.get_text(separator=' ', strip=True)
                answered_matches = re.findall(r'(\d+)\s*(?:с\s+ответами|ответами|ответ)', page_text, re.IGNORECASE)
                if answered_matches:
                    answered_reviews_count = max(int(m) for m in answered_matches)
                    if answered_reviews_count <= reviews_count_total:  # Проверяем разумность
                        logger.info(f"Found answered reviews count from page text: {answered_reviews_count}")
                    else:
                        answered_reviews_count = 0
            
            logger.info(f"Found answered reviews count from card page: {answered_reviews_count}")
            
            # Прокручиваем, чтобы подгрузить все отзывы
            self._scroll_to_load_all_reviews(expected_count=reviews_count_total)
            
            # Дополнительное ожидание для загрузки всех отзывов после прокрутки
            time.sleep(2)
            page_source, soup_content = self._get_page_source_and_soup()
            
            # Кликаем на все "Читать целиком" для загрузки полного текста отзывов на первой странице
            try:
                expand_all_script = """
                var expandLinks = document.querySelectorAll('span._17ww69i, a[class*="читать"], [class*="читать целиком"], [class*="_17ww69i"]');
                var clicked = 0;
                for (var i = 0; i < expandLinks.length; i++) {
                    var link = expandLinks[i];
                    if (link.offsetParent !== null && link.textContent.toLowerCase().includes('читать')) {
                        try {
                            link.click();
                            clicked++;
                            if (clicked >= 100) break; // Ограничиваем количество кликов
                        } catch(e) {}
                    }
                }
                return clicked;
                """
                clicked_count = self.driver.execute_script(expand_all_script)
                if clicked_count > 0:
                    logger.info(f"Clicked 'read more' on {clicked_count} reviews to load full text on first page")
                    time.sleep(2)  # Ждем загрузки полного текста
                    page_source, soup_content = self._get_page_source_and_soup()  # Обновляем HTML
            except Exception as expand_error:
                logger.warning(f"Could not expand review texts on first page: {expand_error}")
            
            # Проверяем, сколько отзывов найдено после прокрутки
            test_review_elements = soup_content.select("div._1k5soqfl, [data-review-id], [class*='review-item'], div[class*='_4db12d']")
            logger.info(f"Found {len(test_review_elements)} review elements after scroll on first page")

            # Пагинация по страницам отзывов
            pagination_links = soup_content.select(
                'a[href*="/tab/reviews"][href*="page="], a[href*="/reviews"][href*="page="]'
            )
            all_pages_urls: set[str] = set()
            for link in pagination_links:
                href = link.get('href', '')
                if href and 'page=' in href:
                    if not href.startswith('http'):
                        href = urllib.parse.urljoin("https://2gis.ru", href)
                    all_pages_urls.add(href)
            
            # Также ищем кнопку "Показать еще" или "Загрузить еще отзывы"
            load_more_selectors = [
                'button[class*="load"], button[class*="more"], a[class*="load"], a[class*="more"]',
                '[class*="показать"], [class*="загрузить"], [class*="еще"]',
            ]
            has_load_more = False
            for selector in load_more_selectors:
                load_more_buttons = soup_content.select(selector)
                for button in load_more_buttons:
                    button_text = button.get_text(strip=True).lower()
                    if any(word in button_text for word in ['показать', 'загрузить', 'еще', 'more', 'load']):
                        has_load_more = True
                        logger.info(f"Found 'load more' button: {button_text}")
                        break
                if has_load_more:
                    break
            
            if has_load_more:
                logger.info("Page has 'load more' button - will use infinite scroll approach")

            all_reviews: List[Dict[str, Any]] = []
            # Шаг 1: Инициализация счетчиков для расчета среднего времени ответа (по предложенному плану)
            # Используем timedelta для более точного расчета
            total_response_time = timedelta(0)  # Сумма всех разниц во времени
            count_with_replies = 0              # Количество отзывов, на которые был ответ
            # Сохраняем старые переменные для совместимости
            response_time_sum_days: float = 0.0
            response_time_count: int = 0
            # Сохраняем card_url для сохранения данных о датах ответов
            current_card_url = card_url
            pages_to_process: List[str] = [reviews_url]
            if all_pages_urls:
                # Обрабатываем только реально существующие страницы (не более 10 для оптимизации)
                sorted_pages = sorted(all_pages_urls)
                # Ограничиваем до 10 страниц, чтобы не парсить лишнее
                pages_to_process.extend(sorted_pages[:10])
                logger.info(f"Found {len(all_pages_urls)} pagination pages, will process up to 10 pages")

            seen_review_keys: set[str] = set()

            for page_url in pages_to_process:
                if self._is_stopped():
                    logger.info(f"2GIS reviews: stop flag detected before processing reviews page {page_url}, breaking pages loop")
                    break
                try:
                    if page_url != reviews_url:
                        logger.info(f"Processing reviews page: {page_url}")
                        self.driver.navigate(page_url)
                        # Ждем загрузки отзывов на новой странице
                        time.sleep(3)
                        # Прокручиваем страницу для загрузки всех отзывов
                        # Получаем ожидаемое количество отзывов для этой страницы
                        page_source_temp, soup_temp = self._get_page_source_and_soup()
                        expected_count_temp = 0
                        meta_desc_temp = soup_temp.select_one('meta[name="description"]')
                        if meta_desc_temp:
                            desc_content = meta_desc_temp.get('content', '')
                            desc_matches = re.findall(r'(\d+)\s+отзыв', desc_content, re.IGNORECASE)
                            if desc_matches:
                                expected_count_temp = max(int(m) for m in desc_matches)
                        logger.info(f"Scrolling page {page_url} to load reviews (expected: {expected_count_temp})")
                        self._scroll_to_load_all_reviews(expected_count=expected_count_temp)
                        time.sleep(2)  # Увеличено до 2 сек после прокрутки
                        
                        # Проверяем, что новые отзывы действительно загрузились
                        page_source_after, soup_after = self._get_page_source_and_soup()
                        reviews_after_scroll = len(soup_after.select("div._1k5soqfl, [data-review-id], [class*='review-item']"))
                        logger.info(f"After scroll on page {page_url}: found {reviews_after_scroll} review elements")
                    
                        page_source, soup_content = self._get_page_source_and_soup()
                    
                    # Кликаем на все "Читать целиком" для загрузки полного текста отзывов
                    # Повторяем попытку до 3 раз при ошибках
                    expand_attempts = 0
                    max_expand_attempts = 3
                    while expand_attempts < max_expand_attempts:
                        try:
                            expand_all_script = """
                            var expandLinks = document.querySelectorAll('span._17ww69i, a[class*="читать"], [class*="читать целиком"], [class*="_17ww69i"]');
                            var clicked = 0;
                            for (var i = 0; i < expandLinks.length; i++) {
                                var link = expandLinks[i];
                                if (link.offsetParent !== null && link.textContent.toLowerCase().includes('читать')) {
                                    try {
                                        link.click();
                                        clicked++;
                                        if (clicked >= 100) break; // Увеличено с 50 до 100
                                    } catch(e) {}
                                }
                            }
                            return clicked;
                            """
                            clicked_count = self.driver.execute_script(expand_all_script)
                            if clicked_count > 0:
                                logger.info(f"Clicked 'read more' on {clicked_count} reviews to load full text (attempt {expand_attempts + 1})")
                                time.sleep(2.5)  # Увеличено до 2.5 сек для загрузки полного текста
                                page_source, soup_content = self._get_page_source_and_soup()  # Обновляем HTML
                                break  # Успешно, выходим из цикла
                            else:
                                break  # Нет кнопок для клика, выходим
                        except Exception as expand_error:
                            expand_attempts += 1
                            if expand_attempts < max_expand_attempts:
                                logger.warning(f"Could not expand review texts (attempt {expand_attempts}/{max_expand_attempts}): {expand_error}, retrying...")
                                time.sleep(1)
                            else:
                                logger.warning(f"Could not expand review texts after {max_expand_attempts} attempts: {expand_error}")

                    # Для 2ГИС карточек отзывы лежат в контейнерах div._1k5soqfl
                    # Пробуем различные селекторы для поиска отзывов
                    review_elements = soup_content.select("div._1k5soqfl")
                    if not review_elements:
                        # Пробуем найти отзывы через data-атрибуты или другие признаки
                        review_elements = soup_content.select('[data-review-id], [class*="review-item"], [class*="review"]')
                    if not review_elements:
                        # Фоллбэк: ищем любые элементы, содержащие текст отзыва
                        # Ищем элементы с классом, содержащим "review" или похожие паттерны
                        review_elements = soup_content.select(
                            'div[class*="review"], li[class*="review"], article[class*="review"], '
                            'div[class*="_1k5"], div[class*="_4db12d"]'
                        )
                    if not review_elements:
                        logger.warning(f"No review elements found on page {page_url}")
                        continue
                    
                    logger.info(f"Found {len(review_elements)} review elements on page {page_url}")
                    
                    # ЭТАП 1: Собираем все элементы отзывов (минимальная фильтрация)
                    # Сохраняем все элементы для последующего детального парсинга
                    collected_review_elements = []
                    skipped_count = 0
                    
                    if len(review_elements) == 0:
                        # Пробуем найти отзывы через альтернативные методы
                        logger.warning(f"No reviews found with standard selectors on {page_url}")
                        # Сохраняем HTML для отладки
                        try:
                            debug_dir = os.path.join("debug", "2gis_reviews")
                            os.makedirs(debug_dir, exist_ok=True)
                            ts = dt_module.datetime.now().strftime("%Y%m%d_%H%M%S")
                            debug_path = os.path.join(debug_dir, f"no_reviews_{ts}.html")
                            with open(debug_path, "w", encoding="utf-8") as f:
                                f.write(page_source)
                            logger.info(f"Saved page HTML to {debug_path} for debugging")
                        except Exception as debug_error:
                            logger.warning(f"Could not save debug HTML: {debug_error}")
                        continue

                    # Собираем все элементы отзывов (только базовая фильтрация)
                    for review_elem in review_elements:
                        if self._is_stopped():
                            logger.info("2GIS reviews: stop flag detected during collection, breaking")
                            break
                        
                        # Минимальная фильтрация: пропускаем только явно невалидные элементы
                        elem_text = review_elem.get_text(strip=True)
                        if not elem_text or len(elem_text) < 3:
                            skipped_count += 1
                            continue
                        # Пропускаем элементы навигации
                        if any(skip_word in elem_text.lower() for skip_word in ['читать целиком', 'показать еще', 'следующая', 'предыдущая', 'страница']):
                            skipped_count += 1
                            continue
                        
                        # Сохраняем элемент для последующего парсинга
                        collected_review_elements.append(review_elem)
                    
                    logger.info(f"Collected {len(collected_review_elements)} review elements for detailed parsing (skipped {skipped_count} invalid elements)")
                    
                    # ЭТАП 2: Парсим детали каждого отзыва в правильном порядке
                    # Согласно предложению: сначала собираем все отзывы, потом парсим детали
                    processed_count = 0
                    for review_elem in collected_review_elements:
                        if self._is_stopped():
                            logger.info("2GIS reviews: stop flag detected inside reviews loop, breaking")
                            break
                        
                        processed_count += 1
                        
                        # Обновляем прогресс для каждого обработанного отзыва (каждые 3 отзыва для более частого обновления)
                        if processed_count % 3 == 0 or processed_count == 1:
                            total_expected = reviews_count_total if reviews_count_total > 0 else 0
                            # Используем общее количество добавленных отзывов из всех страниц (не обработанных элементов, а реальных отзывов)
                            total_processed = len(all_reviews)
                            if total_expected > 0:
                                self._update_progress(f"Парсинг отзывов: обработано {total_processed} из {total_expected}")
                            else:
                                self._update_progress(f"Парсинг отзывов: обработано {total_processed} отзывов")
                        
                        # ПАРСИНГ ДЕТАЛЕЙ ОТЗЫВА В ПРАВИЛЬНОМ ПОРЯДКЕ (согласно предложению):
                        # 1. Автор отзыва: span._16s5yj36
                        # 2. Дата отзыва: div._a5f6uz
                        # 3. Текст отзыва: a._1msln3t
                        # 4. Ответ организации: div._1wk3bjs
                        # 5. Дата ответа организации: div._1evjsdb
                        # 6. ID отзыва и ответа
                        # 7. Время ответа (если есть ответ)
                        
                        # Инициализируем переменные для ID (нужны для генерации response_id)
                        review_id = None
                        response_id = None
                        
                        # Извлекаем ID отзыва заранее (нужен для генерации response_id)
                        # ПРИОРИТЕТ 1: data-review-id атрибут
                        review_id = review_elem.get('data-review-id') or review_elem.get('data-id') or review_elem.get('id')
                        # ПРИОРИТЕТ 2: Ищем в дочерних элементах
                        if not review_id:
                            review_id_elem = review_elem.select_one('[data-review-id], [data-id]')
                            if review_id_elem:
                                review_id = review_id_elem.get('data-review-id') or review_id_elem.get('data-id')
                        # ПРИОРИТЕТ 3: Ищем в ссылках (может быть в href)
                        if not review_id:
                            link_elem = review_elem.select_one('a[href*="review"], a[href*="отзыв"]')
                            if link_elem:
                                href = link_elem.get('href', '')
                                # Извлекаем ID из URL, например /review/12345
                                id_match = re.search(r'/review[\/\-]?(\d+)', href, re.IGNORECASE)
                                if id_match:
                                    review_id = id_match.group(1)
                        
                        # 1. Автор отзыва - используем точный селектор из структуры 2GIS
                        # ПРИОРИТЕТ 1: Точный селектор span._16s5yj36 (согласно предложению)
                        author_name = ""
                        author_elem = review_elem.select_one('span._16s5yj36, span[class*="_16s5yj36"]')
                        if author_elem:
                            author_name = author_elem.get_text(strip=True)
                            # Также проверяем атрибут title, если есть
                            if not author_name:
                                author_name = author_elem.get('title', '').strip()
                        
                        # Fallback: ищем в других селекторах
                        if not author_name:
                            author_elem = review_elem.select_one('[class*="author"], [class*="user"], [class*="name"], [title]')
                            if author_elem:
                                author_name = author_elem.get_text(strip=True)
                                if not author_name:
                                    author_name = author_elem.get('title', '').strip()
                        
                        # Fallback: извлекаем из текста элемента
                        if not author_name:
                            all_text = review_elem.get_text()
                            name_match = re.search(
                                r'([А-ЯЁ][а-яё]+\s+[А-ЯЁ][а-яё]+|[А-ЯЁ][а-яё]+)', all_text
                            )
                            if name_match and len(name_match.group(1)) > 2:
                                author_name = name_match.group(1)
                        
                        if not author_name:
                            author_name = "Аноним"

                        # Дата отзыва - используем точный селектор из структуры 2GIS
                        # ПРИОРИТЕТ 1: Точный селектор div._a5f6uz
                        date_elem = review_elem.select_one('div._a5f6uz, div[class*="_a5f6uz"], [class*="date"], time, [class*="time"]')
                        review_date: Optional[dt_module.datetime] = None
                        date_text = ""
                        if date_elem:
                            date_text = date_elem.get_text(strip=True)
                            datetime_attr = date_elem.get('datetime', '')
                            if datetime_attr:
                                try:
                                    review_date = dt_module.datetime.fromisoformat(
                                        datetime_attr.replace('Z', '+00:00')
                                    )
                                except Exception:
                                    review_date = parse_russian_date(date_text)
                            else:
                                review_date = parse_russian_date(date_text)
                        else:
                            all_text = review_elem.get_text()
                            date_match = re.search(
                                r'(\d{1,2}\s+[а-яё]+\s+\d{4})', all_text, re.IGNORECASE
                            )
                            if date_match:
                                date_text = date_match.group(1)
                                review_date = parse_russian_date(date_text)

                        # Рейтинг - пробуем различные способы извлечения
                        rating_value = 0.0
                        
                        # Способ 1: Ищем звезды через различные селекторы
                        stars = review_elem.select('[class*="star"], img[class*="icon"], svg[class*="star"], [class*="rating-star"]')
                        if stars:
                            filled_stars = len(
                                [
                                    s
                                    for s in stars
                                    if 'active' in str(s.get('class', []))
                                    or 'fill' in str(s.get('class', []))
                                    or 'filled' in str(s.get('class', []))
                                    or s.get('fill') and s.get('fill') != 'none'
                                    or s.get('style') and 'fill' in str(s.get('style', ''))
                                ]
                            )
                            if filled_stars > 0:
                                rating_value = float(filled_stars)

                        # Способ 2: Ищем рейтинг в data-атрибутах
                        if not rating_value:
                            rating_attr = review_elem.get('data-rating') or review_elem.get('data-score')
                            if rating_attr:
                                try:
                                    rating_value = float(rating_attr)
                                except (ValueError, TypeError):
                                    pass
                        
                        # Способ 3: Ищем рейтинг в тексте элементов с классом rating/score
                        if not rating_value:
                            rating_elem = review_elem.select_one(
                                '[class*="rating"], [class*="score"], [aria-label*="оценка"], [aria-label*="rating"]'
                            )
                            if rating_elem:
                                rating_text = rating_elem.get_text(strip=True)
                                rating_match = re.search(r'(\d+(?:\.\d+)?)', rating_text)
                                if rating_match:
                                    rating_value = float(rating_match.group(1))

                        # Способ 4: Ищем рейтинг во всем тексте отзыва (паттерны типа "5 из 5", "4.5", "⭐5")
                        if not rating_value:
                            all_text = review_elem.get_text(separator=' ', strip=True)
                            # Ищем паттерны: "5 из 5", "4.5 звезд", "⭐5", "5/5"
                            rating_patterns = [
                                r'(\d+(?:\.\d+)?)\s*(?:из|/)\s*5',
                                r'(\d+(?:\.\d+)?)\s*(?:звезд|star|⭐)',
                                r'⭐\s*(\d+(?:\.\d+)?)',
                                r'(\d+(?:\.\d+)?)\s*/\s*5',
                                r'rating[:\s]*(\d+(?:\.\d+)?)',
                            ]
                            for pattern in rating_patterns:
                                match = re.search(pattern, all_text, re.IGNORECASE)
                                if match:
                                    try:
                                        rating_value = float(match.group(1))
                                        if 1 <= rating_value <= 5:
                                            break
                                    except (ValueError, IndexError):
                                        continue
                        
                        # Способ 5: Ищем рейтинг в дочерних элементах через SVG или другие индикаторы
                        if not rating_value:
                            # Ищем количество заполненных звезд через SVG path или другие индикаторы
                            filled_indicators = review_elem.select('[fill="#ffb81c"], [fill="#ffb800"], [class*="filled"], [style*="fill"]')
                            if filled_indicators:
                                rating_value = min(len(filled_indicators), 5.0)
                        
                        logger.debug(f"Extracted rating: {rating_value} for review by {author_name}")

                        # 3. Текст отзыва - используем точный селектор из структуры 2GIS
                        # ПРИОРИТЕТ 1: Точный селектор a._1msln3t (согласно предложению)
                        # ВАЖНО: На странице 2ГИС текст отзыва может быть в разных местах:
                        # - a._1msln3t - ссылка с текстом отзыва (часто сокращенный)
                        # - div внутри review_elem - полный текст отзыва
                        # - После клика "читать целиком" текст раскрывается
                        review_text = ""
                        # Пробуем несколько вариантов селектора для текста отзыва
                        text_elem = review_elem.select_one('a._1msln3t, a[class*="_1msln3t"], a[class*="msln3t"]')
                        if text_elem:
                            review_text = text_elem.get_text(separator=' ', strip=True)
                        
                        # Также пробуем найти текст в других местах
                        if not review_text:
                            # Ищем в div с похожими классами
                            text_elem = review_elem.select_one('div[class*="_1msln3t"], div[class*="msln3t"], [class*="review-text"]')
                            if text_elem:
                                review_text = text_elem.get_text(separator=' ', strip=True)
                        
                        # Дополнительно: ищем текст в дочерних элементах, исключая служебные
                        if not review_text or len(review_text) < 10:
                            # Ищем все текстовые блоки внутри отзыва, исключая автора, дату, ответ
                            text_candidates = []
                            for child in review_elem.find_all(['div', 'p', 'span', 'a']):
                                child_class = ' '.join(child.get('class', []))
                                # Пропускаем служебные элементы
                                if any(skip in child_class.lower() for skip in ['_16s5yj36', '_a5f6uz', '_1wk3bjs', '_1evjsdb', 'читать', 'целиком', 'показать']):
                                    continue
                                child_text = child.get_text(separator=' ', strip=True)
                                if child_text and len(child_text) >= 10:  # Минимум 10 символов для валидного текста
                                    text_candidates.append(child_text)
                            
                            # Берем самый длинный текст (скорее всего это основной текст отзыва)
                            if text_candidates:
                                review_text = max(text_candidates, key=len)
                        
                        # Fallback: пробуем другие селекторы, если точный не сработал
                        if not review_text or len(review_text) < 3:
                            review_text_selectors = [
                                '[class*="_1wlx08h"]',  # Класс для текста отзыва в 2GIS (сокращенный)
                                '[class*="_msln3t"]',  # Класс для полного текста отзыва в 2GIS
                                'div[class*="_kcpnuw"]',  # Класс для контента отзыва
                                'div[class*="text"]',
                                '[class*="content"]',
                                '[class*="comment"]',
                                'p[class*="text"]',
                                'div[class*="review-text"]',
                                '[class*="text"][class*="review"]',
                            ]
                            for text_selector in review_text_selectors:
                                text_elements = review_elem.select(text_selector)
                                for text_element in text_elements:
                                    candidate_text = text_element.get_text(separator=' ', strip=True)
                                    candidate_text = ' '.join(candidate_text.split())
                                    
                                    # Очищаем от информации об авторе и количестве отзывов в начале
                                    # Убираем невидимые символы и имя автора с количеством отзывов
                                    candidate_text = re.sub(
                                        r'^[\s\u200b\u200c\u200d\u2060\u00a0]*[a-zA-Zа-яёА-ЯЁ0-9_\-]+\s*[\s\u200b\u200c\u200d\u2060\u00a0]*\d+\s*[\s\u200b\u200c\u200d\u2060\u00a0]*отзыв[аов]*\s*',
                                        '',
                                        candidate_text,
                                        flags=re.IGNORECASE
                                    )
                                    # Дополнительная очистка для полных имен
                                    candidate_text = re.sub(
                                        r'^[\s\u200b\u200c\u200d\u2060\u00a0]*[А-ЯЁA-Z][а-яёa-z]+\s+[А-ЯЁA-Z][а-яёa-z]+\s*[\s\u200b\u200c\u200d\u2060\u00a0]*\d+\s*[\s\u200b\u200c\u200d\u2060\u00a0]*отзыв[аов]*\s*',
                                        '',
                                        candidate_text,
                                        flags=re.IGNORECASE
                                    )
                                    candidate_text = re.sub(
                                        r'^[\s\u200b\u200c\u200d\u2060\u00a0]*[А-ЯЁA-Z][а-яёa-z]+\s*[\s\u200b\u200c\u200d\u2060\u00a0]*\d+\s*[\s\u200b\u200c\u200d\u2060\u00a0]*отзыв[аов]*\s*',
                                        '',
                                        candidate_text,
                                        flags=re.IGNORECASE
                                    )
                                    
                                    # Очищаем от "Полезно?" в конце
                                    candidate_text = re.sub(
                                        r'\s*(Полезно\??|полезно\??)\s*$',
                                        '',
                                        candidate_text,
                                        flags=re.IGNORECASE
                                    )
                                    
                                    candidate_text = ' '.join(candidate_text.split()).strip()
                                    
                                    # Принимаем текст отзыва, если он не пустой и не слишком короткий
                                    # Уменьшили минимальную длину с 10 до 3 символов
                                    if candidate_text and len(candidate_text) >= 3:
                                        # Исключаем тексты, которые выглядят как метаданные (только даты, имена и т.д.)
                                        # Но только если текст очень короткий (меньше 20 символов)
                                        if len(candidate_text) >= 20 or not re.match(r'^[\d\sа-яёА-ЯЁ,\.]+$', candidate_text):
                                            review_text = candidate_text
                                        break
                                if review_text:
                                    break

                        # Если не нашли текст через селекторы, пробуем извлечь из всего элемента
                        # Но исключаем элементы с классом "читать целиком" и другие служебные элементы
                        if not review_text or len(review_text) < 3:
                            # Пробуем найти текст в дочерних элементах, исключая служебные
                            text_parts = []
                            for child in review_elem.find_all(['div', 'p', 'span']):
                                child_class = ' '.join(child.get('class', []))
                                child_text = child.get_text(separator=' ', strip=True)
                                # Пропускаем служебные элементы
                                if any(skip in child_class.lower() for skip in ['читать', 'целиком', 'показать', 'еще', 'load', 'more']):
                                    continue
                                if child_text and len(child_text) >= 3:  # Согласовано с минимальной длиной отзыва
                                    text_parts.append(child_text)
                            
                            if text_parts:
                                # Берем самый длинный текст (скорее всего это основной текст отзыва)
                                review_text = max(text_parts, key=len)
                            else:
                                # Фоллбэк: извлекаем из всего элемента, но более аккуратно
                                all_text = review_elem.get_text(separator=' ', strip=True)
                                
                                # Удаляем служебные элементы из текста
                                # Удаляем ссылки "читать целиком" и подобные
                                all_text = re.sub(r'читать\s+целиком', '', all_text, flags=re.IGNORECASE)
                                all_text = re.sub(r'показать\s+еще', '', all_text, flags=re.IGNORECASE)
                                
                                # Очищаем от информации об авторе и количестве отзывов в начале
                                all_text = re.sub(
                                    r'^[a-zA-Zа-яёА-ЯЁ0-9_\-]+\s+\d+\s+отзыв[аов]*\s*',
                                    '',
                                    all_text,
                                    flags=re.IGNORECASE
                                )
                                
                                # Очищаем от метаданных
                            cleaned_text = re.sub(
                                r'\d+[.,]\d+\s*(звезд|star|⭐)',
                                '',
                                all_text,
                                flags=re.IGNORECASE,
                            )
                            cleaned_text = re.sub(
                                r'\d{1,2}\s+('
                                r'янв|фев|мар|апр|май|июн|июл|авг|сен|окт|ноя|дек'
                                r')[а-яё]*\s+\d{4}',
                                '',
                                cleaned_text,
                                flags=re.IGNORECASE,
                            )
                                # Удаляем только отдельные слова "Полезно", "Оценка" и т.д., но не удаляем их из текста отзыва
                            cleaned_text = re.sub(
                                    r'^\s*(Полезно|полезно|Оценка|оценка|Отзыв|отзыв|звезд|лайк)\s*$',
                                '',
                                cleaned_text,
                                    flags=re.IGNORECASE | re.MULTILINE,
                            )
                            
                            # Очищаем от "Полезно?" в конце текста
                            cleaned_text = re.sub(
                                r'\s*(Полезно\??|полезно\??)\s*$',
                                '',
                                cleaned_text,
                                flags=re.IGNORECASE
                            )
                            
                            cleaned_text = ' '.join(cleaned_text.split()).strip()
                            
                            # Принимаем текст, если он длиннее 3 символов
                            # Но проверяем, что это не только метаданные (даты, имена)
                            if len(cleaned_text) >= 3:
                                # Если текст очень короткий (меньше 15 символов), проверяем, что это не только метаданные
                                if len(cleaned_text) >= 15 or not re.match(r'^[\d\sа-яёА-ЯЁ,\.\-]+$', cleaned_text):
                                    review_text = cleaned_text

                        # 4. Ответ организации - используем точный селектор из структуры 2GIS
                        # ПРИОРИТЕТ 1: Точный селектор div._1wk3bjs (согласно предложению)
                        answer_elem = review_elem.select_one('div._1wk3bjs, div[class*="_1wk3bjs"]')
                        has_response = bool(answer_elem)
                        response_text = ""
                        response_date: Optional[dt_module.datetime] = None
                        has_official_marker_in_answer = False  # Флаг для маркера "официальный ответ" в блоке ответа
                        
                        # Fallback: ищем в других селекторах, если точный не сработал
                        if not answer_elem:
                            answer_elem = review_elem.select_one('[class*="answer"], [class*="reply"], [class*="response"]')
                            has_response = bool(answer_elem)
                        
                        # Также проверяем наличие даты ответа как признак наличия ответа
                        if not has_response:
                            response_date_check = review_elem.select_one('div._1evjsdb, div[class*="_1evjsdb"]')
                            if response_date_check:
                                has_response = True
                                answer_elem = response_date_check.parent if response_date_check.parent else response_date_check

                        # Извлекаем ID ответа из блока ответа
                        response_id = None
                        if answer_elem:
                            # Сначала извлекаем текст ответа (нужен для генерации ID)
                            response_text_elem = answer_elem.select_one(
                                '[class*="text"], [class*="content"], div[class*="_1wk3bjs"]'
                            )
                            if response_text_elem:
                                response_text = response_text_elem.get_text(strip=True)
                            else:
                                response_text = answer_elem.get_text(strip=True)
                            
                            # ПРИОРИТЕТ 1: data-response-id или data-id атрибут в блоке ответа
                            response_id = answer_elem.get('data-response-id') or answer_elem.get('data-id') or answer_elem.get('id')
                            # ПРИОРИТЕТ 2: Ищем в дочерних элементах блока ответа
                            if not response_id:
                                response_id_elem = answer_elem.select_one('[data-response-id], [data-id]')
                                if response_id_elem:
                                    response_id = response_id_elem.get('data-response-id') or response_id_elem.get('data-id')
                            # ПРИОРИТЕТ 3: Генерируем ID на основе review_id и содержимого ответа
                            if not response_id and review_id:
                                response_content_hash = hashlib.md5(
                                    f"{review_id}_{response_text[:50] if response_text else ''}".encode('utf-8')
                                ).hexdigest()[:16]
                                response_id = f"resp_{response_content_hash}"
                            # ПРИОРИТЕТ 4: Если нет review_id, генерируем на основе содержимого
                            if not response_id:
                                response_content_hash = hashlib.md5(
                                    f"{author_name}_{date_text}_response_{response_text[:50] if response_text else ''}".encode('utf-8')
                                ).hexdigest()[:16]
                                response_id = f"resp_{response_content_hash}"

                            # 5. Дата ответа организации - используем точный селектор из структуры 2GIS
                            # ПРИОРИТЕТ 1: Точный селектор div._1evjsdb (согласно предложению)
                            response_date_elem = answer_elem.select_one('div._1evjsdb, div[class*="_1evjsdb"]')
                            # Fallback: ищем в других селекторах
                            if not response_date_elem:
                                response_date_elem = answer_elem.select_one('[class*="date"], time, [class*="response-date"], [class*="answer-date"]')
                            if response_date_elem:
                                response_date_text = response_date_elem.get_text(strip=True)
                                if not response_date_text:
                                    # Пробуем извлечь из атрибутов
                                    response_date_text = response_date_elem.get('datetime', '') or response_date_elem.get('data-date', '')
                                if response_date_text:
                                    # Передаем год отзыва в парсер, чтобы правильно определить год ответа
                                    if review_date:
                                        response_date = parse_russian_date(response_date_text, current_year=review_date.year)
                                        # Если дата ответа без года, но есть дата отзыва, используем год отзыва
                                        if response_date and review_date:
                                            # Если в response_date_text нет года, но есть день и месяц
                                            if not re.search(r'\d{4}', response_date_text):
                                                # Определяем год ответа: если ответ пришел позже отзыва в том же году - используем год отзыва
                                                # Если ответ пришел раньше отзыва (переход через границу года) - используем следующий год
                                                if response_date.month < review_date.month or (response_date.month == review_date.month and response_date.day < review_date.day):
                                                    # Ответ пришел в следующем году
                                                    response_date = response_date.replace(year=review_date.year + 1)
                                                    logger.debug(f"Adjusted 2GIS response date year: answer came in next year (review={review_date.isoformat()}, response={response_date.isoformat()})")
                                                else:
                                                    # Ответ пришел в том же году
                                                    response_date = response_date.replace(year=review_date.year)
                                                    logger.debug(f"Adjusted 2GIS response date year: using year {response_date.year} from review date {review_date.year}")
                                    else:
                                        # Если нет даты отзыва, используем текущий год
                                        response_date = parse_russian_date(response_date_text)
                            
                            # Проверяем маркер "официальный ответ" ТОЛЬКО в блоке ответа
                            answer_text = answer_elem.get_text(separator=' ', strip=True).lower() if answer_elem else ""
                            has_official_marker_in_answer = 'официальный ответ' in answer_text or 'ответ компании' in answer_text or 'ответ организации' in answer_text
                        else:
                            # Если ответа нет, response_id остается None
                            response_id = None
                        
                        # Если ответ найден, но дата не извлечена, пробуем найти дату в самом элементе отзыва
                        if has_response and not response_date:
                            # Ищем дату ответа напрямую в элементе отзыва (div._1evjsdb)
                            response_date_elem_direct = review_elem.select_one('div._1evjsdb, div[class*="_1evjsdb"]')
                            if response_date_elem_direct:
                                response_date_text = response_date_elem_direct.get_text(strip=True)
                                if not response_date_text:
                                    response_date_text = response_date_elem_direct.get('datetime', '') or response_date_elem_direct.get('data-date', '')
                                if response_date_text:
                                    # Передаем год отзыва в парсер, чтобы правильно определить год ответа
                                    if review_date:
                                        response_date = parse_russian_date(response_date_text, current_year=review_date.year)
                                        # Если дата ответа без года, но есть дата отзыва, используем год отзыва
                                        if response_date and review_date:
                                            # Если в response_date_text нет года, но есть день и месяц
                                            if not re.search(r'\d{4}', response_date_text):
                                                # Определяем год ответа: если ответ пришел позже отзыва в том же году - используем год отзыва
                                                # Если ответ пришел раньше отзыва (переход через границу года) - используем следующий год
                                                if response_date.month < review_date.month or (response_date.month == review_date.month and response_date.day < review_date.day):
                                                    # Ответ пришел в следующем году
                                                    response_date = response_date.replace(year=review_date.year + 1)
                                                    logger.debug(f"Adjusted 2GIS response date year (direct): answer came in next year (review={review_date.isoformat()}, response={response_date.isoformat()})")
                                                else:
                                                    # Ответ пришел в том же году
                                                    response_date = response_date.replace(year=review_date.year)
                                                    logger.debug(f"Adjusted 2GIS response date year (direct): using year {response_date.year} from review date {review_date.year}")
                                    else:
                                        # Если нет даты отзыва, используем текущий год
                                        response_date = parse_russian_date(response_date_text)
                                    logger.debug(f"Extracted response_date from direct search: {response_date}")

                        # 2ГИС часто помечает официальный ответ только текстом
                        # вида "29 мая 2025, официальный ответ" без специальных классов.
                        # Если специальных блоков нет, пробуем найти такой текст в общем содержимом.
                        # ВАЖНО: Если ранняя проверка уже нашла ответ, не перезаписываем has_response
                        if not has_response:
                            full_text = review_elem.get_text(separator=' ', strip=True)
                            full_text_lower = full_text.lower()
                            # Проверяем различные варианты обозначения ответа компании
                            response_indicators = [
                                'официальный ответ',
                                'ответ компании',
                                'ответ организации',
                                'ответ от компании',
                            ]
                            for indicator in response_indicators:
                                if indicator in full_text_lower:
                                    has_response = True
                                    # Пытаемся вытащить дату ответа из фрагмента "DD месяц YYYY, официальный ответ"
                                    m_resp = re.search(
                                        r'(\d{1,2}\s+[а-яё]+\s+\d{4}).{0,40}' + re.escape(indicator),
                                        full_text_lower,
                                        re.IGNORECASE,
                                    )
                                    if m_resp:
                                        try:
                                            response_date = parse_russian_date(m_resp.group(1))
                                        except Exception:
                                            response_date = None
                                    # Если нашли ответ, но не нашли дату, пробуем извлечь response_text
                                    if not response_text:
                                        # Ищем текст ответа после индикатора
                                        response_match = re.search(
                                            r'(?:' + re.escape(indicator) + r'[:\s]*)(.+?)(?:\n|$)',
                                            full_text,
                                            re.IGNORECASE | re.DOTALL
                                        )
                                        if response_match:
                                            response_text = response_match.group(1).strip()
                                    break

                        # Логируем информацию о каждом обработанном элементе (до проверки дубликатов)
                        if processed_count <= 20:  # Логируем первые 20 для отладки
                            logger.debug(
                                f"Processing review element {processed_count}: "
                                f"author={author_name}, rating={rating_value}, "
                                f"text_len={len(review_text)}, has_text={bool(review_text)}, "
                                f"date={date_text}"
                            )

                        # ОТКЛЮЧЕНО: Проверка на дубликаты отключена для сбора всех отзывов
                        # Это позволяет собирать все отзывы, включая возможные дубликаты,
                        # чтобы получить точное количество отвеченных отзывов из структуры страницы
                        # Уникальный ключ отзыва (оставляем для возможной будущей статистики)
                        review_key = f"{author_name}_{date_text}_{rating_value}_" \
                            f"{hashlib.md5(review_text[:50].encode('utf-8', errors='ignore')).hexdigest()[:10]}"
                        # ПРОВЕРКА НА ДУБЛИКАТЫ ОТКЛЮЧЕНА - сохраняем все отзывы
                        # if review_key in seen_review_keys:
                        #     if processed_count <= 20 or skipped_count % 10 == 0:
                        #         logger.debug(f"Skipping duplicate review: key={review_key[:50]}")
                        #     skipped_count += 1
                        #     continue
                        # seen_review_keys.add(review_key)

                        # Очищаем текст отзыва от лишних элементов
                        if review_text:
                            # Убираем информацию об авторе и количестве отзывов в начале текста
                            # Паттерны типа "МБ Максим Балышев 2 отзыва" или "Алексей 2 отзыва" или "username 5 отзывов"
                            # Убираем невидимые символы и имя автора с количеством отзывов в начале
                            
                            # Паттерн 1: Инициалы + полное имя + количество отзывов (например: "МБ Максим Балышев 2 отзыва")
                            review_text = re.sub(
                                r'^[\s\u200b\u200c\u200d\u2060\u00a0]*[А-ЯЁA-Z]{1,3}\s+[А-ЯЁA-Z][а-яёa-z]+\s+[А-ЯЁA-Z][а-яёa-z]+\s*[\s\u200b\u200c\u200d\u2060\u00a0]*\d+\s*[\s\u200b\u200c\u200d\u2060\u00a0]*отзыв[аов]*\s*',
                                '',
                                review_text,
                                flags=re.IGNORECASE
                            )
                            # Паттерн 2: Полное имя + количество отзывов (например: "Алексей Иванов 2 отзыва")
                            review_text = re.sub(
                                r'^[\s\u200b\u200c\u200d\u2060\u00a0]*[А-ЯЁA-Z][а-яёa-z]+\s+[А-ЯЁA-Z][а-яёa-z]+\s*[\s\u200b\u200c\u200d\u2060\u00a0]*\d+\s*[\s\u200b\u200c\u200d\u2060\u00a0]*отзыв[аов]*\s*',
                                '',
                                review_text,
                                flags=re.IGNORECASE
                            )
                            # Паттерн 3: Одиночное имя или username + количество отзывов (например: "Алексей 2 отзыва" или "username 5 отзывов")
                            review_text = re.sub(
                                r'^[\s\u200b\u200c\u200d\u2060\u00a0]*[a-zA-Zа-яёА-ЯЁ0-9_\-]+\s*[\s\u200b\u200c\u200d\u2060\u00a0]*\d+\s*[\s\u200b\u200c\u200d\u2060\u00a0]*отзыв[аов]*\s*',
                                '',
                                review_text,
                                flags=re.IGNORECASE
                            )
                            # Паттерн 4: Инициалы + количество отзывов (например: "МБ 2 отзыва")
                            review_text = re.sub(
                                r'^[\s\u200b\u200c\u200d\u2060\u00a0]*[А-ЯЁA-Z]{1,3}\s*[\s\u200b\u200c\u200d\u2060\u00a0]*\d+\s*[\s\u200b\u200c\u200d\u2060\u00a0]*отзыв[аов]*\s*',
                                '',
                                review_text,
                                flags=re.IGNORECASE
                            )
                            
                            # Убираем служебные тексты в конце: "Полезно?", "Полезно", "Подписаться"
                            review_text = re.sub(
                                r'\s*(Полезно\??|полезно\??|Подписаться|подписаться)\s*$',
                                '',
                                review_text,
                                flags=re.IGNORECASE
                            )
                            
                            # Убираем служебные тексты в начале
                            review_text = re.sub(
                                r'^\s*(Полезно\??|полезно\??|Подписаться|подписаться)\s+',
                                '',
                                review_text,
                                flags=re.IGNORECASE
                            )
                            
                            # Убираем чисто служебные тексты (если весь текст состоит только из них)
                            rt_lower = review_text.strip().lower()
                            service_texts = {
                                'подписаться',
                                'полезно?',
                                'полезно',
                                'подписаться полезно?',
                            }
                            if rt_lower in service_texts or ('подписаться' in rt_lower and len(rt_lower) <= 20):
                                review_text = ""
                            
                            # Очищаем от лишних пробелов
                            review_text = ' '.join(review_text.split()).strip()
                        
                        # Проверяем, является ли элемент ответом компании (а не отзывом пользователя)
                        # Признаки ответа компании:
                        # 1. Текст начинается с типичных фраз ответов
                        # 2. Содержит упоминания компании/поддержки
                        # 3. Нет рейтинга ИЛИ рейтинг есть, но текст явно является ответом
                        review_text_lower = (review_text or "").lower()
                        response_text_lower = (response_text or "").lower()
                        full_text_lower = (review_elem.get_text(separator=' ', strip=True) or "").lower()
                        
                        # Типичные фразы начала ответов компании
                        company_response_phrases = [
                            'спасибо за ваш',
                            'спасибо за ваш отзыв',
                            'благодарим вас',
                            'благодарим',
                            'добрый день',
                            'здравствуйте',
                            'наша команда',
                            'наша техническая поддержка',
                            'обращайтесь по телефону',
                            'мы рады',
                            'мы стараемся',
                            'наша поддержка работает',
                            'наша техническая поддержка работает',
                            'наша поддержка',
                        ]
                        
                        # Проверяем, является ли это ответом компании
                        # ВАЖНО: проверяем только начало текста, чтобы не ловить обычные отзывы
                        # Ужесточаем проверку для более точного определения ответов компании
                        is_company_response = False
                        
                        # Проверка 1: Если автор совпадает с названием компании или содержит "официальный ответ"
                        # Получаем название компании из карточки, если доступно
                        card_name = ""
                        try:
                            # Пытаемся найти название компании на странице
                            card_name_elem = soup_content.select_one('h1, [class*="name"], [class*="title"]')
                            if card_name_elem:
                                card_name = card_name_elem.get_text(strip=True)
                        except Exception:
                            pass
                        
                        # Если автор совпадает с названием компании или содержит его - это ответ компании
                        # Улучшенная проверка: учитываем, что автор может быть "Победа, агентство", а название "Победа"
                        # ВАЖНО: не пропускаем отзывы с рейтингом, если текст не похож на ответ компании
                        if card_name and author_name:
                            author_lower = author_name.lower().strip()
                            card_name_lower = card_name.lower().strip()
                            # Извлекаем основное слово из названия компании (до запятой или первого слова)
                            card_main_word = card_name_lower.split(',')[0].strip().split()[0] if card_name_lower else ""
                            
                            # Проверяем, содержит ли автор название компании или основное слово
                            author_matches_company = False
                            if card_main_word and len(card_main_word) > 2:
                                if card_main_word in author_lower:
                                    author_matches_company = True
                            # Также проверяем полное совпадение или обратное включение
                            if (card_name_lower in author_lower or author_lower in card_name_lower) and len(card_name_lower) > 3:
                                author_matches_company = True
                            
                            # Если автор совпадает с названием компании, но у отзыва есть рейтинг и текст не похож на ответ компании,
                            # то это может быть отзыв пользователя (например, "Победа оправдала ожидания")
                            if author_matches_company:
                                # Проверяем, не является ли это отзывом пользователя с рейтингом
                                if rating_value > 0 and review_text_lower:
                                    # Если текст начинается с названия компании в кавычках или содержит "оправдала", "работали" и т.д. - это отзыв пользователя
                                    review_start = review_text_lower[:100].strip()
                                    user_review_indicators = [
                                        'оправдала', 'оправдали', 'работали', 'работаем', 'обратились', 'обращались',
                                        'заказали', 'заказывали', 'рекомендую', 'рекомендуем', 'довольны', 'доволен',
                                        'нравится', 'понравилось', 'хорошо', 'отлично', 'качественно'
                                    ]
                                    # Если текст содержит индикаторы отзыва пользователя - не пропускаем
                                    if any(indicator in review_start for indicator in user_review_indicators):
                                        logger.info(f"Author matches company but text indicates user review: author='{author_name}', text_preview={review_text[:50]}")
                                        author_matches_company = False
                                
                                # Если автор содержит "агентство", "компания" или подобное - это точно ответ компании
                                if 'агентство' in author_lower or 'компания' in author_lower or 'организация' in author_lower:
                                    is_company_response = True
                                    logger.info(f"Author contains company type: author='{author_name}', company='{card_name}'")
                                elif author_matches_company:
                                    is_company_response = True
                                    logger.info(f"Author matches company name: author='{author_name}', company='{card_name}'")
                        
                        # Проверка 2: Если в тексте элемента есть "официальный ответ" - это ответ компании
                        full_text_lower_check = (review_elem.get_text(separator=' ', strip=True) or "").lower()
                        if 'официальный ответ' in full_text_lower_check and rating_value == 0:
                            is_company_response = True
                            logger.info(f"Found 'официальный ответ' marker in review element")
                        
                        # Проверка 3: Проверяем текст отзыва на типичные фразы ответов компании
                        if review_text_lower:
                            # Проверяем только начало текста (первые 200 символов) для более точного определения
                            review_start = review_text_lower[:200].strip()
                            
                            # Сначала проверяем самые характерные фразы, которые точно указывают на ответ компании
                            # "Спасибо за ваш [положительный/отрицательный] отзыв" - типичное начало ответа
                            if review_start.startswith('спасибо за ваш отзыв') or review_start.startswith('благодарим вас за'):
                                is_company_response = True
                            elif review_start.startswith('спасибо за ваш') and ('положительный' in review_start[:60] or 'отрицательный' in review_start[:60] or 'отзыв' in review_start[:60]):
                                is_company_response = True
                            elif review_start.startswith('благодарим вас') or review_start.startswith('благодарим за'):
                                is_company_response = True
                            elif review_start.startswith('благодарим') and len(review_text_lower) < 100:  # Короткие благодарности - скорее всего ответ компании
                                is_company_response = True
                            # Проверяем "Добрый день" / "Здравствуйте" в начале + характерные слова компании
                            elif (review_start.startswith('добрый день') or review_start.startswith('здравствуйте')) and any(word in review_start[:150] for word in ['наша', 'поддержка', 'обращайтесь', 'рады', 'стараемся', 'команда', 'техническая']):
                                is_company_response = True
                            # Проверяем фразы, которые точно указывают на ответ компании
                            elif any(phrase in review_start[:100] for phrase in ['наша техническая поддержка', 'обращайтесь по телефону', 'наша команда', 'мы рады', 'мы стараемся', 'напишите на', 'позвоните по']):
                                is_company_response = True
                        
                        # Пропускаем ответы компании - они не являются отзывами пользователей
                        # ВАЖНО: используем has_official_marker_in_answer, который проверяется только в блоке ответа
                        # Потому что в элементе отзыва может быть и отзыв пользователя, и ответ компании
                        
                        # ВАЖНО: ответы компании НЕ должны попадать в список отзывов, даже если у них есть рейтинг
                        # Если это ответ компании (по любой проверке) - пропускаем БЕЗ ИСКЛЮЧЕНИЙ
                        if is_company_response or has_official_marker_in_answer:
                            logger.info(f"Skipping company response: author={author_name}, text_preview={review_text[:100] if review_text else 'N/A'}, has_response={has_response}, rating={rating_value}, is_company_response={is_company_response}, has_official_marker={has_official_marker_in_answer}")
                            skipped_count += 1
                            continue
                        
                        # Дополнительная проверка: если автор совпадает с названием компании И текст начинается с типичных фраз ответа
                        # это точно ответ компании, даже если есть рейтинг
                        if card_name and author_name:
                            author_lower = author_name.lower().strip()
                            card_name_lower = card_name.lower().strip()
                            if card_name_lower in author_lower or author_lower in card_name_lower:
                                if review_text_lower:
                                    review_start = review_text_lower[:150].strip()
                                    if any(phrase in review_start for phrase in ['спасибо за ваш', 'благодарим вас', 'благодарим за', 'добрый день', 'здравствуйте', 'наша команда', 'наша поддержка']):
                                        logger.info(f"Skipping company response: author matches company and text starts with company response phrase: author={author_name}, text_preview={review_text[:100]}")
                                        skipped_count += 1
                                        continue
                        
                        # Проверяем, является ли это валидным отзывом с ответом
                        is_valid_review_with_response = has_response and (response_text or response_date)
                        
                        # Пропускаем только чистые ответы компании БЕЗ отзыва пользователя
                        if has_response and rating_value == 0 and (not review_text or len(review_text.strip()) < 10):
                            logger.debug(f"Skipping response without rating and text: author={author_name}, text_len={len(review_text) if review_text else 0}")
                            skipped_count += 1
                            continue
                        
                        # Дополнительная проверка: если элемент имеет has_response=True, но нет рейтинга и короткий текст,
                        # это скорее всего ответ компании, а не отзыв пользователя
                        if has_response and rating_value == 0 and review_text and len(review_text.strip()) < 20:
                            # Проверяем, не начинается ли текст с типичных фраз ответа компании
                            review_start_lower = review_text[:100].lower().strip()
                            if any(phrase in review_start_lower for phrase in ['спасибо', 'благодарим', 'добрый день', 'здравствуйте', 'наша', 'команда', 'поддержка']):
                                logger.info(f"Skipping likely company response: short text with has_response=True, text_preview={review_text[:100]}")
                                skipped_count += 1
                                continue
                        
                        # Пропускаем только элементы без рейтинга И без текста И без ответа
                        # Принимаем отзывы с рейтингом ИЛИ с текстом ИЛИ с ответом
                        if rating_value == 0 and (not review_text or len(review_text.strip()) < 3) and not is_valid_review_with_response:
                            if processed_count <= 20 or skipped_count % 10 == 0:
                                logger.debug(f"Skipping element without rating, text, and response: author={author_name}, has_response={has_response}")
                            skipped_count += 1
                            continue
                        
                        # Принимаем элементы с рейтингом ИЛИ с текстом (минимум 3 символа) ИЛИ с ответом
                        if rating_value > 0 or (review_text and len(review_text.strip()) >= 3) or is_valid_review_with_response:
                            # review_id уже извлечен выше в начале парсинга (из атрибутов, ссылок и т.д.)
                            # ПРИОРИТЕТ 4: Если не найден, генерируем на основе содержимого (хэш)
                            # ТЕПЕРЬ author_name и date_text уже определены, можно использовать для генерации
                            if not review_id:
                                # Используем хэш от автора, даты и начала текста для уникальности
                                review_content_hash = hashlib.md5(
                                    f"{author_name}_{date_text}_{review_text[:50] if review_text else review_elem.get_text(strip=True)[:50]}".encode('utf-8')
                                ).hexdigest()[:16]
                                review_id = f"hash_{review_content_hash}"
                            
                            # ВАЖНО: Убеждаемся, что review_id установлен (для использования в API)
                            if not review_id:
                                # Финальный fallback: генерируем на основе всего элемента
                                review_content_hash = hashlib.md5(
                                    f"{review_elem.get_text(strip=True)[:100]}".encode('utf-8')
                                ).hexdigest()[:16]
                                review_id = f"hash_{review_content_hash}"
                            
                            review_data = {
                                    'review_id': review_id,  # ID отзыва для использования в API
                                    'review_rating': rating_value,
                                    'review_text': review_text or "",
                                    'review_author': author_name or "Аноним",
                                    'review_date': format_russian_date(review_date)
                                    if review_date
                                    else (date_text or ""),
                                    'review_date_datetime': review_date,  # Сохраняем исходный datetime объект для вычисления времени ответа
                                    'has_response': has_response,
                                    'response_id': response_id if has_response else None,  # ID ответа для использования в API (только если есть ответ)
                                    'response_text': response_text,
                                    'response_date': format_russian_date(response_date)
                                    if response_date
                                    else "",
                                    'response_date_datetime': response_date,  # Сохраняем исходный datetime объект для вычисления времени ответа
                                }
                            all_reviews.append(review_data)
                            
                            # Обновляем прогресс после добавления отзыва (каждые 3 отзыва для более частого обновления)
                            if len(all_reviews) % 3 == 0 or len(all_reviews) == 1:
                                total_expected = reviews_count_total if reviews_count_total > 0 else 0
                                total_processed = len(all_reviews)
                                if total_expected > 0:
                                    self._update_progress(f"Парсинг отзывов: обработано {total_processed} из {total_expected}")
                                else:
                                    self._update_progress(f"Парсинг отзывов: обработано {total_processed} отзывов")

                            # Классификация, как и для Яндекса:
                            # 1–2★ — негатив, 3★ — нейтрально, 4–5★ — позитив.
                            # ВАЖНО: классифицируем ТОЛЬКО отзывы с рейтингом > 0
                            if rating_value > 0:
                                if rating_value >= 4:
                                    reviews_info['positive_reviews'] += 1
                                elif rating_value == 3:
                                    reviews_info['neutral_reviews'] += 1
                                elif rating_value in (1, 2):
                                    reviews_info['negative_reviews'] += 1

                            if has_response:
                                reviews_info['answered_count'] += 1
                            else:
                                reviews_info['unanswered_count'] += 1

                            # Шаг 3: Извлечение данных и расчет внутри цикла парсинга (по предложенному плану)
                            # Накапливаем время ответа для расчёта среднего
                            # ВАЖНО: вычисляем только для отзывов пользователей с ответами компании
                            # Используем timedelta для более точного расчета
                            if has_response and review_date and response_date:
                                try:
                                    # Сохраняем данные о датах в отдельный список для последующего расчета
                                    if hasattr(self, '_response_dates_data'):
                                        self._response_dates_data.append({
                                            'card_url': current_card_url,
                                            'review_date': review_date.isoformat() if isinstance(review_date, dt_module.datetime) else str(review_date),
                                            'response_date': response_date.isoformat() if isinstance(response_date, dt_module.datetime) else str(response_date),
                                        })
                                    
                                    # Вычисляем разницу между датой ответа и датой отзыва (timedelta)
                                    time_difference = response_date - review_date
                                    
                                    # Проверяем, что ответ не пришел раньше отзыва (проверка на логику)
                                    if time_difference >= timedelta(0):
                                        # Накапливаем данные в глобальных переменных (Шаг 1)
                                        total_response_time += time_difference
                                        count_with_replies += 1
                                        
                                        # Также сохраняем в старых переменных для совместимости (в днях)
                                        delta_days = time_difference.days
                                        response_time_sum_days += float(delta_days)
                                        response_time_count += 1
                                        
                                        logger.debug(f"Added response time: {time_difference} ({delta_days} days) (review: {review_date}, response: {response_date})")
                                    else:
                                        logger.warning(f"Negative time difference detected during parsing: {time_difference} (review: {review_date}, response: {response_date})")
                                        logger.warning(f"  This review will be excluded from average response time calculation")
                                except Exception as e:
                                    logger.debug(f"Error calculating response time: {e}")
                                    pass

                            if review_text:
                                reviews_info['texts'].append(review_text)

                            logger.debug(
                                f"Added review: author={review_data['review_author']}, "
                                f"rating={rating_value}, text_len={len(review_text)}, "
                                f"date={review_data['review_date']}"
                            )
                        else:
                            # Детальное логирование причин пропуска элементов
                            skip_reason = []
                            if not review_text or len(review_text.strip()) < 3:
                                skip_reason.append(f"no_text(len={len(review_text) if review_text else 0})")
                            if rating_value == 0:
                                skip_reason.append("no_rating")
                            if not author_name or author_name == "Аноним":
                                skip_reason.append("no_author")
                            
                            if processed_count <= 20 or skipped_count % 10 == 0:  # Логируем первые 20 и каждые 10 пропусков
                                logger.debug(
                                    f"Skipped review element #{processed_count}: {', '.join(skip_reason) if skip_reason else 'unknown reason'}"
                                )
                            skipped_count += 1

                    # Логируем статистику по обработанной странице
                    added_reviews = len([r for r in all_reviews if r.get('review_author')])
                    logger.info(
                        f"Page {page_url}: found {len(review_elements)} elements, "
                        f"processed {processed_count}, skipped {skipped_count}, "
                        f"added {added_reviews} reviews"
                    )
                    
                    # Обновляем прогресс после обработки страницы
                    total_expected = reviews_count_total if reviews_count_total > 0 else 0
                    total_processed = len(all_reviews)
                    if total_expected > 0:
                        self._update_progress(f"Парсинг отзывов: обработано {total_processed} из {total_expected}")
                    else:
                        self._update_progress(f"Парсинг отзывов: обработано {total_processed} отзывов")
                    
                    if skipped_count > processed_count * 0.5:  # Если пропущено больше 50% элементов
                        logger.warning(
                            f"High skip rate on page {page_url}: {skipped_count}/{len(review_elements)} elements skipped. "
                            f"This might indicate overly aggressive filtering."
                        )
                    
                except Exception as page_error:
                    logger.error(
                        f"Error processing 2GIS reviews page {page_url}: {page_error}",
                        exc_info=True,
                    )
                    # Сохраняем частичные результаты даже при ошибке
                    logger.info(f"Continuing with partial results: {len(all_reviews)} reviews collected so far")
                    continue

            # ВАЖНО: используем reviews_count_total из структуры страницы для агрегированной информации
            # Это точное значение, которое отображается на странице карточки
            # А len(all_reviews) - это фактическое количество распарсенных отзывов (может быть меньше из-за фильтрации)
            if reviews_count_total > 0:
                reviews_info['reviews_count'] = reviews_count_total
                logger.info(f"Using reviews count from card page structure: {reviews_count_total} (parsed reviews: {len(all_reviews)})")
            else:
                reviews_info['reviews_count'] = len(all_reviews)
                logger.info(f"Using parsed reviews count: {len(all_reviews)} (no structure count found)")
            
            # Сохраняем количество оценок из структуры страницы
            if ratings_count_total > 0:
                reviews_info['ratings_count'] = ratings_count_total
                logger.info(f"Using ratings count from card page structure: {ratings_count_total}")
            else:
                reviews_info['ratings_count'] = 0
            
            # Разделяем отзывы на блоки: позитивные, негативные, нейтральные, с ответом
            positive_reviews_list = [r for r in all_reviews if r.get('review_rating', 0) >= 4]
            negative_reviews_list = [r for r in all_reviews if r.get('review_rating', 0) in (1, 2)]
            neutral_reviews_list = [r for r in all_reviews if r.get('review_rating', 0) == 3]
            answered_reviews_list = [r for r in all_reviews if r.get('has_response', False)]
            
            logger.info(f"Reviews grouped: positive={len(positive_reviews_list)}, negative={len(negative_reviews_list)}, neutral={len(neutral_reviews_list)}, answered={len(answered_reviews_list)}")
            
            # ВАЖНО: вычисляем среднее время ответа ТОЛЬКО для блока "С ответом"
            # Используем исходные datetime объекты из review_data, если они есть
            answered_response_time_sum = 0.0
            answered_response_time_count = 0
            for review in answered_reviews_list:
                if review.get('has_response', False):
                    # ПРИОРИТЕТ 1: Используем исходные datetime объекты, если они сохранены в review
                    review_date_parsed = review.get('review_date_datetime')
                    response_date_parsed = review.get('response_date_datetime')
                    
                    # ПРИОРИТЕТ 2: Если datetime объекты не сохранены, парсим из строк
                    if not review_date_parsed or not response_date_parsed:
                        review_date_str = review.get('review_date', '')
                        response_date_str = review.get('response_date', '')
                        if review_date_str and response_date_str:
                            try:
                                review_date_parsed = parse_russian_date(review_date_str)
                                response_date_parsed = parse_russian_date(response_date_str)
                            except Exception as e:
                                logger.debug(f"Error parsing dates for response time: {e}")
                                continue
                    
                    if review_date_parsed and response_date_parsed:
                        delta = (response_date_parsed - review_date_parsed).days
                        if delta >= 0 and delta < 3650:  # Разумные пределы: не более 10 лет
                            answered_response_time_sum += float(delta)
                            answered_response_time_count += 1
                            logger.info(f"Added response time for answered review: {delta} days (review_date={review.get('review_date', 'N/A')}, response_date={review.get('response_date', 'N/A')})")
                        elif delta < 0:
                            logger.warning(f"Negative response time delta: {delta} days (review_date={review.get('review_date', 'N/A')}, response_date={review.get('response_date', 'N/A')}, review_datetime={review.get('review_date_datetime', 'N/A')}, response_datetime={review.get('response_date_datetime', 'N/A')})")
                            # Логируем детали для отладки
                            if review.get('review_date_datetime') and review.get('response_date_datetime'):
                                logger.warning(f"  DEBUG: review_date_datetime={review.get('review_date_datetime')}, response_date_datetime={review.get('response_date_datetime')}")
                        elif delta >= 3650:
                            logger.warning(f"Unrealistic response time delta: {delta} days (review_date={review.get('review_date', 'N/A')}, response_date={review.get('response_date', 'N/A')})")
            
            # Шаг 4: Финальный расчет среднего времени ответа (после завершения цикла парсинга)
            # Используем данные, накопленные в цикле (total_response_time и count_with_replies)
            logger.info("\n--- АГРЕГИРОВАННЫЕ ДАННЫЕ ДЛЯ РАСЧЕТА СРЕДНЕГО ВРЕМЕНИ ОТВЕТА ---")
            
            if count_with_replies > 0:
                # Среднее время (получится объект timedelta)
                average_time = total_response_time / count_with_replies
                # Конвертируем в дни для совместимости
                avg_response_time_days = average_time.total_seconds() / 86400.0
                
                logger.info(f"Количество отзывов с ответом: {count_with_replies}")
                logger.info(f"Общее накопленное время ожидания: {total_response_time} ({total_response_time.total_seconds() / 86400.0:.1f} дней)")
                logger.info(f"СРЕДНЕЕ ВРЕМЯ ОТВЕТА: {average_time} ({avg_response_time_days:.1f} дней)")
                
                reviews_info['avg_response_time_days'] = round(avg_response_time_days, 1)
            else:
                logger.info("Ответов организации не найдено для расчета среднего времени.")
                # Fallback: используем расчет из answered_reviews_list, если основной расчет не дал результатов
                if answered_response_time_count > 0:
                    avg_response_time_answered = round(answered_response_time_sum / answered_response_time_count, 1)
                    reviews_info['avg_response_time_days'] = avg_response_time_answered
                    logger.info(f"Using fallback calculation: {avg_response_time_answered} days (from {answered_response_time_count} answered reviews)")
                else:
                    reviews_info['avg_response_time_days'] = 0.0
                    logger.info("No response time data available for answered reviews block")
            
            # ВАЖНО: используем количество отвеченных отзывов из структуры страницы для агрегированной информации
            # Это точное значение, которое отображается на странице карточки
            parsed_answered_count = len(answered_reviews_list)
            if answered_reviews_count > 0:
                reviews_info['answered_count'] = answered_reviews_count
                logger.info(f"Using answered reviews count from card page structure: {answered_reviews_count} (parsed answered: {parsed_answered_count})")
            # Если не нашли в структуре, используем фактическое количество из парсинга
            elif parsed_answered_count > 0:
                reviews_info['answered_count'] = parsed_answered_count
                logger.info(f"Using parsed answered reviews count: {parsed_answered_count} (no structure count found)")
            
            # Сохраняем рейтинг карточки из структуры страницы
            reviews_info['card_rating_from_page'] = card_rating_from_page
            
            # Сохраняем отзывы разделенными по блокам
            reviews_info['details'] = all_reviews  # Все отзывы
            reviews_info['positive_reviews_list'] = positive_reviews_list  # Блок позитивных
            reviews_info['negative_reviews_list'] = negative_reviews_list  # Блок негативных
            reviews_info['neutral_reviews_list'] = neutral_reviews_list  # Блок нейтральных
            reviews_info['answered_reviews_list'] = answered_reviews_list  # Блок с ответом
            
            # Логируем статистику по найденным отзывам
            reviews_with_rating = sum(1 for r in all_reviews if r.get('review_rating', 0) > 0)
            reviews_with_text = sum(1 for r in all_reviews if r.get('review_text', '').strip())
            logger.info(
                f"2GIS reviews extraction completed: total={len(all_reviews)}, "
                f"with_rating={reviews_with_rating}, with_text={reviews_with_text}, "
                f"positive={len(positive_reviews_list)}, "
                f"negative={len(negative_reviews_list)}, "
                f"answered={len(answered_reviews_list)}"
            )

            # -------------------------------------------------------------
            # Вариант B2: если по звёздам не удалось посчитать
            # positive/negative, пробуем оценить их количеством
            # отзывов под фильтрами "Положительные"/"Отрицательные".
            # -------------------------------------------------------------
            try:
                def _click_filter(filter_text: str) -> bool:
                    script = """
                        var txt = arguments[0];
                        var candidates = document.querySelectorAll('li, button, div, span');
                        for (var i = 0; i < candidates.length; i++) {
                            var el = candidates[i];
                            if (!el || !el.innerText) continue;
                            var t = el.innerText.trim();
                            if (t === txt) {
                                el.click();
                                return true;
                            }
                        }
                        return false;
                    """
                    try:
                        return bool(self.driver.execute_script(script, filter_text))
                    except Exception:
                        return False

                def _count_current_reviews() -> int:
                    # НЕ вызываем _scroll_to_load_all_reviews здесь, чтобы избежать рекурсии
                    # Просто считаем текущие отзывы на странице
                    time.sleep(0.5)  # Небольшая пауза для загрузки
                    page_source_local, soup_local = self._get_page_source_and_soup()
                    elems_local = soup_local.select("div._1k5soqfl")
                    if not elems_local:
                        elems_local = soup_local.select(
                            '[class*="review"], li[class*="review"], div[class*="review"]'
                        )
                    return len(elems_local)

                # Если по карточке мы так и не нашли ни одного отзыва
                # с рейтингом, пробуем приблизительно оценить
                # позитив/негатив через встроенные фильтры 2ГИС.
                if reviews_info['positive_reviews'] == 0 and reviews_info['negative_reviews'] == 0:
                    # Сначала пытаемся переключиться на "Положительные"
                    if _click_filter("Положительные"):
                        pos_cnt = _count_current_reviews()
                        if pos_cnt >= 0:
                            reviews_info['positive_reviews'] = pos_cnt

                    # Затем на "Отрицательные"
                    if _click_filter("Отрицательные"):
                        neg_cnt = _count_current_reviews()
                        if neg_cnt >= 0:
                            reviews_info['negative_reviews'] = neg_cnt
            except Exception as e:
                logger.warning(
                    f"2GIS: could not apply filters for positive/negative reviews: {e}",
                    exc_info=True,
                )

            return reviews_info

        except Exception as e:
            logger.warning(
                f"Error getting reviews info for 2GIS card: {e}", exc_info=True
            )
            return reviews_info

    def _scroll_to_load_all_reviews(self, expected_count: int = 0) -> None:
        """
        Прокручивает страницу отзывов 2GIS для загрузки всех отзывов.
        Использует те же селекторы, что и при извлечении отзывов.
        
        Args:
            expected_count: Ожидаемое количество отзывов (для оптимизации прокрутки)
        """
        try:
            import time as time_module
            start_time = time_module.time()
            max_scroll_time = 600  # Увеличено до 10 минут для загрузки всех отзывов
            scroll_iterations = 0
            max_scrolls = self._reviews_scroll_iterations_max
            no_change_count = 0
            required_no_change = 25  # Увеличено до 25 для более стабильной остановки (продолжаем прокрутку дольше)
            last_review_count = 0
            max_no_change_iterations = 50  # Увеличено до 50 - продолжаем прокрутку, пока не перестанут появляться новые
            button_click_failures = 0  # Счетчик неудачных попыток клика на кнопку
            max_button_click_failures = 3  # Максимум неудачных попыток клика
            
            # Селекторы должны совпадать с теми, что используются при извлечении отзывов
            review_selectors = [
                "div._1k5soqfl",  # Основной класс для отзывов в 2GIS
                '[data-review-id]',
                '[class*="review-item"]',
                'div[class*="review"]',
                'div[class*="_4db12d"]',  # Альтернативный класс
                'li[class*="review"]',
            ]
                
            logger.info(f"Starting scroll to load all reviews... (expected: {expected_count if expected_count > 0 else 'unknown'})")
            
            # Если знаем ожидаемое количество отзывов, используем его как ориентир
            target_reviews = expected_count if expected_count > 0 else None
            
            while scroll_iterations < max_scrolls:
                # Проверяем таймаут
                elapsed_time = time_module.time() - start_time
                if elapsed_time > max_scroll_time:
                    logger.warning(f"Scroll timeout after {elapsed_time:.1f}s, stopping")
                    break
                
                if self._is_stopped():
                    logger.info("2GIS reviews scroll: stop flag detected, breaking scroll loop")
                    break
                
                # Получаем текущее количество отзывов
                page_source, soup = self._get_page_source_and_soup()
                current_review_count = 0
                for selector in review_selectors:
                    found = soup.select(selector)
                    # Фильтруем элементы, которые явно не являются отзывами
                    # Используем ту же минимальную длину (3 символа), что и при обработке
                    valid_reviews = [
                        elem for elem in found 
                        if elem.get_text(strip=True) and len(elem.get_text(strip=True)) >= 3
                        and 'читать целиком' not in elem.get_text(strip=True).lower()
                    ]
                    current_review_count = max(current_review_count, len(valid_reviews))
                
                # Детальное логирование процесса прокрутки
                elapsed_time = time_module.time() - start_time
                logger.info(
                    f"Scroll iteration {scroll_iterations + 1}/{max_scrolls}: "
                    f"found {current_review_count} reviews "
                    f"(target: {target_reviews if target_reviews else 'unknown'}, "
                    f"no_change: {no_change_count}/{required_no_change}, "
                    f"elapsed: {elapsed_time:.1f}s)"
                )
                
                # Проверяем, увеличилось ли количество отзывов
                if current_review_count > last_review_count:
                    last_review_count = current_review_count
                    no_change_count = 0  # Сбрасываем счетчик при появлении новых отзывов
                    logger.info(f"New reviews found! Total: {current_review_count}" + (f" / {target_reviews}" if target_reviews else ""))
                    
                    # Если достигли целевого количества отзывов, продолжаем еще немного для уверенности
                    # Увеличено до 98% для более полной загрузки
                    if target_reviews and current_review_count >= target_reviews * 0.98:  # 98% от целевого
                        logger.info(f"Reached {current_review_count} reviews (98%+ of target {target_reviews}), continuing to load remaining...")
                else:
                    no_change_count += 1
                    logger.debug(f"Review count unchanged: {current_review_count} (no_change: {no_change_count}/{required_no_change})")
                    
                    # Если знаем целевое количество и еще не достигли его, продолжаем прокрутку
                    # Увеличиваем required_no_change для целевого количества, чтобы не останавливаться преждевременно
                    if target_reviews and current_review_count < target_reviews:
                        logger.debug(f"Review count {current_review_count} < target {target_reviews}, continuing scroll... (no_change: {no_change_count})")
                        # Не останавливаемся, продолжаем прокрутку даже если количество не меняется
                        # Останавливаемся только если достигли целевого количества или превысили таймаут
                    # Останавливаемся только если:
                    # 1. Достигли целевого количества
                    # 2. ИЛИ количество не меняется в течение required_no_change итераций И мы не знаем целевого количества
                    elif target_reviews and current_review_count >= target_reviews:
                        logger.info(f"Reached target reviews count: {current_review_count} >= {target_reviews}, stopping scroll")
                        break
                    elif not target_reviews and no_change_count >= required_no_change:
                        logger.info(f"Review count stable at {current_review_count} for {no_change_count} iterations, stopping scroll")
                        break
                
                # Прокручиваем страницу - пробуем разные способы
                try:
                    # Способ 1: Кликаем на "Читать целиком" для загрузки полного текста отзывов
                    try:
                        expand_reviews_script = """
                        var expandLinks = document.querySelectorAll('span._17ww69i, a[class*="читать"], [class*="читать целиком"]');
                        var clicked = 0;
                        for (var i = 0; i < Math.min(expandLinks.length, 10); i++) {
                            var link = expandLinks[i];
                            if (link.offsetParent !== null) {
                                try {
                                    link.click();
                                    clicked++;
                                } catch(e) {}
                            }
                        }
                        return clicked;
                        """
                        clicked_count = self.driver.execute_script(expand_reviews_script)
                        if clicked_count > 0:
                            logger.debug(f"Clicked 'read more' on {clicked_count} reviews")
                            time.sleep(2.5)  # Увеличено до 2.5 сек для загрузки полного текста
                    except Exception as click_error:
                        logger.debug(f"Could not click 'read more' links: {click_error}")
                    
                    # Способ 2: Ищем и кликаем кнопку "Показать еще" / "Загрузить еще"
                    # Проверяем наличие кнопки после каждой порции, не только при достижении 45 отзывов
                    # Это позволяет загружать отзывы более агрессивно
                    should_check_button = (
                        (current_review_count >= 20 and target_reviews and current_review_count < target_reviews) or
                        (current_review_count % 20 == 0)  # Проверяем каждые 20 отзывов
                    )
                    if should_check_button and button_click_failures < max_button_click_failures:
                        try:
                            # Сохраняем количество отзывов до клика
                            reviews_before_click = current_review_count
                            
                            # Более агрессивный поиск кнопок "Показать еще"
                            load_more_script = """
                            // Расширенный поиск кнопок загрузки
                            var selectors = [
                                'button[class*="load"]', 'button[class*="more"]', 'button[class*="show"]',
                                'a[class*="load"]', 'a[class*="more"]', 'a[class*="show"]',
                                'span[class*="load"]', 'span[class*="more"]', 'span[class*="show"]',
                                '[class*="показать"]', '[class*="загрузить"]', '[class*="еще"]',
                                'button', 'a', 'span[role="button"]', '[onclick*="load"]'
                            ];
                            
                            for (var s = 0; s < selectors.length; s++) {
                                var buttons = document.querySelectorAll(selectors[s]);
                                for (var i = 0; i < buttons.length; i++) {
                                    var btn = buttons[i];
                                    if (!btn || btn.offsetParent === null) continue;  // Пропускаем невидимые
                                    
                                    var text = (btn.textContent || btn.innerText || btn.getAttribute('aria-label') || '').toLowerCase();
                                    var hasRelevantText = (
                                        text.includes('показать') || text.includes('загрузить') || 
                                        text.includes('еще') || text.includes('more') || 
                                        text.includes('load') || text.includes('следующ') ||
                                        text.includes('далее') || text.includes('next')
                                    ) && !text.includes('читать');
                                    
                                    if (hasRelevantText) {
                                        try {
                                            // Пробуем несколько способов клика
                                            btn.scrollIntoView({behavior: 'smooth', block: 'center'});
                                            btn.focus();
                                            btn.click();
                                            return true;
                                        } catch(e1) {
                                            try {
                                                var event = new MouseEvent('click', {bubbles: true, cancelable: true});
                                                btn.dispatchEvent(event);
                                                return true;
                                            } catch(e2) {
                                                try {
                                                    var mouseDown = new MouseEvent('mousedown', {bubbles: true, cancelable: true});
                                                    var mouseUp = new MouseEvent('mouseup', {bubbles: true, cancelable: true});
                                                    btn.dispatchEvent(mouseDown);
                                                    btn.dispatchEvent(mouseUp);
                                                    return true;
                                                } catch(e3) {}
                                            }
                                        }
                                    }
                                }
                            }
                            return false;
                            """
                            clicked = self.driver.execute_script(load_more_script)
                            if clicked:
                                logger.info("Clicked 'load more' / 'next page' button to load more reviews")
                                time.sleep(3.5)  # Увеличено до 3.5 сек для загрузки новых отзывов после клика
                                
                                # Проверяем, увеличилось ли количество отзывов
                                page_source_after, soup_after = self._get_page_source_and_soup()
                                reviews_after_click = 0
                                for selector in review_selectors:
                                    found_after = soup_after.select(selector)
                                    valid_after = [
                                        elem for elem in found_after 
                                        if elem.get_text(strip=True) and len(elem.get_text(strip=True)) >= 3
                                        and 'читать целиком' not in elem.get_text(strip=True).lower()
                                    ]
                                    reviews_after_click = max(reviews_after_click, len(valid_after))
                                
                                if reviews_after_click <= reviews_before_click:
                                    button_click_failures += 1
                                    logger.warning(
                                        f"Clicked 'load more' but reviews count didn't increase "
                                        f"({reviews_before_click} -> {reviews_after_click}). "
                                        f"Failures: {button_click_failures}/{max_button_click_failures}"
                                    )
                                    if button_click_failures >= max_button_click_failures:
                                        logger.warning("Too many failed button clicks. Will stop clicking and use pagination instead.")
                                        # Увеличиваем no_change_count, чтобы прекратить дальнейшие клики
                                        no_change_count = max(no_change_count, 3)
                                else:
                                    # Успешный клик - сбрасываем счетчик
                                    button_click_failures = 0
                                    logger.info(f"Button click successful! Reviews increased: {reviews_before_click} -> {reviews_after_click}")
                        except Exception as click_error:
                            button_click_failures += 1
                            logger.debug(f"Could not click load more button: {click_error}. Failures: {button_click_failures}/{max_button_click_failures}")
                    
                    # Способ 3: Прокрутка контейнера с отзывами (если есть)
                    scroll_container_script = """
                    var containers = document.querySelectorAll('[class*="scroll"], [class*="reviews"], [class*="list"]');
                    for (var i = 0; i < containers.length; i++) {
                        var container = containers[i];
                        if (container.scrollHeight > container.clientHeight && container.scrollHeight > 500) {
                            container.scrollTop = container.scrollHeight;
                            return true;
                        }
                    }
                    return false;
                    """
                    has_scroll_container = self.driver.execute_script(scroll_container_script)
                    if has_scroll_container:
                        time.sleep(2.5)  # Увеличено до 2.5 сек для загрузки после прокрутки контейнера
                    
                    # Способ 4: Прокрутка всей страницы
                    self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                    time.sleep(2.5)  # Увеличено до 2.5 сек между порциями отзывов
                    
                    # Способ 5: Прокрутка на фиксированное расстояние
                    self.driver.execute_script("window.scrollBy(0, 1500);")
                    time.sleep(2.5)  # Увеличено до 2.5 сек для загрузки новых отзывов
                    
                except Exception as scroll_error:
                    logger.warning(f"Error during scroll: {scroll_error}")
                    # Продолжаем, даже если прокрутка не удалась
                
                scroll_iterations += 1
            
            elapsed_total = time_module.time() - start_time
            logger.info(
                f"Scroll completed after {scroll_iterations} iterations in {elapsed_total:.1f}s. "
                f"Found {last_review_count} reviews (target: {target_reviews if target_reviews else 'unknown'})"
            )
        except Exception as e:
            logger.warning(f"Error scrolling reviews: {e}", exc_info=True)

    def _update_aggregated_data(self, card_data: Dict[str, Any]) -> None:
        """
        Обновляем агрегированные счетчики по карточкам 2GIS.
        Логика максимально приближена к YandexParser._update_aggregated_data.
        """
        try:
            # Количество карточек
            self._aggregated_data['total_cards'] += 1

            # Рейтинг карточки
            rating_str = str(card_data.get('card_rating', '')).replace(',', '.').strip()
            try:
                card_rating_float = float(rating_str) if rating_str and rating_str.replace('.', '', 1).isdigit() else 0.0
            except (ValueError, TypeError):
                card_rating_float = 0.0

            self._aggregated_data['total_rating_sum'] += card_rating_float

            # Отзывы
            # ВАЖНО: используем card_reviews_count из snippet данных (если есть), это точное значение со страницы поиска
            reviews_count = card_data.get('card_reviews_count', 0) or 0
            # Если reviews_count = 0, но есть детальные отзывы, используем их количество
            if reviews_count == 0:
                detailed_reviews = card_data.get('detailed_reviews', [])
                if detailed_reviews:
                    reviews_count = len(detailed_reviews)
                    logger.info(f"Using detailed reviews count for aggregation (snippet was 0): {reviews_count} (card: {card_data.get('card_name', 'Unknown')})")
            
            positive_reviews = card_data.get('card_reviews_positive', 0) or 0
            negative_reviews = card_data.get('card_reviews_negative', 0) or 0
            answered_reviews = card_data.get('card_answered_reviews_count', 0) or 0

            # ВАЖНО: добавляем только если reviews_count > 0, чтобы не искажать статистику
            if reviews_count > 0:
                self._aggregated_data['total_reviews_count'] += reviews_count
                self._aggregated_data['total_positive_reviews'] += positive_reviews
                self._aggregated_data['total_negative_reviews'] += negative_reviews
                self._aggregated_data['total_answered_reviews_count'] += answered_reviews
                self._aggregated_data['total_unanswered_reviews_count'] += max(0, reviews_count - answered_reviews)

            if answered_reviews > 0:
                self._aggregated_data['total_answered_count'] += 1

            # Среднее время ответа по карточке (если когда‑нибудь появится для 2GIS)
            if card_data.get('card_avg_response_time'):
                try:
                    response_time_str = str(card_data['card_avg_response_time']).strip()
                    if response_time_str:
                        response_time_days = float(response_time_str)
                        if response_time_days > 0:
                            self._aggregated_data['total_response_time_sum_days'] += response_time_days
                            self._aggregated_data['total_response_time_calculated_count'] += 1
                except (ValueError, TypeError):
                    logger.warning(
                        f"Could not convert response time to float for card '{card_data.get('card_name', 'Unknown')}': "
                        f"{card_data.get('card_avg_response_time')}"
                    )

            logger.info(
                f"2GIS aggregated data updated for '{card_data.get('card_name', 'Unknown')}': "
                f"rating={card_rating_float}, reviews={reviews_count}, "
                f"positive={positive_reviews}, negative={negative_reviews}"
            )
        except Exception as e:
            logger.warning(
                f"Could not update aggregated data for card '{card_data.get('card_name', 'Unknown')}': {e}",
                exc_info=True
            )

    def _normalize_url_for_comparison(self, url: str) -> str:
        """Нормализует URL для сравнения"""
        if not url:
            return ""
        url = url.strip().lower()
        # Убираем протокол
        url = re.sub(r'^https?://', '', url)
        # Убираем www.
        url = re.sub(r'^www\.', '', url)
        # Убираем trailing slash
        url = url.rstrip('/')
        # Убираем путь и параметры (оставляем только домен)
        url = url.split('/')[0]
        url = url.split('?')[0]
        return url
    
    def _website_matches(self, card_website: str, target_website: str) -> bool:
        """
        Проверяет, соответствует ли сайт карточки целевому сайту.
        Нормализует оба URL для сравнения (убирает протокол, www, trailing slash).
        """
        if not card_website or not target_website:
            return False
        
        normalized_card = self._normalize_url_for_comparison(card_website)
        normalized_target = self._normalize_url_for_comparison(target_website)
        
        # Дополнительная проверка: если домены не совпадают точно, проверяем части домена
        # Например, smarthome.spb.ru может совпадать с www.smarthome.spb.ru
        if normalized_card == normalized_target:
            return True
        
        # Проверяем, содержит ли один домен другой (для поддоменов)
        # Например, если целевой сайт smarthome.spb.ru, а карточка имеет provayder.net - не совпадает
        # Но если карточка имеет www.smarthome.spb.ru - должно совпадать
        card_parts = normalized_card.split('.')
        target_parts = normalized_target.split('.')
        
        # Если оба домена имеют минимум 2 части, сравниваем базовые домены
        if len(card_parts) >= 2 and len(target_parts) >= 2:
            # Берем последние 2 части (например, spb.ru из smarthome.spb.ru)
            card_base = '.'.join(card_parts[-2:])
            target_base = '.'.join(target_parts[-2:])
            if card_base == target_base:
                # Если базовые домены совпадают, проверяем, что основной домен тоже совпадает
                # Например, smarthome.spb.ru и www.smarthome.spb.ru
                card_main = card_parts[0] if len(card_parts) > 2 else card_parts[0]
                target_main = target_parts[0] if len(target_parts) > 2 else target_parts[0]
                # Игнорируем www и другие служебные префиксы
                if card_main in ['www', 'www2', 'www3']:
                    card_main = card_parts[1] if len(card_parts) > 1 else card_main
                if target_main in ['www', 'www2', 'www3']:
                    target_main = target_parts[1] if len(target_parts) > 1 else target_main
                return card_main == target_main
        
        return False

    def _quick_extract_address(self, card_url: str) -> str:
        """
        Быстро извлекает только адрес из карточки без полного парсинга.
        Используется для ранней фильтрации карточек по адресу.
        """
        original_url = self.driver.get_current_url()
        try:
            self.driver.navigate(card_url)
            time.sleep(1)  # Короткая задержка для загрузки минимального контента
            page_source, soup = self._get_page_source_and_soup()
            
            address_selectors = [
                'a[href*="/geo/"]',
                'span._wrdavn',
                '[class*="address"]',
                '[class*="location"]',
                '[itemprop="address"]',
            ]
            
            address = ""
            for selector in address_selectors:
                address_elem = soup.select_one(selector)
                if address_elem:
                    raw_address = address_elem.get_text(strip=True)
                    if raw_address and len(raw_address) > 5:
                        address = self._normalize_address(raw_address)
                        break
            
            return address
        except Exception as e:
            logger.warning(f"Error during quick address extraction for {card_url}: {e}")
            return ""
        finally:
            # Возвращаемся на предыдущую страницу, чтобы не нарушать основной цикл
            self.driver.navigate(original_url)
            time.sleep(1)
    
    def _get_card_snippet_data(self, card_element: Tag) -> Optional[Dict[str, Any]]:
        """
        Извлекает данные из snippet карточки на странице поиска 2ГИС.
        Возвращает рейтинг, количество отзывов и другую информацию, которая отображается точно.
        """
        try:
            snippet_data: Dict[str, Any] = {}
            logger.debug(f"Extracting snippet data from card element")
            
            # Проверяем, что элемент не пустой
            if not card_element:
                logger.warning("Card element is None or empty")
                return None
            
            # Логируем HTML элемента для отладки (первые 500 символов)
            card_html_preview = str(card_element)[:500] if card_element else "N/A"
            logger.debug(f"Card element HTML preview: {card_html_preview}")
            
            # Название
            name_selectors = [
                'h1', 'h2', 'h3',
                'a[href*="/firm/"], a[href*="/station/"]',
                '[class*="title"]',
                '[class*="name"]',
            ]
            name = ""
            for selector in name_selectors:
                name_elem = card_element.select_one(selector)
                if name_elem:
                    name = name_elem.get_text(strip=True)
                    if name and len(name) > 2:
                        snippet_data['card_name'] = name
                        break
            
            # Рейтинг - ищем число от 1 до 5 (возможно с десятичной частью)
            rating = ""
            rating_value = 0.0
            # Сначала ищем в тексте элемента рейтинг (например, "5" или "4.5")
            card_text = card_element.get_text(separator=' ', strip=True)
            rating_match = re.search(r'\b([1-5](?:\.\d+)?)\b', card_text)
            if rating_match:
                potential_rating = float(rating_match.group(1))
                if 1.0 <= potential_rating <= 5.0:
                    rating_value = potential_rating
                    snippet_data['card_rating'] = rating_value
                    logger.info(f"Found rating in card text: {rating_value}")
            
            # Если не нашли в тексте, пробуем селекторы
            if rating_value == 0.0:
                rating_selectors = [
                    '[class*="rating"]',
                    '[class*="star"]',
                    '[class*="score"]',
                    'span:contains("5"), span:contains("4"), span:contains("3")',
                ]
                for selector in rating_selectors:
                    try:
                        rating_elem = card_element.select_one(selector)
                        if rating_elem:
                            rating_text = rating_elem.get_text(strip=True)
                            rating_match = re.search(r'([1-5](?:\.\d+)?)', rating_text)
                            if rating_match:
                                potential_rating = float(rating_match.group(1))
                                if 1.0 <= potential_rating <= 5.0:
                                    rating_value = potential_rating
                                    rating = rating_text
                                    snippet_data['card_rating'] = rating_value
                                    logger.info(f"Found rating via selector {selector}: {rating_value}")
                                    break
                    except Exception as e:
                        logger.debug(f"Error with rating selector {selector}: {e}")
                        continue
            
            # Количество отзывов (очень важно - это точное значение со страницы поиска)
            # Реальная структура: <span class="_1xhlznaa">25</span>
            reviews_count = 0
            reviews_selectors = [
                'span._1xhlznaa',  # Точный селектор для количества отзывов
                'span[class*="_1xhlznaa"]',
                '[class*="_1xhlznaa"]',
                'span[class*="reviews"]',  # Альтернативные селекторы
                '[class*="reviews-count"]',
                '[data-reviews]',  # Data-атрибут
            ]
            for selector in reviews_selectors:
                try:
                    reviews_elems = card_element.select(selector)
                    for reviews_elem in reviews_elems:
                        reviews_text = reviews_elem.get_text(strip=True)
                        # Если это число напрямую (как в _1xhlznaa)
                        if reviews_text.isdigit():
                            reviews_count = int(reviews_text)
                            snippet_data['card_reviews_count'] = reviews_count
                            logger.info(f"Found reviews count via selector {selector}: {reviews_count}")
                            break
                        # Также проверяем data-атрибуты
                        if reviews_elem.get('data-reviews'):
                            try:
                                reviews_count = int(reviews_elem.get('data-reviews'))
                                snippet_data['card_reviews_count'] = reviews_count
                                logger.info(f"Found reviews count via data-reviews attribute: {reviews_count}")
                                break
                            except (ValueError, TypeError):
                                pass
                    if reviews_count > 0:
                        break
                except Exception as e:
                    logger.debug(f"Error with reviews selector {selector}: {e}")
                    continue
            
            # Если не нашли через точный селектор, ищем в тексте всего элемента
            if reviews_count == 0:
                card_text = card_element.get_text(separator=' ', strip=True)
                logger.debug(f"Card element text preview: {card_text[:200]}")
                
                # Ищем паттерны типа "25 оценок" или "25 отзывов"
                reviews_match = re.search(r'(\d+)\s*(?:оценок|отзыв)', card_text, re.IGNORECASE)
                if reviews_match:
                    potential_count = int(reviews_match.group(1))
                    if 0 < potential_count < 10000:  # Разумные пределы
                        reviews_count = potential_count
                        snippet_data['card_reviews_count'] = reviews_count
                        logger.info(f"Found reviews count in card text (pattern): {reviews_count}")
                
                # Также пробуем найти просто число рядом со словом "оценок" или "отзыв"
                if reviews_count == 0:
                    # Ищем числа перед словами "оценок", "отзыв", "отзывов"
                    numbers_before_words = re.findall(r'(\d+)\s*(?:оценок|отзыв)', card_text, re.IGNORECASE)
                    if numbers_before_words:
                        potential_count = max(int(n) for n in numbers_before_words)
                        if 0 < potential_count < 10000:
                            reviews_count = potential_count
                            snippet_data['card_reviews_count'] = reviews_count
                            logger.info(f"Found reviews count in card text (numbers before words): {reviews_count}")
                
                # Последняя попытка: ищем все числа и выбираем наиболее вероятное (в разумных пределах)
                if reviews_count == 0:
                    numbers = re.findall(r'\b(\d+)\b', card_text)
                    valid_numbers = [int(n) for n in numbers if 5 <= int(n) <= 1000]
                    if valid_numbers:
                        # Берем наибольшее число в разумных пределах
                        reviews_count = max(valid_numbers)
                        snippet_data['card_reviews_count'] = reviews_count
                        logger.info(f"Found potential reviews count in card text (max number): {reviews_count}")
            
            # Положительные отзывы: ищем по разным селекторам (на странице поиска и на странице отзывов разные классы)
            positive_reviews = 0
            positive_selectors = [
                'label._k8czfzz[title="Положительные"]',  # Старый селектор для страницы поиска
                'label._skhdh07[title="Положительные"]',  # Новый селектор для страницы отзывов
                'label[class*="_k8czfzz"]',
                'label[class*="_skhdh07"]',
                'label[title="Положительные"]',  # Универсальный по title
            ]
            for selector in positive_selectors:
                try:
                    positive_elems = card_element.select(selector)
                    for positive_elem in positive_elems:
                        # Проверяем title или текст
                        title = positive_elem.get('title', '')
                        elem_text = positive_elem.get_text(strip=True)
                        if 'положительные' in title.lower() or 'положительные' in elem_text.lower():
                            # Ищем число в родительском элементе или рядом
                            parent = positive_elem.find_parent()
                            if parent:
                                parent_text = parent.get_text(strip=True)
                                positive_match = re.search(r'(\d+)', parent_text)
                                if positive_match:
                                    positive_reviews = int(positive_match.group(1))
                                    snippet_data['card_reviews_positive'] = positive_reviews
                                    logger.info(f"Found positive reviews via selector {selector}: {positive_reviews}")
                                    break
                    if positive_reviews > 0:
                        break
                except Exception as e:
                    logger.debug(f"Error with selector {selector}: {e}")
                    continue
            
            # Отрицательные отзывы: ищем по точным селекторам из структуры страницы
            # Структура: <li class="_utrabfm"><label class="_xnqndcf" title="Отрицательные">...<span class="_1iurgbx">Отрицательные</span></label></li>
            negative_reviews = 0
            negative_selectors = [
                'label._xnqndcf[title="Отрицательные"]',  # Точный селектор для страницы карточки
                'label[class*="_xnqndcf"][title="Отрицательные"]',
                'label._movtqjn[title="Отрицательные"]',  # Альтернативный селектор
                'label[class*="_movtqjn"][title="Отрицательные"]',
                'label[title="Отрицательные"]',  # Универсальный по title
            ]
            for selector in negative_selectors:
                try:
                    negative_elems = card_element.select(selector)
                    for negative_elem in negative_elems:
                        # Проверяем title
                        title = negative_elem.get('title', '').lower()
                        if 'отрицательные' in title:
                            # Ищем число в родительском элементе <li> или в тексте
                            parent = negative_elem.find_parent('li')
                            if parent:
                                parent_text = parent.get_text(strip=True)
                                # Ищем число в тексте родителя
                                negative_match = re.search(r'(\d+)', parent_text)
                                if negative_match:
                                    negative_reviews = int(negative_match.group(1))
                                    snippet_data['card_reviews_negative'] = negative_reviews
                                    logger.info(f"Found negative reviews via selector {selector}: {negative_reviews}")
                                    break
                            # Если не нашли в родителе, ищем в самом элементе
                            elem_text = negative_elem.get_text(strip=True)
                            negative_match = re.search(r'(\d+)', elem_text)
                            if negative_match:
                                negative_reviews = int(negative_match.group(1))
                                snippet_data['card_reviews_negative'] = negative_reviews
                                logger.info(f"Found negative reviews via selector {selector} (from element text): {negative_reviews}")
                                break
                    if negative_reviews > 0:
                        break
                except Exception as e:
                    logger.debug(f"Error with selector {selector}: {e}")
                    continue
            
            # Логируем результат извлечения
            # ВАЖНО: возвращаем данные даже если не все поля найдены (например, только reviews_count)
            if snippet_data and len(snippet_data) > 0:
                logger.info(f"Extracted snippet data: reviews={snippet_data.get('card_reviews_count', 0)}, "
                          f"positive={snippet_data.get('card_reviews_positive', 0)}, "
                          f"negative={snippet_data.get('card_reviews_negative', 0)}, "
                          f"rating={snippet_data.get('card_rating', 0)}")
            else:
                logger.warning(f"No snippet data extracted (empty dict). Card text: {card_element.get_text(strip=True)[:200] if card_element else 'N/A'}")
            
            # Сайт - извлекаем из snippet ПЕРЕД возвратом
            website_selectors = [
                'a[href^="http"]:not([href*="2gis.ru"]):not([href*="yandex.ru"])',
                'a[class*="website"]',
                'a[itemprop="url"]',
                'a[href*="link.2gis.ru"]',  # Также проверяем ссылки через link.2gis.ru
            ]
            website = ""
            candidate_websites = []
            
            for selector in website_selectors:
                website_elems = card_element.select(selector)
                for website_elem in website_elems:
                    href = website_elem.get('href', '')
                    if not href:
                        continue
                    # Пропускаем служебные домены
                    if '2gis.ru' in href or 'yandex.ru' in href:
                        continue
                    # Если это link.2gis.ru, пытаемся извлечь реальный URL
                    if 'link.2gis.ru' in href:
                        # Упрощенная проверка - ищем домен в тексте ссылки
                        link_text = website_elem.get_text(strip=True)
                        if link_text and '.' in link_text and len(link_text) < 100:
                            # Проверяем, похоже ли это на домен
                            if re.match(r'^[a-zA-Z0-9][a-zA-Z0-9\-\.]*\.[a-zA-Z]{2,}$', link_text):
                                candidate_websites.append(f"http://{link_text}")
                                logger.debug(f"Found candidate website from link.2gis.ru text: http://{link_text}")
                    else:
                        candidate_websites.append(href)
                        logger.debug(f"Found candidate website from snippet: {href}")
            
            # Если есть целевой сайт для фильтрации, выбираем наиболее подходящий
            if candidate_websites and self._target_website:
                normalized_target = self._normalize_url_for_comparison(self._target_website)
                for candidate in candidate_websites:
                    normalized_candidate = self._normalize_url_for_comparison(candidate)
                    if normalized_candidate == normalized_target:
                        website = candidate
                        logger.debug(f"Selected matching website: {website} (matches target: {self._target_website})")
                        break
            
            # Если не нашли совпадение, берем первую подходящую
            if not website and candidate_websites:
                website = candidate_websites[0]
                logger.debug(f"Selected first candidate website: {website}")
            
            if website:
                snippet_data['card_website'] = website
                logger.debug(f"Added website to snippet data: {website}")
            
            # Логируем результат извлечения
            # ВАЖНО: возвращаем данные даже если не все поля найдены (например, только reviews_count)
            if snippet_data and len(snippet_data) > 0:
                logger.info(f"Extracted snippet data: reviews={snippet_data.get('card_reviews_count', 0)}, "
                          f"positive={snippet_data.get('card_reviews_positive', 0)}, "
                          f"negative={snippet_data.get('card_reviews_negative', 0)}, "
                          f"rating={snippet_data.get('card_rating', 0)}, website={snippet_data.get('card_website', 'N/A')}")
            else:
                logger.warning(f"No snippet data extracted (empty dict). Card text: {card_element.get_text(strip=True)[:200] if card_element else 'N/A'}")
            
            # ВАЖНО: возвращаем snippet_data даже если не все поля найдены, главное чтобы было хотя бы одно поле
            return snippet_data if (snippet_data and len(snippet_data) > 0) else None
        except Exception as e:
            logger.debug(f"Error extracting snippet data from 2GIS card: {e}")
            return None

    def _quick_extract_website(self, card_url: str) -> str:
        """
        Быстро извлекает только сайт из карточки без полного парсинга.
        Используется для ранней фильтрации карточек по сайту.
        ОТКЛЮЧЕНО: фильтрация по сайту временно отключена.
        Обрабатывает ссылки через link.2gis.ru, извлекая реальный URL.
        """
        original_url = self.driver.get_current_url()
        try:
            self.driver.navigate(card_url)
            time.sleep(1)  # Короткая задержка для загрузки минимального контента
            page_source, soup = self._get_page_source_and_soup()
            
            def extract_url_from_link_2gis(href: str) -> Optional[str]:
                """Извлекает реальный URL из ссылки через link.2gis.ru"""
                if not href or 'link.2gis.ru' not in href.lower():
                    return None
                try:
                    # Пытаемся найти URL в параметрах или декодировать base64
                    # Сначала ищем в query параметрах
                    parsed = urllib.parse.urlparse(href)
                    # Проверяем, есть ли URL в конце пути (после последнего /)
                    path_parts = parsed.path.split('/')
                    if len(path_parts) > 0:
                        last_part = path_parts[-1]
                        # Пытаемся декодировать base64
                        try:
                            import base64
                            decoded = base64.urlsafe_b64decode(last_part + '==')
                            decoded_str = decoded.decode('utf-8', errors='ignore')
                            # Ищем URL в декодированной строке
                            url_match = re.search(r'https?://[^\s"\'<>]+', decoded_str)
                            if url_match:
                                return url_match.group(0)
                        except:
                            pass
                    # Если не получилось, ищем в query параметрах
                    query_params = urllib.parse.parse_qs(parsed.query)
                    for key, values in query_params.items():
                        for value in values:
                            if 'http://' in value or 'https://' in value:
                                url_match = re.search(r'https?://[^\s"\'<>]+', value)
                                if url_match:
                                    return url_match.group(0)
                except Exception as e:
                    logger.debug(f"Error extracting URL from link.2gis.ru: {e}")
                return None
            
            website = ""
            # Сначала ищем прямые ссылки (не через link.2gis.ru)
            # Исключаем служебные домены 2ГИС и других сервисов
            excluded_domains = [
                '2gis.ru', '2gis.', 'yandex.ru', 'maps.yandex', 'link.2gis', 'redirect.2gis',
                'account.2gis.com', 'hh.ru', 'otello.ru', '2gis.am', '2gis.kz', '2gis.kg', '2gis.ae',
                'vk.com', 't.me', 'facebook.com', 'instagram.com', 'twitter.com'
            ]
            website_selectors = [
                'a[href^="http"]',
                'a[data-qa-id="website"]',
                'a[class*="website"]',
                'a[class*="site"]',
                'a[itemprop="url"]',
            ]
            for selector in website_selectors:
                website_elems = soup.select(selector)
                for website_elem in website_elems:
                    href = website_elem.get('href', '')
                    if not href:
                        continue
                    # Проверяем, что это не служебная ссылка
                    href_lower = href.lower()
                    is_excluded = any(domain in href_lower for domain in excluded_domains)
                    if not is_excluded:
                        website = href
                        logger.info(f"Found direct website link: {website}")
                        break
                if website:
                    break
            
            # Если не нашли прямую ссылку, ищем через link.2gis.ru
            if not website:
                link_2gis_elems = soup.select('a[href*="link.2gis.ru"]')
                logger.info(f"Found {len(link_2gis_elems)} link.2gis.ru elements, checking for website...")
                
                # Собираем все возможные сайты из link.2gis.ru
                candidate_websites = []
                
                for elem in link_2gis_elems:
                    href = elem.get('href', '')
                    link_text = elem.get_text(strip=True)
                    logger.info(f"Checking link.2gis.ru element: text='{link_text[:50]}', href='{href[:100]}'")
                    
                    if href:
                        # Сначала проверяем текст ссылки - там часто указан домен
                        if link_text and '.' in link_text and len(link_text) < 100:
                            # Проверяем, похоже ли это на домен (не служебный)
                            if re.match(r'^[a-zA-Z0-9][a-zA-Z0-9\-\.]*\.[a-zA-Z]{2,}$', link_text):
                                # Исключаем служебные домены
                                if not any(domain in link_text.lower() for domain in excluded_domains):
                                    candidate_websites.append(f"http://{link_text}")
                                    logger.info(f"Found candidate website from link text: http://{link_text}")
                        
                        # Пытаемся извлечь реальный URL из href
                        extracted_url = extract_url_from_link_2gis(href)
                        if extracted_url:
                            # Проверяем, что это не служебный домен
                            if not any(domain in extracted_url.lower() for domain in excluded_domains):
                                candidate_websites.append(extracted_url)
                                logger.info(f"Found candidate website from link.2gis.ru href: {extracted_url}")
                
                # Если есть целевой сайт для фильтрации, выбираем наиболее подходящий
                if candidate_websites and self._target_website:
                    normalized_target = self._normalize_url_for_comparison(self._target_website)
                    for candidate in candidate_websites:
                        normalized_candidate = self._normalize_url_for_comparison(candidate)
                        if normalized_candidate == normalized_target:
                            website = candidate
                            logger.info(f"Selected matching website: {website} (matches target: {self._target_website})")
                            break
                
                # Если не нашли совпадение, берем первую подходящую
                if not website and candidate_websites:
                    website = candidate_websites[0]
                    logger.info(f"Selected first candidate website: {website}")
            
            if website:
                # Нормализуем URL (убираем протокол для сравнения, но возвращаем с протоколом)
                if not website.startswith('http://') and not website.startswith('https://'):
                    website = f"http://{website}"
                logger.info(f"Extracted website from {card_url}: {website}")
            else:
                logger.info(f"No website found for {card_url}")
            
            return website
        except Exception as e:
            logger.warning(f"Error during quick website extraction for {card_url}: {e}")
            return ""
        finally:
            # Возвращаемся на предыдущую страницу, чтобы не нарушать основной цикл
            self.driver.navigate(original_url)
            time.sleep(1)

    def parse(self, url: str, search_query_site: Optional[str] = None, search_query_address: Optional[str] = None) -> Dict[str, Any]:
        logger.info(f"Starting 2GIS parser for URL: {url}")
        self._url = url
        self._target_website = search_query_site  # Сохраняем целевой сайт для фильтрации
        self._target_address = search_query_address  # Сохраняем целевой адрес для фильтрации
        if self._target_website:
            logger.info(f"Target website for filtering: {self._target_website}")
        if self._target_address:
            logger.info(f"Target address for filtering: {self._target_address}")

        self._update_progress("Инициализация парсера 2GIS...")

        # Сбрасываем агрегированные данные перед новым запуском
        self._aggregated_data = {
            'total_cards': 0,
            'total_rating_sum': 0.0,
            'total_reviews_count': 0,
            'total_positive_reviews': 0,
            'total_negative_reviews': 0,
            'total_answered_count': 0,
            'total_answered_reviews_count': 0,
            'total_unanswered_reviews_count': 0,
            'total_response_time_sum_days': 0.0,
            'total_response_time_calculated_count': 0,
        }
        
        # Создаем папку для сохранения данных о датах ответов
        self._response_dates_dir = os.path.join("output", "response_dates")
        os.makedirs(self._response_dates_dir, exist_ok=True)
        self._response_dates_file = os.path.join(self._response_dates_dir, f"response_dates_{dt_module.datetime.now().strftime('%Y%m%d_%H%M%S')}.json")
        self._response_dates_data: List[Dict[str, Any]] = []  # Список для хранения данных о датах ответов

        card_data_list: List[Dict[str, Any]] = []

        # Извлекаем название компании из URL
        # Формат URL: https://2gis.ru/search/{encoded_company_name}%20{encoded_location}
        search_query_name = "2gisSearch"
        try:
            if '/search/' in url:
                # Берем часть после /search/ и до первого ? или конца
                search_part = url.split('/search/')[1].split('?')[0]
                # Декодируем URL-кодирование
                search_part = urllib.parse.unquote(search_part)
                # Убираем город из запроса, если он есть (обычно в конце через пробел или %20)
                # Оставляем только название компании (первая часть до последнего пробела, если есть город)
                parts = search_part.split()
                # Если есть несколько частей, берем все кроме последней (которая может быть городом)
                # Но если частей мало (1-2), берем все
                if len(parts) > 2:
                    # Вероятно, последняя часть - это город, убираем её
                    search_query_name = ' '.join(parts[:-1])
                else:
                    search_query_name = search_part
        except Exception as e:
            logger.warning(f"Could not extract company name from 2GIS URL: {e}")
            search_query_name = "2gisSearch"

        aggregated_info: Dict[str, Any] = {
            'search_query_name': search_query_name,
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

        try:
            logger.info(f"Navigating to URL: {url}")
            self._update_progress("Поиск карточек...")
            self.driver.navigate(url)
            time.sleep(3)

            logger.info("Injecting XHR counter script")
            self.driver.execute_script(self._add_xhr_counter_script())

            logger.info("Waiting for page to load...")
            time.sleep(2)

            # Собираем ссылки пагинации и обходим страницы по URL, а не кликами,
            # чтобы гарантированно обработать все 2ГИС‑страницы (1..N).
            page_source, soup = self._get_page_source_and_soup()
            pagination_urls = self._get_pagination_links(soup, url)

            all_card_urls: set[str] = set()
            max_pages = 20
            
            # Словари для раннего извлечения адресов и сайтов из snippets (оптимизация)
            card_url_to_address: Dict[str, str] = {}
            card_url_to_website: Dict[str, str] = {}
            # Словарь для хранения данных из snippet (рейтинг, количество отзывов - точные данные со страницы поиска)
            card_url_to_snippet_data: Dict[str, Dict[str, Any]] = {}

            pages_to_process: List[str] = [url]
            if pagination_urls:
                # ограничиваемся разумным числом страниц
                pages_to_process.extend(pagination_urls[: max_pages - 1])

            for page_num, page_url in enumerate(pages_to_process, start=1):
                if self._is_stopped():
                    logger.info(f"2GIS: stop flag detected before processing search page {page_num}, breaking pages loop")
                    break
                try:
                    if page_num > 1:
                        logger.info(f"Processing 2GIS search page {page_num}/{len(pages_to_process)}: {page_url}")
                        self._update_progress(
                            f"Поиск карточек: обработка страницы {page_num}/{len(pages_to_process)}, найдено {len(all_card_urls)} карточек"
                        )
                        self.driver.navigate(page_url)
                        time.sleep(2)

                    initial_card_count = len(all_card_urls)
                    logger.info(f"Initial card count on page {page_num}: {initial_card_count}")

                    self._update_progress(
                        f"Поиск карточек: прокрутка страницы {page_num} для загрузки всех карточек..."
                    )
                    cards_count_after_scroll = self._scroll_to_load_all_cards()
                    logger.info(
                        f"Scroll completed for 2GIS page {page_num}. Found {cards_count_after_scroll} cards after scrolling."
                    )
                    time.sleep(2)

                    # После прокрутки обновляем DOM и собираем ссылки карточек
                    page_source, soup = self._get_page_source_and_soup()
                    page_urls = self._get_links()
                    
                    # ОПТИМИЗАЦИЯ: Одновременно извлекаем адреса и сайты из snippets для ранней фильтрации
                    for selector in self._card_selectors:
                        card_elements = soup.select(selector)
                        for card_elem in card_elements:
                            link_elem = card_elem.select_one('a[href*="/firm/"], a[href*="/station/"]')
                            if not link_elem:
                                continue
                            href = link_elem.get('href', '')
                            if not href:
                                continue
                            if not href.startswith('http'):
                                href = urllib.parse.urljoin("https://2gis.ru", href)
                            
                            if href not in page_urls:
                                continue
                            
                            # Извлекаем адрес из snippet (если нужно)
                            if self._target_address and href not in card_url_to_address:
                                address_elem = card_elem.select_one('a[href*="/geo/"], span._wrdavn, [class*="address"], [class*="location"]')
                                if address_elem:
                                    address = address_elem.get_text(strip=True)
                                    if address and len(address) > 5:
                                        card_url_to_address[href] = self._normalize_address(address)
                            
                            # Извлекаем данные из snippet (сайт, рейтинг, количество отзывов)
                            snippet_data = self._get_card_snippet_data(card_elem)
                            if snippet_data and len(snippet_data) > 0:
                                logger.info(f"Extracted snippet data for {href[:80]}: reviews={snippet_data.get('card_reviews_count', 0)}, rating={snippet_data.get('card_rating', 0)}")
                            else:
                                logger.warning(f"Could not extract snippet data for {href[:80]}, card element text preview: {card_elem.get_text(strip=True)[:100] if card_elem else 'N/A'}")
                            if snippet_data and len(snippet_data) > 0:
                                # Нормализуем URL для единообразия (убираем query параметры и фрагменты)
                                normalized_href = href.split('?')[0].split('#')[0].rstrip('/')
                                # Сохраняем все данные из snippet для использования в агрегированной информации
                                card_url_to_snippet_data[normalized_href] = snippet_data
                                # Логируем для отладки
                                snippet_reviews = snippet_data.get('card_reviews_count', 0)
                                if snippet_reviews > 0:
                                    logger.info(f"Extracted snippet data for {normalized_href[:80]}: reviews={snippet_reviews}, rating={snippet_data.get('card_rating', 0)}")
                                # Также сохраняем с оригинальным href на случай, если URL не нормализуется одинаково
                                card_url_to_snippet_data[href] = snippet_data
                                
                                # Извлекаем сайт из snippet (если нужно)
                                if not self._target_address and self._target_website and href not in card_url_to_website:
                                    website = snippet_data.get('card_website', '')
                                    if website:
                                        card_url_to_website[href] = website
                    
                    all_card_urls.update(page_urls)

                    new_cards = len(all_card_urls) - initial_card_count
                    logger.info(
                        f"2GIS page {page_num}: found {new_cards} new cards. Total collected: {len(all_card_urls)}"
                    )

                    if len(all_card_urls) >= self._max_records:
                        logger.info(f"Reached max records limit ({self._max_records}). Stopping pagination.")
                        break

                except Exception as page_error:
                    logger.warning(f"Error processing 2GIS search page {page_num}: {page_error}", exc_info=True)
                    continue

            card_urls = list(all_card_urls)

            logger.info(f"Found {len(card_urls)} card URLs")

            if not card_urls:
                logger.warning("No card URLs found on the page")
                self._update_progress("Карточки не найдены")
                return {
                    'cards_data': [],
                    'aggregated_info': aggregated_info,
                }

            # ОПТИМИЗАЦИЯ: Ранняя фильтрация по адресу (если указан целевой адрес)
            # Используем уже извлеченные адреса из snippets (извлечены при сборе карточек)
            filtered_card_urls = card_urls
            
            if self._target_address:
                logger.info(f"Применяю раннюю фильтрацию по адресу: {self._target_address}")
                logger.info(f"Использую адреса, извлеченные при сборе карточек для {len(filtered_card_urls)} карточек...")
                original_count = len(filtered_card_urls)
                matching_urls = []
                
                # Фильтруем карточки по уже извлеченным адресам
                for card_url in filtered_card_urls:
                    address = card_url_to_address.get(card_url, '')
                    if address:
                        if self._address_matches(address, self._target_address):
                            matching_urls.append(card_url)
                            logger.info(f"Карточка прошла фильтр по адресу: {card_url[:80]} -> {address[:50]}")
                        else:
                            logger.debug(f"Карточка исключена (адрес не совпадает): {card_url[:80]} -> {address[:50]}")
                
                # Если не все карточки были найдены на страницах поиска, проверяем остальные
                remaining = [url for url in filtered_card_urls if url not in card_url_to_address]
                if remaining:
                    logger.info(f"Проверяю адреса для оставшихся {len(remaining)} карточек (переход на детальные страницы)...")
                    self._update_progress(f"Ранняя фильтрация по адресу: проверка {len(remaining)} карточек...")
                    for idx, card_url in enumerate(remaining[:self._max_records], 1):
                        if self._is_stopped():
                            break
                        if idx % 10 == 0:
                            self._update_progress(f"Ранняя фильтрация по адресу: {idx}/{len(remaining)}")
                        
                        # Быстро извлекаем адрес, переходя на карточку
                        try:
                            address = self._quick_extract_address(card_url)
                            card_url_to_address[card_url] = address
                            if self._address_matches(address, self._target_address):
                                matching_urls.append(card_url)
                                logger.info(f"Карточка прошла фильтр по адресу: {card_url[:80]} -> {address[:50]}")
                            else:
                                logger.debug(f"Карточка исключена (адрес не совпадает): {card_url[:80]} -> {address[:50]}")
                        except Exception as e:
                            logger.warning(f"Ошибка при извлечении адреса для {card_url}: {e}")
                            continue
                
                filtered_card_urls = matching_urls
                logger.info(f"Ранняя фильтрация по адресу завершена: {original_count} -> {len(filtered_card_urls)} карточек (осталось {len(filtered_card_urls)} для полного парсинга)")
            
            # ОПТИМИЗАЦИЯ: Ранняя фильтрация по сайту (если адрес НЕ указан, но сайт указан)
            # Используем уже извлеченные сайты из snippets (извлечены при сборе карточек)
            if not self._target_address and self._target_website:
                logger.info(f"Применяю раннюю фильтрацию по сайту: {self._target_website}")
                logger.info(f"Использую сайты, извлеченные при сборе карточек для {len(filtered_card_urls)} карточек...")
                original_count = len(filtered_card_urls)
                matching_urls = []
                
                # Фильтруем карточки по уже извлеченным сайтам
                for card_url in filtered_card_urls:
                    website = card_url_to_website.get(card_url, '')
                    if website:
                        normalized_card = self._normalize_url_for_comparison(website)
                        normalized_target = self._normalize_url_for_comparison(self._target_website)
                        if self._website_matches(website, self._target_website):
                            matching_urls.append(card_url)
                            logger.info(f"Карточка прошла фильтр по сайту: {card_url[:80]} -> {website[:50]} (нормализовано: {normalized_card} == {normalized_target})")
                        else:
                            logger.warning(f"Карточка исключена (сайт не совпадает): {card_url[:80]} -> {website[:50]} (нормализовано: {normalized_card} != {normalized_target}, целевой: {self._target_website})")
                    else:
                        logger.warning(f"Карточка не имеет извлеченного сайта: {card_url[:80]}, будет проверена позже")
                
                # Если не все карточки были найдены на страницах поиска, проверяем остальные
                remaining = [url for url in filtered_card_urls if url not in card_url_to_website]
                if remaining:
                    logger.info(f"Проверяю сайты для оставшихся {len(remaining)} карточек (переход на детальные страницы)...")
                    self._update_progress(f"Ранняя фильтрация по сайту: проверка {len(remaining)} карточек...")
                    for idx, card_url in enumerate(remaining[:self._max_records], 1):
                        if self._is_stopped():
                            break
                        if idx % 10 == 0:
                            self._update_progress(f"Ранняя фильтрация по сайту: {idx}/{len(remaining)}")
                        
                        # Быстро извлекаем сайт, переходя на карточку
                        try:
                            website = self._quick_extract_website(card_url)
                            card_url_to_website[card_url] = website
                            if self._website_matches(website, self._target_website):
                                matching_urls.append(card_url)
                                logger.info(f"Карточка прошла фильтр по сайту: {card_url[:80]} -> {website[:50]}")
                            else:
                                logger.debug(f"Карточка исключена (сайт не совпадает): {card_url[:80]} -> {website[:50]}")
                        except Exception as e:
                            logger.warning(f"Ошибка при извлечении сайта для {card_url}: {e}")
                            continue
                
                filtered_card_urls = matching_urls
                logger.info(f"Ранняя фильтрация по сайту завершена: {original_count} -> {len(filtered_card_urls)} карточек (осталось {len(filtered_card_urls)} для полного парсинга)")

            self._update_progress(f"Сканирование карточек: 0/{len(filtered_card_urls)}")

            name_selectors = [
                'h1[class*="title"]',
                'h1',
                '[class*="title"]',
                '[class*="name"]',
            ]
            address_selectors = [
                # Основной, наиболее стабильный вариант: ссылка на гео-объект
                'a[href*="/geo/"]',
                # Обёртка вокруг ссылки на адрес (у 2ГИС сейчас класс вида _wrdavn)
                'span._wrdavn',
                # Более общие селекторы на случай изменений в вёрстке
                '[class*="address"]',
                '[class*="location"]',
                '[itemprop="address"]',
            ]
            rating_selectors = [
                '[class*="rating"]',
                '[class*="star"]',
                '[class*="score"]',
            ]
            phone_selectors = [
                'a[href^="tel:"]',
                '[class*="phone"]',
            ]

            for idx, card_url in enumerate(filtered_card_urls[: self._max_records], start=1):
                if self._is_stopped():
                    logger.info(f"2GIS: stop flag detected before processing card {idx}, breaking cards loop")
                    break
                try:
                    self._update_progress(
                            f"Сканирование карточек: {idx}/{min(len(filtered_card_urls), self._max_records)}"
                    )

                    logger.info(
                        f"Processing card {idx}/{min(len(filtered_card_urls), self._max_records)}: {card_url}"
                    )
                    self.driver.navigate(card_url)
                    time.sleep(2)

                    page_source, soup = self._get_page_source_and_soup()

                    # Название
                    name = ""
                    for selector in name_selectors:
                        name_elem = soup.select_one(selector)
                        if name_elem:
                            name = name_elem.get_text(strip=True)
                            if name:
                                break

                    # Адрес
                    address = ""
                    for selector in address_selectors:
                        address_elem = soup.select_one(selector)
                        if address_elem:
                            raw_address = address_elem.get_text(strip=True)
                            if raw_address and len(raw_address) > 5:
                                address = self._normalize_address(raw_address)
                                break

                    # Рейтинг
                    rating = ""
                    rating_value = 0.0
                    for selector in rating_selectors:
                        rating_elem = soup.select_one(selector)
                        if rating_elem:
                            rating_text = rating_elem.get_text(strip=True)
                            rating_match = re.search(r'(\d+(?:\.\d+)?)', rating_text)
                            if rating_match:
                                rating_value = float(rating_match.group(1))
                                rating = rating_text
                                break

                    if not rating_value:
                        stars = soup.select('[class*="star"], img[class*="icon"]')
                        if stars:
                            filled_stars = len(
                                [
                                    s
                                    for s in stars
                                    if 'active' in str(s.get('class', [])) or 'fill' in str(s.get('class', []))
                                ]
                            )
                            if filled_stars > 0:
                                rating_value = filled_stars
                                rating = str(filled_stars)

                    # Телефон - сначала кликаем на кнопку "показать телефон", если она есть
                    try:
                        # Ищем и кликаем кнопку "показать телефон" через JavaScript
                        show_phone_script = """
                        var selectors = [
                            'button[class*="phone"]',
                            'a[class*="phone"]',
                            'span[class*="phone"]',
                            '[class*="показать"]',
                            '[class*="телефон"]',
                            'button[aria-label*="телефон"]',
                            'button[aria-label*="phone"]',
                            '[data-qa*="phone"]',
                            '[data-test*="phone"]',
                        ];
                        for (var s = 0; s < selectors.length; s++) {
                            var buttons = document.querySelectorAll(selectors[s]);
                            for (var i = 0; i < buttons.length; i++) {
                                var btn = buttons[i];
                                if (!btn || btn.offsetParent === null) continue;
                                var text = (btn.textContent || btn.innerText || btn.getAttribute('aria-label') || '').toLowerCase();
                                if (text.includes('показать') || text.includes('телефон') || text.includes('номер') || 
                                    text.includes('phone') || text.includes('show')) {
                                    try {
                                        btn.scrollIntoView({behavior: 'smooth', block: 'center'});
                                        btn.click();
                                        return true;
                                    } catch(e) {
                                        try {
                                            var event = new MouseEvent('click', {bubbles: true, cancelable: true});
                                            btn.dispatchEvent(event);
                                            return true;
                                        } catch(e2) {}
                                    }
                                }
                            }
                        }
                        return false;
                        """
                        clicked = self.driver.execute_script(show_phone_script)
                        if clicked:
                            time.sleep(1.5)  # Ждем загрузки телефона
                            # Обновляем soup после клика
                            page_source, soup = self._get_page_source_and_soup()
                            logger.info("Clicked 'show phone' button, updated page source")
                        else:
                            logger.debug("No 'show phone' button found or click failed")
                    except Exception as phone_click_err:
                        logger.warning(f"Error trying to click show phone button: {phone_click_err}")
                    
                    # Теперь извлекаем телефон
                    phone = ""
                    for selector in phone_selectors:
                        phone_elem = soup.select_one(selector)
                        if phone_elem:
                            phone = phone_elem.get_text(strip=True)
                            if not phone and phone_elem.get('href'):
                                phone = phone_elem.get('href').replace('tel:', '').strip()
                            if phone:
                                break
                    
                    # Если телефон не найден, пробуем найти через data-атрибуты или скрытые элементы
                    if not phone:
                        # Ищем элементы с data-phone или data-tel
                        phone_data_elems = soup.select('[data-phone], [data-tel], [data-number]')
                        for elem in phone_data_elems:
                            phone = elem.get('data-phone') or elem.get('data-tel') or elem.get('data-number')
                            if phone:
                                break

                    # Сайт (используем предварительно извлеченный, если есть)
                    website = card_url_to_website.get(card_url, "")
                    if not website:
                        # Если сайт не был извлечен при ранней фильтрации, используем тот же метод
                        website = self._quick_extract_website(card_url)
                        # Сохраняем для возможного переиспользования
                        if website:
                            card_url_to_website[card_url] = website
                    
                    if website:
                        if card_url_to_website.get(card_url):
                            logger.debug(f"Использован предварительно извлеченный сайт для карточки '{name}': {website}")
                        else:
                            logger.debug(f"Извлечен сайт из 2GIS карточки '{name}': {website}")

                    # Обновляем прогресс перед началом парсинга отзывов
                    self._update_progress(f"Парсинг отзывов для карточки {idx}/{min(len(filtered_card_urls), self._max_records)}: {name[:50]}")
                    
                    reviews_data = self._get_card_reviews_info_2gis(card_url)

                    if not name:
                        logger.warning(f"Skipping 2GIS card without name: {card_url}")
                        continue

                    # Используем данные из snippet для агрегированной информации (точные данные со страницы поиска)
                    # Для детальных отзывов используем данные из парсинга страницы карточки
                    detailed_reviews_list = reviews_data.get('details', [])
                    # ВАЖНО: Логируем количество отзывов для отладки
                    logger.info(f"Card '{name}': reviews_data keys = {list(reviews_data.keys()) if reviews_data else 'N/A'}")
                    logger.info(f"Card '{name}': detailed_reviews_list length = {len(detailed_reviews_list) if detailed_reviews_list else 0}")
                    if detailed_reviews_list and len(detailed_reviews_list) > 0:
                        logger.info(f"Card '{name}': First review sample keys = {list(detailed_reviews_list[0].keys()) if isinstance(detailed_reviews_list[0], dict) else 'N/A'}")
                    else:
                        logger.warning(f"Card '{name}': detailed_reviews_list is empty! reviews_data.get('details') = {reviews_data.get('details', 'N/A') if reviews_data else 'N/A'}")
                    # Нормализуем card_url для поиска в словаре (убираем query параметры и фрагменты)
                    normalized_card_url = card_url.split('?')[0].split('#')[0].rstrip('/')
                    snippet_data = card_url_to_snippet_data.get(normalized_card_url, {})
                    if not snippet_data:
                        # Пробуем найти по оригинальному URL
                        snippet_data = card_url_to_snippet_data.get(card_url, {})
                    if not snippet_data:
                        # Пробуем найти по URL без последнего слэша
                        alt_url = normalized_card_url.rstrip('/')
                        snippet_data = card_url_to_snippet_data.get(alt_url, {})
                    if not snippet_data:
                        # Пробуем найти по любому варианту URL, который содержит ID фирмы
                        firm_id_match = re.search(r'/firm/(\d+)', card_url)
                        if firm_id_match:
                            firm_id = firm_id_match.group(1)
                            for key, data in card_url_to_snippet_data.items():
                                if firm_id in key:
                                    snippet_data = data
                                    logger.info(f"Found snippet data by firm ID {firm_id}: reviews={snippet_data.get('card_reviews_count', 0)}")
                                    break
                    
                    if snippet_data:
                        logger.info(f"Found snippet data for card {card_url[:80]}: reviews={snippet_data.get('card_reviews_count', 0)}, rating={snippet_data.get('card_rating', 0)}")
                    else:
                        logger.warning(f"No snippet data found for card {card_url[:80]}, normalized={normalized_card_url[:80]}. Available keys: {list(card_url_to_snippet_data.keys())[:3]}")
                    
                    # Для агрегированной информации используем данные из snippet (если есть)
                    # Это гарантирует точность данных, как на странице поиска
                    snippet_reviews_count = snippet_data.get('card_reviews_count', 0)
                    snippet_rating = snippet_data.get('card_rating', 0.0)
                    snippet_positive = snippet_data.get('card_reviews_positive', 0)
                    snippet_negative = snippet_data.get('card_reviews_negative', 0)
                    
                    # Если есть данные из snippet - используем их для агрегированной информации
                    # Иначе используем данные из детального парсинга
                    # ВАЖНО: snippet данные приоритетнее, так как они точнее отражают реальное количество на странице поиска
                    if snippet_reviews_count > 0:
                        actual_reviews_count = snippet_reviews_count
                        logger.info(f"Using snippet reviews count for aggregation: {actual_reviews_count} (card: {name})")
                    else:
                        # Используем количество из детального парсинга
                        detailed_count = len(detailed_reviews_list) if detailed_reviews_list else 0
                        reviews_data_count = reviews_data.get('reviews_count', 0) if reviews_data else 0
                        # Берем максимальное значение из доступных
                        actual_reviews_count = max(detailed_count, reviews_data_count)
                        logger.info(f"Using detailed reviews count for aggregation: {actual_reviews_count} (detailed={detailed_count}, reviews_data={reviews_data_count}, card: {name})")
                    
                    # Для рейтинга используем данные из структуры страницы карточки (приоритет), затем из snippet
                    card_rating_from_page = reviews_data.get('card_rating_from_page', 0.0)
                    if card_rating_from_page > 0:
                        rating_value = card_rating_from_page
                        rating = str(card_rating_from_page)
                        logger.info(f"Using card rating from page structure: {rating_value} (card: {name})")
                    elif snippet_rating and snippet_rating > 0:
                        rating_value = snippet_rating
                        rating = str(snippet_rating)
                        logger.info(f"Using snippet rating for aggregation: {rating_value} (card: {name})")
                    
                    # Для положительных, отрицательных и нейтральных отзывов используем данные из структуры страницы (приоритет), затем из snippet
                    # Извлекаем из reviews_data списки отзывов по блокам
                    positive_reviews_list = reviews_data.get('positive_reviews_list', [])
                    negative_reviews_list = reviews_data.get('negative_reviews_list', [])
                    neutral_reviews_list = reviews_data.get('neutral_reviews_list', [])
                    answered_reviews_list = reviews_data.get('answered_reviews_list', [])
                    
                    # Используем количество из структуры страницы, если есть
                    if snippet_positive > 0:
                        reviews_data['positive_reviews'] = snippet_positive
                        logger.info(f"Using snippet positive reviews for aggregation: {snippet_positive} (card: {name})")
                    elif len(positive_reviews_list) > 0:
                        # Используем количество из распарсенных позитивных отзывов
                        reviews_data['positive_reviews'] = len(positive_reviews_list)
                        logger.info(f"Using parsed positive reviews count: {len(positive_reviews_list)} (card: {name})")
                    
                    if snippet_negative > 0:
                        reviews_data['negative_reviews'] = snippet_negative
                        logger.info(f"Using snippet negative reviews for aggregation: {snippet_negative} (card: {name})")
                    elif len(negative_reviews_list) > 0:
                        # Используем количество из распарсенных негативных отзывов
                        reviews_data['negative_reviews'] = len(negative_reviews_list)
                        logger.info(f"Using parsed negative reviews count: {len(negative_reviews_list)} (card: {name})")
                    
                    # Нейтральные отзывы (3⭐) - используем количество из распарсенных отзывов
                    if len(neutral_reviews_list) > 0:
                        reviews_data['neutral_reviews'] = len(neutral_reviews_list)
                        logger.info(f"Using parsed neutral reviews count: {len(neutral_reviews_list)} (card: {name})")
                    else:
                        reviews_data['neutral_reviews'] = 0
                    
                    # Нейтральные отзывы (3⭐) - используем количество из распарсенных отзывов
                    if len(neutral_reviews_list) > 0:
                        reviews_data['neutral_reviews'] = len(neutral_reviews_list)
                        logger.info(f"Using parsed neutral reviews count: {len(neutral_reviews_list)} (card: {name})")
                    else:
                        reviews_data['neutral_reviews'] = 0

                    card_data: Dict[str, Any] = {
                        'card_name': name,
                        'card_address': address,
                        'card_rating': rating,
                        'card_reviews_count': actual_reviews_count,  # Используем данные из структуры страницы для агрегированной информации
                        'card_ratings_count': reviews_data.get('ratings_count', 0),  # Количество оценок из структуры страницы
                        'card_website': website,
                        'card_phone': phone,
                        'card_rubrics': "",
                        'card_response_status': "UNKNOWN",
                        'card_avg_response_time': reviews_data.get('avg_response_time_days', 0.0),  # Среднее время ответа для блока "С ответом"
                        'card_reviews_positive': reviews_data.get('positive_reviews', 0),
                        'card_reviews_negative': reviews_data.get('negative_reviews', 0),
                        'card_reviews_neutral': reviews_data.get('neutral_reviews', 0),  # Нейтральные отзывы (3⭐)
                        'card_reviews_texts': "; ".join(reviews_data.get('texts', [])),
                        'card_answered_reviews_count': reviews_data.get('answered_count', 0),  # Используем значение из структуры страницы (если найдено)
                        'card_unanswered_reviews_count': max(0, actual_reviews_count - reviews_data.get('answered_count', 0)),  # Вычисляем на основе структуры страницы
                        'detailed_reviews': detailed_reviews_list,  # Все отзывы
                        'positive_reviews_list': positive_reviews_list,  # Блок позитивных отзывов
                        'negative_reviews_list': negative_reviews_list,  # Блок негативных отзывов
                        'answered_reviews_list': answered_reviews_list,  # Блок отзывов с ответом (для вычисления среднего времени ответа)
                        'source': '2gis',
                    }
                    
                    # Логируем количество отзывов в карточке для отладки
                    detailed_reviews_count = len(card_data.get('detailed_reviews', []))
                    if detailed_reviews_count > 0:
                        logger.info(f"Card '{name}': {detailed_reviews_count} detailed reviews saved to card_data")

                    # Фильтрация по адресу уже выполнена на раннем этапе, дополнительная проверка не нужна
                    # Фильтрация по сайту отключена

                    card_data_list.append(card_data)

                    if len(card_data_list) >= self._max_records:
                        break

                except Exception as e:
                    logger.error(f"Error processing card {card_url}: {e}", exc_info=True)
                    continue

            # Фильтруем карточки по названию компании перед агрегацией
            if card_data_list and search_query_name and search_query_name != "2gisSearch":
                filtered_cards = self._filter_cards_by_name(card_data_list, search_query_name)
                logger.info(f"Filtered {len(card_data_list)} 2GIS cards to {len(filtered_cards)} card(s) matching company name '{search_query_name}'")
                card_data_list = filtered_cards

            # Обновляем агрегированные данные только для отфильтрованных карточек
            self._aggregated_data = {
                'total_cards': 0,
                'total_rating_sum': 0.0,
                'total_reviews_count': 0,
                'total_positive_reviews': 0,
                'total_negative_reviews': 0,
                'total_answered_count': 0,
                'total_answered_reviews_count': 0,
                'total_unanswered_reviews_count': 0,
                'total_response_time_sum_days': 0.0,
                'total_response_time_calculated_count': 0,
            }
            for card_data in card_data_list:
                self._update_aggregated_data(card_data)

            # Заполняем агрегированную статистику для 2ГИС на основе уже собранных карточек.
            # ВАЖНО: здесь мы считаем агрегаты как «сумму по карточкам», чтобы цифры
            # в верхнем блоке (Всего отзывов / Негативных / Позитивных) совпадали
            # с тем, что пользователь видит в списке карточек.
            logger.info(f"Starting aggregation for {len(card_data_list)} 2GIS cards...")
            aggregation_start_time = time.time()
            total_cards = len(card_data_list)
            aggregated_info['total_cards_found'] = total_cards

            # Общее количество отзывов и разбивка по тональности — сумма по карточкам.
            total_reviews = 0
            total_ratings = 0  # Общее количество оценок
            total_positive = 0
            total_negative = 0
            total_neutral = 0  # Нейтральные отзывы (3⭐)
            total_answered = 0
            total_unanswered = 0

            ratings: List[float] = []

            for card in card_data_list:
                # Получаем детальные отзывы для точного подсчета
                detailed_reviews = card.get('detailed_reviews', [])
                if isinstance(detailed_reviews, str):
                    try:
                        detailed_reviews = json.loads(detailed_reviews)
                    except:
                        detailed_reviews = []
                if not isinstance(detailed_reviews, list):
                    detailed_reviews = []
                
                # Всего отзывов по карточке - используем фактическое количество из detailed_reviews
                reviews_cnt = len(detailed_reviews) if detailed_reviews else (card.get('card_reviews_count', 0) or 0)
                total_reviews += reviews_cnt

                # Всего оценок по карточке
                ratings_cnt = card.get('card_ratings_count', 0) or 0
                total_ratings += ratings_cnt

                # Тональность по карточке - пересчитываем из detailed_reviews для точности
                if detailed_reviews:
                    card_positive = sum(1 for r in detailed_reviews if isinstance(r, dict) and r.get('review_rating', 0) >= 4)
                    card_negative = sum(1 for r in detailed_reviews if isinstance(r, dict) and r.get('review_rating', 0) in (1, 2))
                    card_neutral = sum(1 for r in detailed_reviews if isinstance(r, dict) and r.get('review_rating', 0) == 3)
                    total_positive += card_positive
                    total_negative += card_negative
                    total_neutral += card_neutral
                else:
                    # Fallback: используем значения из карточки, если detailed_reviews нет
                    total_positive += card.get('card_reviews_positive', 0) or 0
                    total_negative += card.get('card_reviews_negative', 0) or 0
                    total_neutral += card.get('card_reviews_neutral', 0) or 0

                # Ответы / без ответа по карточке - пересчитываем из detailed_reviews для точности
                if detailed_reviews:
                    card_answered = sum(1 for r in detailed_reviews if isinstance(r, dict) and r.get('has_response', False))
                    card_unanswered = reviews_cnt - card_answered
                    total_answered += card_answered
                    total_unanswered += card_unanswered
                else:
                    # Fallback: используем значения из карточки
                    total_answered += card.get('card_answered_reviews_count', 0) or 0
                    total_unanswered += card.get('card_unanswered_reviews_count', 0) or 0

                # Рейтинг карточки (если его отдал 2ГИС)
                rating_str = str(card.get('card_rating', '')).replace(',', '.').strip()
                try:
                    if rating_str and rating_str.replace('.', '', 1).isdigit():
                        rating_val = float(rating_str)
                        if rating_val > 0:
                            ratings.append(rating_val)
                except (ValueError, TypeError):
                    continue

            aggregated_info['aggregated_reviews_count'] = total_reviews
            aggregated_info['aggregated_ratings_count'] = total_ratings  # Общее количество оценок
            aggregated_info['aggregated_positive_reviews'] = total_positive
            aggregated_info['aggregated_negative_reviews'] = total_negative
            aggregated_info['aggregated_neutral_reviews'] = total_neutral  # Нейтральные отзывы (3⭐)
            aggregated_info['aggregated_answered_reviews_count'] = total_answered
            aggregated_info['aggregated_unanswered_reviews_count'] = total_unanswered

            # Сохраняем общий рейтинг карточки из структуры страницы (если есть только одна карточка)
            if len(card_data_list) == 1:
                single_card_rating = card_data_list[0].get('card_rating', '')
                if single_card_rating:
                    try:
                        rating_val = float(str(single_card_rating).replace(',', '.'))
                        if 1.0 <= rating_val <= 5.0:
                            aggregated_info['aggregated_card_rating_from_page'] = rating_val
                    except (ValueError, TypeError):
                        pass

            # 1) Основной вариант: средний рейтинг по всем карточкам, если 2ГИС отдал рейтинг карточек.
            if ratings:
                aggregated_info['aggregated_rating'] = round(sum(ratings) / len(ratings), 2)

            # 2) Если по карточкам рейтинг недоступен, но у нас есть разбивка по
            #    позитивным/негативным отзывам, считаем примерный общий рейтинг.
            if (
                aggregated_info.get('aggregated_rating', 0) == 0
                and (total_positive + total_negative) > 0
            ):
                pos = total_positive
                neg = total_negative
                total_for_estimate = pos + neg
                # Грубая оценка: негативные (1–2⭐) ~ 2 балла, позитивные (4–5⭐) ~ 4.5 балла.
                approx_rating = (neg * 2.0 + pos * 4.5) / total_for_estimate
                aggregated_info['aggregated_rating'] = round(approx_rating, 2)

            # Среднее время ответа и процент отвеченных — оставляем по агрегированным данным,
            # т.к. они считаются честно по карточкам.
            # Также проверяем среднее время ответа из данных отзывов (если есть)
            if self._aggregated_data['total_response_time_calculated_count'] > 0:
                aggregated_info['aggregated_avg_response_time'] = round(
                    self._aggregated_data['total_response_time_sum_days']
                    / self._aggregated_data['total_response_time_calculated_count'],
                    2,
                )
            else:
                # Если нет данных из агрегации, пробуем взять из данных отзывов
                logger.info("Calculating average response time from detailed reviews (fallback method)...")
                response_times = []
                total_reviews_processed = 0
                for card_idx, card in enumerate(card_data_list, 1):
                    if card_idx % 10 == 0:
                        logger.debug(f"Processing response times: card {card_idx}/{len(card_data_list)}")
                    reviews_data = card.get('detailed_reviews', [])
                    if isinstance(reviews_data, str):
                        try:
                            reviews_data = json.loads(reviews_data)
                        except:
                            reviews_data = []
                    if not isinstance(reviews_data, list):
                        continue
                    total_reviews_processed += len(reviews_data)
                    # Ограничиваем количество обрабатываемых отзывов для ускорения (берем первые 100 отзывов с ответами)
                    reviews_with_response = [r for r in reviews_data if isinstance(r, dict) and r.get('has_response') and r.get('review_date') and r.get('response_date')]
                    for review in reviews_with_response[:100]:  # Ограничиваем до 100 отзывов на карточку
                        try:
                            # datetime уже импортирован глобально, не нужно импортировать локально
                            from src.parsers.date_parser import parse_russian_date
                            review_date = parse_russian_date(review['review_date'])
                            response_date = parse_russian_date(review['response_date'])
                            if review_date and response_date:
                                delta = (response_date - review_date).days
                                if delta >= 0:
                                    response_times.append(delta)
                        except Exception:
                            pass
                logger.info(f"Processed {total_reviews_processed} reviews, found {len(response_times)} response time entries")
                if response_times:
                    aggregated_info['aggregated_avg_response_time'] = round(sum(response_times) / len(response_times), 2)
                    logger.info(f"Calculated average response time: {aggregated_info['aggregated_avg_response_time']} days")
                else:
                    aggregated_info['aggregated_avg_response_time'] = 0.0
                    logger.info("No response time data found in detailed reviews")

            if total_reviews > 0:
                aggregated_info['aggregated_answered_reviews_percent'] = round(
                    (total_answered / total_reviews) * 100,
                    2,
                )

            # Сохраняем данные о датах ответов в файл для последующего расчета
            if hasattr(self, '_response_dates_data') and self._response_dates_data:
                try:
                    with open(self._response_dates_file, 'w', encoding='utf-8') as f:
                        json.dump(self._response_dates_data, f, ensure_ascii=False, indent=2)
                    logger.info(f"Saved {len(self._response_dates_data)} response dates to {self._response_dates_file}")
                    
                    # Пересчитываем среднее время ответа из всех сохраненных данных
                    total_delta = timedelta(0)
                    count = 0
                    for item in self._response_dates_data:
                        try:
                            review_dt = dt_module.datetime.fromisoformat(item['review_date']) if isinstance(item.get('review_date'), str) and 'T' in item['review_date'] else parse_russian_date(item['review_date'])
                            response_dt = dt_module.datetime.fromisoformat(item['response_date']) if isinstance(item.get('response_date'), str) and 'T' in item['response_date'] else parse_russian_date(item['response_date'])
                            if review_dt and response_dt:
                                delta = response_dt - review_dt
                                if delta >= timedelta(0):
                                    total_delta += delta
                                    count += 1
                        except Exception as e:
                            logger.debug(f"Error parsing dates from saved data: {e}")
                            continue
                    
                    if count > 0:
                        avg_time = total_delta / count
                        avg_days = avg_time.total_seconds() / 86400.0
                        logger.info(f"Recalculated average response time from saved data: {avg_days:.2f} days (from {count} reviews)")
                        # Обновляем агрегированные данные, если новый расчет более точный
                        if count > aggregated_info.get('aggregated_answered_reviews_count', 0) * 0.8:  # Если использовано более 80% данных
                            aggregated_info['aggregated_avg_response_time'] = round(avg_days, 2)
                            logger.info(f"Updated aggregated_avg_response_time to {avg_days:.2f} days based on saved data")
                except Exception as e:
                    logger.warning(f"Error saving response dates data: {e}")
            
            aggregation_time = time.time() - aggregation_start_time
            logger.info(f"Aggregation completed in {aggregation_time:.2f} seconds for {len(card_data_list)} cards")
            self._update_progress(f"Агрегация результатов: найдено {len(card_data_list)} карточек")

            logger.info(f"Parsed {len(card_data_list)} cards from 2GIS")

        except Exception as e:
            logger.error(f"Error during 2GIS parsing: {e}", exc_info=True)
            self._update_progress(f"Ошибка: {str(e)}")
            return {
                'cards_data': card_data_list,
                'aggregated_info': aggregated_info,
            }

        return {
            'cards_data': card_data_list,
            'aggregated_info': aggregated_info,
        }
