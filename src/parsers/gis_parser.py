from __future__ import annotations
import json
import re
import logging
import time
import urllib.parse
import hashlib
import os
from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime, timedelta
from bs4 import BeautifulSoup

from src.drivers.base_driver import BaseDriver
from src.config.settings import Settings
from src.parsers.base_parser import BaseParser
from src.parsers.date_parser import parse_russian_date, format_russian_date

logger = logging.getLogger(__name__)


class GisParser(BaseParser):
    def __init__(self, driver: BaseDriver, settings: Settings):
        super().__init__(driver, settings)
        self._url: str = ""

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
            'positive_reviews': 0,
            'negative_reviews': 0,
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

                ts = datetime.now().strftime("%Y%m%d_%H%M%S")
                debug_path = os.path.join(debug_dir, f"reviews_{firm_id}_{ts}.html")
                with open(debug_path, "w", encoding="utf-8") as f:
                    f.write(page_source)

                logger.info(f"Saved 2GIS reviews debug HTML to {debug_path}")
            except Exception as dump_error:
                logger.warning(f"Could not save 2GIS reviews debug HTML: {dump_error}")

            # Попробуем оценить общее количество отзывов по счетчику
            reviews_count_total = 0
            count_selectors = [
                '[class*="review"][class*="tab"]',
                'a[href*="/tab/reviews"]',
                'meta[name="description"]',  # В meta description часто указывается количество отзывов
            ]
            for selector in count_selectors:
                for elem in soup_content.select(selector):
                    text = elem.get_text(strip=True) if hasattr(elem, 'get_text') else (elem.get('content', '') if hasattr(elem, 'get') else str(elem))
                    matches = re.findall(r'(\d+)', text)
                    if matches:
                        potential_count = max(int(m) for m in matches)
                        if potential_count > reviews_count_total and potential_count < 10000:  # Фильтруем слишком большие числа
                            reviews_count_total = potential_count
            
            # Также пробуем извлечь из meta description
            meta_desc = soup_content.select_one('meta[name="description"]')
            if meta_desc:
                desc_content = meta_desc.get('content', '')
                desc_matches = re.findall(r'(\d+)\s+отзыв', desc_content, re.IGNORECASE)
                if desc_matches:
                    potential_count = max(int(m) for m in desc_matches)
                    if potential_count > reviews_count_total:
                        reviews_count_total = potential_count
            
            logger.info(f"Expected total reviews count: {reviews_count_total}")

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
            response_time_sum_days: float = 0.0
            response_time_count: int = 0
            pages_to_process: List[str] = [reviews_url]
            if all_pages_urls:
                # Увеличиваем количество страниц для обработки, чтобы получить все отзывы
                pages_to_process.extend(sorted(all_pages_urls)[:50])  # Увеличено с 10 до 50
                logger.info(f"Found {len(all_pages_urls)} pagination pages, will process up to 50 pages")

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
                    
                    # Логируем статистику по элементам для отладки
                    skipped_count = 0
                    processed_count = 0
                    
                    if len(review_elements) == 0:
                        # Пробуем найти отзывы через альтернативные методы
                        logger.warning(f"No reviews found with standard selectors on {page_url}")
                        # Сохраняем HTML для отладки
                        try:
                            debug_dir = os.path.join("debug", "2gis_reviews")
                            os.makedirs(debug_dir, exist_ok=True)
                            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
                            debug_path = os.path.join(debug_dir, f"no_reviews_{ts}.html")
                            with open(debug_path, "w", encoding="utf-8") as f:
                                f.write(page_source)
                            logger.info(f"Saved page HTML to {debug_path} for debugging")
                        except Exception as debug_error:
                            logger.warning(f"Could not save debug HTML: {debug_error}")
                        continue

                    for review_elem in review_elements:
                        if self._is_stopped():
                            logger.info("2GIS reviews: stop flag detected inside reviews loop, breaking")
                            break
                        
                        # Пропускаем элементы, которые явно не являются отзывами
                        elem_text = review_elem.get_text(strip=True)
                        # Ослабляем фильтр: принимаем элементы с текстом от 3 символов (было 5)
                        # Это позволит не пропускать короткие, но валидные отзывы
                        if not elem_text or len(elem_text) < 3:
                            if processed_count <= 5:  # Логируем только первые несколько пропусков
                                logger.debug(f"Skipping element: text too short ({len(elem_text)} chars)")
                            skipped_count += 1
                            continue
                        # Пропускаем элементы, которые выглядят как навигация или другие служебные элементы
                        if any(skip_word in elem_text.lower() for skip_word in ['читать целиком', 'показать еще', 'следующая', 'предыдущая', 'страница']):
                            skipped_count += 1
                            continue
                        
                        # НЕ пропускаем элементы на этом этапе - проверка ответов компании будет позже
                        # после извлечения текста отзыва, чтобы не пропускать валидные отзывы
                        
                        processed_count += 1
                        
                        # Автор
                        author_elem = review_elem.select_one(
                            '[class*="author"], [class*="user"], [class*="name"]'
                        )
                        author_name = author_elem.get_text(strip=True) if author_elem else ""
                        if not author_name:
                            all_text = review_elem.get_text()
                            name_match = re.search(
                                r'([А-ЯЁ][а-яё]+\s+[А-ЯЁ][а-яё]+|[А-ЯЁ][а-яё]+)', all_text
                            )
                            if name_match and len(name_match.group(1)) > 2:
                                author_name = name_match.group(1)
                        if not author_name:
                            author_name = "Аноним"

                        # Дата отзыва
                        date_elem = review_elem.select_one('[class*="date"], time, [class*="time"]')
                        review_date: Optional[datetime] = None
                        date_text = ""
                        if date_elem:
                            date_text = date_elem.get_text(strip=True)
                            datetime_attr = date_elem.get('datetime', '')
                            if datetime_attr:
                                try:
                                    review_date = datetime.fromisoformat(
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

                        # Текст отзыва - пробуем различные селекторы
                        # Приоритет: сначала специфичные классы 2GIS, потом общие
                        review_text_selectors = [
                            '[class*="_1wlx08h"]',  # Класс для текста отзыва в 2GIS (сокращенный)
                            '[class*="_msln3t"]',  # Класс для полного текста отзыва в 2GIS
                            'div[class*="_kcpnuw"]',  # Класс для контента отзыва
                            'div[class*="_1wk3bjs"]',  # Класс для текста ответа/отзыва
                            'div[class*="text"]',
                            '[class*="content"]',
                            '[class*="comment"]',
                            'p[class*="text"]',
                            'div[class*="review-text"]',
                            '[class*="text"][class*="review"]',
                        ]
                        review_text = ""
                        for text_selector in review_text_selectors:
                            text_elements = review_elem.select(text_selector)
                            for text_element in text_elements:
                                candidate_text = text_element.get_text(separator=' ', strip=True)
                                candidate_text = ' '.join(candidate_text.split())
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
                                cleaned_text = ' '.join(cleaned_text.split()).strip()
                                
                                # Принимаем текст, если он длиннее 3 символов
                                # Но проверяем, что это не только метаданные (даты, имена)
                                if len(cleaned_text) >= 3:
                                    # Если текст очень короткий (меньше 15 символов), проверяем, что это не только метаданные
                                    if len(cleaned_text) >= 15 or not re.match(r'^[\d\sа-яёА-ЯЁ,\.\-]+$', cleaned_text):
                                        review_text = cleaned_text

                        # Ответ организации
                        answer_elem = review_elem.select_one(
                            '[class*="answer"], [class*="reply"], [class*="response"]'
                        )
                        has_response = bool(answer_elem)
                        response_text = ""
                        response_date: Optional[datetime] = None

                        if answer_elem:
                            response_text_elem = answer_elem.select_one(
                                '[class*="text"], [class*="content"]'
                            )
                            if response_text_elem:
                                response_text = response_text_elem.get_text(strip=True)
                            else:
                                response_text = answer_elem.get_text(strip=True)

                            response_date_elem = answer_elem.select_one(
                                '[class*="date"], time'
                            )
                            if response_date_elem:
                                response_date_text = response_date_elem.get_text(strip=True)
                                response_date = parse_russian_date(response_date_text)

                        # 2ГИС часто помечает официальный ответ только текстом
                        # вида "29 мая 2025, официальный ответ" без специальных классов.
                        # Если специальных блоков нет, пробуем найти такой текст в общем содержимом.
                        if not has_response:
                            full_text = review_elem.get_text(separator=' ', strip=True)
                            full_text_lower = full_text.lower()
                            if 'официальный ответ' in full_text_lower:
                                has_response = True
                                # Пытаемся вытащить дату ответа из фрагмента "DD месяц YYYY, официальный ответ"
                                m_resp = re.search(
                                    r'(\d{1,2}\s+[а-яё]+\s+\d{4}).{0,40}официальный ответ',
                                    full_text_lower,
                                    re.IGNORECASE,
                                )
                                if m_resp:
                                    try:
                                        response_date = parse_russian_date(m_resp.group(1))
                                    except Exception:
                                        response_date = None

                        # Логируем информацию о каждом обработанном элементе (до проверки дубликатов)
                        if processed_count <= 20:  # Логируем первые 20 для отладки
                            logger.debug(
                                f"Processing review element {processed_count}: "
                                f"author={author_name}, rating={rating_value}, "
                                f"text_len={len(review_text)}, has_text={bool(review_text)}, "
                                f"date={date_text}"
                            )
                        
                        # Уникальный ключ отзыва, чтобы не дублировать
                        review_key = f"{author_name}_{date_text}_{rating_value}_" \
                            f"{hashlib.md5(review_text[:50].encode('utf-8', errors='ignore')).hexdigest()[:10]}"
                        if review_key in seen_review_keys:
                            if processed_count <= 20 or skipped_count % 10 == 0:
                                logger.debug(f"Skipping duplicate review: key={review_key[:50]}")
                            skipped_count += 1  # Учитываем дубликаты в статистике пропусков
                            continue
                        seen_review_keys.add(review_key)
                        
                        # Убираем чисто служебные тексты вида "Подписаться", "Полезно?" и т.п.
                        # (аналогично Яндекс парсеру)
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
                        
                        # Если это ответ компании (даже с рейтингом), не добавляем в список отзывов
                        # НЕ учитываем в answered_count, так как это не отзыв пользователя
                        if is_company_response or (has_response and rating_value == 0 and (not review_text or len(review_text) < 10)):
                            if processed_count <= 20 or skipped_count % 10 == 0:
                                logger.debug(f"Found company response (not a user review): author={author_name}, text_preview={review_text[:50] if review_text else 'N/A'}")
                            # НЕ увеличиваем answered_count, так как это не отзыв пользователя
                            # Просто пропускаем этот элемент
                            skipped_count += 1
                            continue
                        
                        # Ослабляем проверку: принимаем отзывы с текстом от 3 символов или с рейтингом
                        if (review_text and len(review_text) >= 3) or rating_value > 0:
                            review_data = {
                                'review_rating': rating_value,
                                'review_text': review_text or "",
                                'review_author': author_name or "Аноним",
                                'review_date': format_russian_date(review_date)
                                if review_date
                                else (date_text or ""),
                                'has_response': has_response,
                                'response_text': response_text,
                                'response_date': format_russian_date(response_date)
                                if response_date
                                else "",
                            }
                            all_reviews.append(review_data)
                            
                            # Классификация, как и для Яндекса:
                            # 1–2★ — негатив, 3★ — нейтрально, 4–5★ — позитив.
                            # ВАЖНО: классифицируем ТОЛЬКО отзывы с рейтингом > 0
                            if rating_value > 0:
                                if rating_value >= 4:
                                    reviews_info['positive_reviews'] += 1
                                elif rating_value in (1, 2):
                                    reviews_info['negative_reviews'] += 1
                            
                            if has_response:
                                reviews_info['answered_count'] += 1
                            else:
                                reviews_info['unanswered_count'] += 1

                            # Накапливаем время ответа для расчёта среднего
                            if has_response and review_date and response_date:
                                try:
                                    delta = (response_date - review_date).days
                                    if delta >= 0:
                                        response_time_sum_days += float(delta)
                                        response_time_count += 1
                                except Exception:
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

            # Сохраняем все отзывы без ограничения (было [:500])
            reviews_info['details'] = all_reviews
            reviews_info['reviews_count'] = (
                len(all_reviews) if all_reviews else reviews_count_total
            )
            
            # Логируем статистику по найденным отзывам
            reviews_with_rating = sum(1 for r in all_reviews if r.get('review_rating', 0) > 0)
            reviews_with_text = sum(1 for r in all_reviews if r.get('review_text', '').strip())
            logger.info(
                f"2GIS reviews extraction completed: total={len(all_reviews)}, "
                f"with_rating={reviews_with_rating}, with_text={reviews_with_text}, "
                f"positive={reviews_info['positive_reviews']}, "
                f"negative={reviews_info['negative_reviews']}"
            )
            if response_time_count > 0:
                reviews_info['avg_response_time_days'] = round(
                    response_time_sum_days / response_time_count, 2
                )
            else:
                reviews_info['avg_response_time_days'] = 0.0

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
            reviews_count = card_data.get('card_reviews_count', 0) or 0
            positive_reviews = card_data.get('card_reviews_positive', 0) or 0
            negative_reviews = card_data.get('card_reviews_negative', 0) or 0
            answered_reviews = card_data.get('card_answered_reviews_count', 0) or 0

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

    def parse(self, url: str) -> Dict[str, Any]:
        logger.info(f"Starting 2GIS parser for URL: {url}")
        self._url = url

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

            self._update_progress(f"Сканирование карточек: 0/{len(card_urls)}")

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

            for idx, card_url in enumerate(card_urls[: self._max_records], start=1):
                if self._is_stopped():
                    logger.info(f"2GIS: stop flag detected before processing card {idx}, breaking cards loop")
                    break
                try:
                    self._update_progress(
                        f"Сканирование карточек: {idx}/{min(len(card_urls), self._max_records)}"
                    )

                    logger.info(
                        f"Processing card {idx}/{min(len(card_urls), self._max_records)}: {card_url}"
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

                    # Телефон
                    phone = ""
                    for selector in phone_selectors:
                        phone_elem = soup.select_one(selector)
                        if phone_elem:
                            phone = phone_elem.get_text(strip=True)
                            if not phone and phone_elem.get('href'):
                                phone = phone_elem.get('href').replace('tel:', '').strip()
                            if phone:
                                break

                    reviews_data = self._get_card_reviews_info_2gis(card_url)

                    if not name:
                        logger.warning(f"Skipping 2GIS card without name: {card_url}")
                        continue

                    # Используем фактическое количество отзывов из details, а не reviews_count
                    # Это гарантирует, что card_reviews_count соответствует количеству detailed_reviews
                    detailed_reviews_list = reviews_data.get('details', [])
                    actual_reviews_count = len(detailed_reviews_list) if detailed_reviews_list else reviews_data.get('reviews_count', 0)
                    
                    card_data: Dict[str, Any] = {
                        'card_name': name,
                        'card_address': address,
                        'card_rating': rating,
                        'card_reviews_count': actual_reviews_count,
                        'card_website': "",
                        'card_phone': phone,
                        'card_rubrics': "",
                        'card_response_status': "UNKNOWN",
                        'card_avg_response_time': reviews_data.get('avg_response_time_days', 0.0),
                        'card_reviews_positive': reviews_data.get('positive_reviews', 0),
                        'card_reviews_negative': reviews_data.get('negative_reviews', 0),
                        'card_reviews_texts': "; ".join(reviews_data.get('texts', [])),
                        'card_answered_reviews_count': reviews_data.get('answered_count', 0),
                        'card_unanswered_reviews_count': reviews_data.get('unanswered_count', 0),
                        'detailed_reviews': detailed_reviews_list,
                        'source': '2gis',
                    }
                    
                    # Логируем количество отзывов в карточке для отладки
                    detailed_reviews_count = len(card_data.get('detailed_reviews', []))
                    if detailed_reviews_count > 0:
                        logger.info(f"Card '{name}': {detailed_reviews_count} detailed reviews saved to card_data")

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
            total_positive = 0
            total_negative = 0
            total_answered = 0
            total_unanswered = 0

            ratings: List[float] = []

            for card in card_data_list:
                # Всего отзывов по карточке
                reviews_cnt = card.get('card_reviews_count', 0) or 0
                total_reviews += reviews_cnt

                # Тональность по карточке
                total_positive += card.get('card_reviews_positive', 0) or 0
                total_negative += card.get('card_reviews_negative', 0) or 0

                # Ответы / без ответа по карточке
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
            aggregated_info['aggregated_positive_reviews'] = total_positive
            aggregated_info['aggregated_negative_reviews'] = total_negative
            aggregated_info['aggregated_answered_reviews_count'] = total_answered
            aggregated_info['aggregated_unanswered_reviews_count'] = total_unanswered

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
                            import json
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
                            from datetime import datetime
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
