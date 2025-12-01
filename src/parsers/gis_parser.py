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
        self._reviews_scroll_iterations_max: int = getattr(self._settings.parser, 'gis_reviews_scroll_max_iter', 100)
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
            time.sleep(3)

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
            ]
            for selector in count_selectors:
                for elem in soup_content.select(selector):
                    text = elem.get_text(strip=True)
                    matches = re.findall(r'(\d+)', text)
                    if matches:
                        potential_count = max(int(m) for m in matches)
                        if potential_count > reviews_count_total:
                            reviews_count_total = potential_count

            # Прокручиваем, чтобы подгрузить все отзывы
            self._scroll_to_load_all_reviews()
            page_source, soup_content = self._get_page_source_and_soup()

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

            all_reviews: List[Dict[str, Any]] = []
            response_time_sum_days: float = 0.0
            response_time_count: int = 0
            pages_to_process: List[str] = [reviews_url]
            if all_pages_urls:
                pages_to_process.extend(sorted(all_pages_urls)[:10])

            seen_review_keys: set[str] = set()

            for page_url in pages_to_process:
                if self._is_stopped():
                    logger.info(f"2GIS reviews: stop flag detected before processing reviews page {page_url}, breaking pages loop")
                    break
                try:
                    if page_url != reviews_url:
                        logger.info(f"Processing reviews page: {page_url}")
                        self.driver.navigate(page_url)
                        time.sleep(2)
                        page_source, soup_content = self._get_page_source_and_soup()

                    # Для 2ГИС карточек отзывы лежат в контейнерах div._1k5soqfl
                    review_elements = soup_content.select("div._1k5soqfl")
                    if not review_elements:
                        # Фоллбэк на более общий вариант (если верстка изменится)
                        review_elements = soup_content.select(
                            '[class*="review"], li[class*="review"], div[class*="review"]'
                        )

                    for review_elem in review_elements:
                        if self._is_stopped():
                            logger.info("2GIS reviews: stop flag detected inside reviews loop, breaking")
                            break
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

                        # Рейтинг
                        rating_value = 0.0
                        stars = review_elem.select('[class*="star"], img[class*="icon"]')
                        if stars:
                            filled_stars = len(
                                [
                                    s
                                    for s in stars
                                    if 'active' in str(s.get('class', []))
                                    or 'fill' in str(s.get('class', []))
                                ]
                            )
                            if filled_stars > 0:
                                rating_value = float(filled_stars)

                        if not rating_value:
                            rating_elem = review_elem.select_one(
                                '[class*="rating"], [class*="score"]'
                            )
                            if rating_elem:
                                rating_text = rating_elem.get_text(strip=True)
                                rating_match = re.search(r'(\d+(?:\.\d+)?)', rating_text)
                                if rating_match:
                                    rating_value = float(rating_match.group(1))

                        # Текст отзыва
                        review_text_selectors = [
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
                                review_text = text_element.get_text(separator=' ', strip=True)
                                review_text = ' '.join(review_text.split())
                                if review_text and len(review_text) > 10:
                                    break
                            if review_text and len(review_text) > 10:
                                break

                        if not review_text or len(review_text) < 10:
                            all_text = review_elem.get_text(separator=' ', strip=True)
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
                            cleaned_text = re.sub(
                                r'Полезно|полезно|Оценка|оценка|Отзыв|отзыв|звезд|лайк',
                                '',
                                cleaned_text,
                                flags=re.IGNORECASE,
                            )
                            cleaned_text = ' '.join(cleaned_text.split()).strip()
                            if len(cleaned_text) > 20:
                                review_text = cleaned_text[:1000]

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

                        # Уникальный ключ отзыва, чтобы не дублировать
                        review_key = f"{author_name}_{date_text}_{rating_value}_" \
                            f"{hashlib.md5(review_text[:50].encode('utf-8', errors='ignore')).hexdigest()[:10]}"
                        if review_key in seen_review_keys:
                            continue
                        seen_review_keys.add(review_key)

                        if review_text or rating_value > 0:
                            all_reviews.append(
                                {
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
                            )

                            # Классификация, как и для Яндекса:
                            # 1–2★ — негатив, 3★ — нейтрально, 4–5★ — позитив.
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

                except Exception as page_error:
                    logger.warning(
                        f"Error processing 2GIS reviews page {page_url}: {page_error}",
                        exc_info=True,
                    )
                    continue

            reviews_info['details'] = all_reviews[:500]
            reviews_info['reviews_count'] = (
                len(all_reviews) if all_reviews else reviews_count_total
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
                    time.sleep(1)
                    self._scroll_to_load_all_reviews()
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

    def _scroll_to_load_all_reviews(self) -> None:
        try:
            scroll_iterations = 0
            max_scrolls = self._reviews_scroll_iterations_max
            scroll_step = self._reviews_scroll_step
            no_change_count = 0
            required_no_change = 8
            last_review_count = 0
            
            while scroll_iterations < max_scrolls:
                if self._is_stopped():
                    logger.info("2GIS reviews scroll: stop flag detected, breaking scroll loop")
                    break
                page_source, soup = self._get_page_source_and_soup()
                review_selectors = [
                    'div[class*="review"]',
                    'div[class*="Review"]',
                    'li[class*="review"]',
                ]
                
                current_review_count = 0
                for selector in review_selectors:
                    found = soup.select(selector)
                    current_review_count = max(current_review_count, len(found))
                
                if current_review_count > last_review_count:
                    last_review_count = current_review_count
                    no_change_count = 0
                else:
                    no_change_count += 1
                    if no_change_count >= required_no_change:
                        break
                
                self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(1)
                
                page_source, soup = self._get_page_source_and_soup()
                new_review_count = 0
                for selector in review_selectors:
                    found = soup.select(selector)
                    new_review_count = max(new_review_count, len(found))
                
                if new_review_count == last_review_count:
                    no_change_count += 1
                    if no_change_count >= required_no_change:
                        break
                else:
                    no_change_count = 0
                    last_review_count = new_review_count
                
                scroll_iterations += 1
            
            logger.info(f"Scroll completed. Found {last_review_count} reviews.")
        except Exception as e:
            logger.warning(f"Error scrolling reviews: {e}")

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

        search_query_name = (
            url.split('/search/')[1].split('?')[0].replace('+', ' ').replace('%20', ' ')
            if '/search/' in url else "2gisSearch"
        )

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

                    card_data: Dict[str, Any] = {
                        'card_name': name,
                        'card_address': address,
                        'card_rating': rating,
                        'card_reviews_count': reviews_data.get('reviews_count', 0),
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
                        'detailed_reviews': reviews_data.get('details', []),
                        'source': '2gis',
                    }

                    card_data_list.append(card_data)
                    self._update_aggregated_data(card_data)

                    if len(card_data_list) >= self._max_records:
                        break

                except Exception as e:
                    logger.error(f"Error processing card {card_url}: {e}", exc_info=True)
                    continue

            # Заполняем агрегированную статистику
            total_cards = len(card_data_list)
            aggregated_info['total_cards_found'] = total_cards

            # 1) Основной вариант: средний рейтинг по всем карточкам, если 2ГИС отдал рейтинг карточек.
            if total_cards > 0 and self._aggregated_data['total_rating_sum'] > 0:
                aggregated_info['aggregated_rating'] = round(
                    self._aggregated_data['total_rating_sum'] / total_cards, 2
                )

            aggregated_info['aggregated_reviews_count'] = self._aggregated_data['total_reviews_count']
            aggregated_info['aggregated_positive_reviews'] = self._aggregated_data['total_positive_reviews']
            aggregated_info['aggregated_negative_reviews'] = self._aggregated_data['total_negative_reviews']

            # 2) Если по карточкам рейтинг недоступен, но у нас есть разбивка по
            #    позитивным/негативным отзывам, считаем примерный общий рейтинг.
            if (
                aggregated_info.get('aggregated_rating', 0) == 0
                and (aggregated_info['aggregated_positive_reviews'] + aggregated_info['aggregated_negative_reviews']) > 0
            ):
                pos = aggregated_info['aggregated_positive_reviews']
                neg = aggregated_info['aggregated_negative_reviews']
                total_for_estimate = pos + neg
                # Грубая оценка: негативные (1–3⭐) ~ 2 балла, позитивные (4–5⭐) ~ 4.5 балла.
                approx_rating = (neg * 2.0 + pos * 4.5) / total_for_estimate
                aggregated_info['aggregated_rating'] = round(approx_rating, 2)
            aggregated_info['aggregated_answered_reviews_count'] = self._aggregated_data[
                'total_answered_reviews_count'
            ]
            aggregated_info['aggregated_unanswered_reviews_count'] = self._aggregated_data[
                'total_unanswered_reviews_count'
            ]

            if self._aggregated_data['total_response_time_calculated_count'] > 0:
                aggregated_info['aggregated_avg_response_time'] = round(
                    self._aggregated_data['total_response_time_sum_days']
                    / self._aggregated_data['total_response_time_calculated_count'],
                    2,
                )

            if self._aggregated_data['total_reviews_count'] > 0:
                aggregated_info['aggregated_answered_reviews_percent'] = round(
                    (
                        self._aggregated_data['total_answered_reviews_count']
                        / self._aggregated_data['total_reviews_count']
                    )
                    * 100,
                    2,
                )

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
