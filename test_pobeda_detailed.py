"""
Детальный тестовый скрипт для парсинга компании "Победа" с сайтом http://pbd.space
Проверяет фильтрацию по сайту, точность данных и сравнивает с реальными результатами
"""
import os
import sys
import time
import logging
from pathlib import Path
from typing import Dict, Any, List

# Добавляем корневую директорию в путь
sys.path.insert(0, str(Path(__file__).parent))

from src.config.settings import Settings
from src.drivers.selenium_driver import SeleniumDriver
from src.parsers.yandex_parser import YandexParser
from src.parsers.gis_parser import GisParser

# Настраиваем детальное логирование
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)-8s | %(name)s | %(message)s',
    datefmt='%d/%m/%Y %H:%M:%S'
)
logger = logging.getLogger(__name__)

# Включаем DEBUG для парсеров
logging.getLogger('src.parsers').setLevel(logging.DEBUG)
logging.getLogger('src.drivers').setLevel(logging.INFO)


def normalize_website_for_comparison(website: str) -> str:
    """Нормализует URL для сравнения"""
    if not website:
        return ""
    import re
    website = website.lower().strip()
    website = re.sub(r'^https?://', '', website)
    website = re.sub(r'^www\.', '', website)
    website = website.rstrip('/')
    website = website.split('/')[0]
    website = website.split('?')[0]
    return website


def filter_cards_by_website(cards: List[Dict[str, Any]], target_website: str) -> List[Dict[str, Any]]:
    """Фильтрует карточки по сайту"""
    if not target_website or not cards:
        return cards
    
    normalized_target = normalize_website_for_comparison(target_website)
    if not normalized_target:
        return cards
    
    logger.info(f"Фильтрация карточек по сайту: целевой '{target_website}' (нормализован: '{normalized_target}')")
    
    filtered = []
    skipped_no_website = 0
    skipped_mismatch = 0
    
    for card in cards:
        card_website = card.get('card_website', '')
        card_name = card.get('card_name', 'N/A')
        
        if not card_website:
            skipped_no_website += 1
            logger.debug(f"Карточка '{card_name}' исключена: сайт не указан")
            continue
        
        normalized_card = normalize_website_for_comparison(card_website)
        if normalized_card == normalized_target:
            filtered.append(card)
            logger.info(f"✓ Карточка '{card_name}' прошла фильтр: сайт '{card_website}'")
        else:
            skipped_mismatch += 1
            logger.debug(f"Карточка '{card_name}' исключена: сайт '{card_website}' (нормализован: '{normalized_card}') != '{normalized_target}'")
    
    logger.info(f"Фильтрация завершена: {len(cards)} -> {len(filtered)} карточек (пропущено без сайта: {skipped_no_website}, не совпадает: {skipped_mismatch})")
    return filtered


def print_card_details(card: Dict[str, Any], index: int):
    """Выводит детальную информацию о карточке"""
    print(f"\n{'='*80}")
    print(f"КАРТОЧКА #{index}")
    print(f"{'='*80}")
    print(f"Название: {card.get('card_name', 'N/A')}")
    print(f"Адрес: {card.get('card_address', 'N/A')}")
    print(f"Рейтинг: {card.get('card_rating', 'N/A')}")
    print(f"Отзывов: {card.get('card_reviews_count', 0)}")
    print(f"Сайт: {card.get('card_website', 'НЕ УКАЗАН')}")
    print(f"Телефон: {card.get('card_phone', 'N/A')}")
    print(f"Источник: {card.get('source', 'N/A')}")
    print(f"Положительных отзывов: {card.get('card_reviews_positive', 0)}")
    print(f"Отрицательных отзывов: {card.get('card_reviews_negative', 0)}")
    print(f"Отвечено на: {card.get('card_answered_reviews_count', 0)}")
    print(f"Не отвечено: {card.get('card_unanswered_reviews_count', 0)}")
    
    detailed_reviews = card.get('detailed_reviews', [])
    if detailed_reviews:
        print(f"\nДетальные отзывы ({len(detailed_reviews)}):")
        for i, review in enumerate(detailed_reviews[:5], 1):  # Показываем первые 5
            print(f"  {i}. Рейтинг: {review.get('review_rating', 'N/A')}, "
                  f"Автор: {review.get('review_author', 'N/A')}, "
                  f"Дата: {review.get('review_date', 'N/A')}")
            if review.get('review_text'):
                text = review.get('review_text', '')[:100]
                print(f"     Текст: {text}...")
        if len(detailed_reviews) > 5:
            print(f"  ... и еще {len(detailed_reviews) - 5} отзывов")


def test_parsing():
    """Тестирует парсинг компании 'Победа' с сайтом http://pbd.space"""
    
    logger.info("=" * 80)
    logger.info("ТЕСТ ПАРСИНГА: Победа, сайт http://pbd.space")
    logger.info("Города: Москва, Санкт-Петербург")
    logger.info("=" * 80)
    
    settings = Settings()
    company_name = "Победа"
    company_site = "http://pbd.space"
    cities = ["Москва", "Санкт-Петербург"]
    
    driver = None
    try:
        logger.info("Инициализация Selenium драйвера...")
        driver = SeleniumDriver(settings=settings)
        driver.start()
        logger.info("Драйвер запущен успешно")
        
        all_cards = []
        all_statistics = {}
        
        for city in cities:
            logger.info("\n" + "=" * 80)
            logger.info(f"ГОРОД: {city}")
            logger.info("=" * 80)
            
            # --- Яндекс Карты ---
            logger.info(f"\n--- Яндекс Карты ({city}) ---")
            yandex_url = f"https://yandex.ru/maps/?text={company_name} {city}"
            logger.info(f"URL: {yandex_url}")
            
            yandex_parser = YandexParser(driver=driver, settings=settings)
            yandex_result = yandex_parser.parse(url=yandex_url, search_query_site=company_site)
            
            if yandex_result and yandex_result.get("cards_data"):
                cards = yandex_result["cards_data"]
                logger.info(f"Найдено карточек Яндекс (до фильтрации): {len(cards)}")
                
                # Показываем сайты всех карточек до фильтрации
                for i, card in enumerate(cards[:10], 1):
                    card_site = card.get('card_website', 'НЕТ')
                    logger.info(f"  Карточка {i}: '{card.get('card_name', 'N/A')}' | Сайт: {card_site}")
                
                # Фильтруем по сайту
                original_count = len(cards)
                cards = filter_cards_by_website(cards, company_site)
                filtered_count = len(cards)
                
                if original_count != filtered_count:
                    logger.info(f"✓ Отфильтровано по сайту: {filtered_count} из {original_count} карточек")
                
                for card in cards:
                    card["source"] = "yandex"
                    card["city"] = city
                all_cards.extend(cards)
                
                if yandex_result.get("aggregated_info"):
                    key = f"yandex_{city}"
                    all_statistics[key] = yandex_result["aggregated_info"]
                    logger.info(f"Статистика Яндекс ({city}): {yandex_result['aggregated_info']}")
            else:
                logger.warning(f"Яндекс ({city}): Карточки не найдены или произошла ошибка")
            
            # --- 2GIS ---
            logger.info(f"\n--- 2GIS ({city}) ---")
            import urllib.parse
            encoded_name = urllib.parse.quote(f"{company_name} {city}")
            encoded_site = urllib.parse.quote(company_site)
            gis_url = f"https://2gis.ru/search/{encoded_name}?search_source=main&company_website={encoded_site}"
            logger.info(f"URL: {gis_url}")
            
            gis_parser = GisParser(driver=driver, settings=settings)
            gis_result = gis_parser.parse(url=gis_url, search_query_site=company_site)
            
            if gis_result and gis_result.get("cards_data"):
                cards = gis_result["cards_data"]
                logger.info(f"Найдено карточек 2GIS (до фильтрации): {len(cards)}")
                
                # Показываем сайты всех карточек до фильтрации
                for i, card in enumerate(cards[:10], 1):
                    card_site = card.get('card_website', 'НЕТ')
                    logger.info(f"  Карточка {i}: '{card.get('card_name', 'N/A')}' | Сайт: {card_site}")
                
                # Фильтруем по сайту
                original_count = len(cards)
                cards = filter_cards_by_website(cards, company_site)
                filtered_count = len(cards)
                
                if original_count != filtered_count:
                    logger.info(f"✓ Отфильтровано по сайту: {filtered_count} из {original_count} карточек")
                
                for card in cards:
                    card["source"] = "2gis"
                    card["city"] = city
                all_cards.extend(cards)
                
                if gis_result.get("aggregated_info"):
                    key = f"2gis_{city}"
                    all_statistics[key] = gis_result["aggregated_info"]
                    logger.info(f"Статистика 2GIS ({city}): {gis_result['aggregated_info']}")
            else:
                logger.warning(f"2GIS ({city}): Карточки не найдены или произошла ошибка")
            
            # Пауза между городами
            if city != cities[-1]:
                logger.info(f"\nПауза 5 секунд перед следующим городом...")
                time.sleep(5)
        
        # --- ИТОГОВЫЕ РЕЗУЛЬТАТЫ ---
        logger.info("\n" + "=" * 80)
        logger.info("ИТОГОВЫЕ РЕЗУЛЬТАТЫ")
        logger.info("=" * 80)
        
        logger.info(f"\nВсего найдено и отфильтровано карточек: {len(all_cards)}")
        
        yandex_cards = [c for c in all_cards if c.get('source') == 'yandex']
        gis_cards = [c for c in all_cards if c.get('source') == '2gis']
        
        logger.info(f"  Яндекс: {len(yandex_cards)} карточек")
        logger.info(f"  2GIS: {len(gis_cards)} карточек")
        
        # Группировка по городам
        by_city = {}
        for card in all_cards:
            city = card.get('city', 'Не указан')
            if city not in by_city:
                by_city[city] = {'yandex': [], '2gis': []}
            source = card.get('source', 'unknown')
            if source in by_city[city]:
                by_city[city][source].append(card)
        
        logger.info(f"\nПо городам:")
        for city, cards_dict in by_city.items():
            logger.info(f"  {city}:")
            logger.info(f"    Яндекс: {len(cards_dict['yandex'])} карточек")
            logger.info(f"    2GIS: {len(cards_dict['2gis'])} карточек")
        
        # Детальная информация по каждой карточке
        print("\n" + "=" * 80)
        print("ДЕТАЛЬНАЯ ИНФОРМАЦИЯ ПО КАРТОЧКАМ")
        print("=" * 80)
        
        for i, card in enumerate(all_cards, 1):
            print_card_details(card, i)
        
        # Сводная статистика
        print("\n" + "=" * 80)
        print("СВОДНАЯ СТАТИСТИКА")
        print("=" * 80)
        for key, stats in all_statistics.items():
            print(f"\n{key}:")
            print(f"  Всего карточек: {stats.get('total_cards_found', 0)}")
            print(f"  Рейтинг: {stats.get('aggregated_rating', 0)}")
            print(f"  Отзывов: {stats.get('aggregated_reviews_count', 0)}")
            print(f"  Положительных: {stats.get('aggregated_positive_reviews', 0)}")
            print(f"  Отрицательных: {stats.get('aggregated_negative_reviews', 0)}")
        
        logger.info("\n" + "=" * 80)
        logger.info("ТЕСТ ЗАВЕРШЕН")
        logger.info("=" * 80)
        
        # Рекомендации для сравнения
        print("\n" + "=" * 80)
        print("РЕКОМЕНДАЦИИ ДЛЯ СРАВНЕНИЯ С РЕАЛЬНЫМИ ДАННЫМИ")
        print("=" * 80)
        print("1. Проверьте на Яндекс.Картах:")
        print(f"   https://yandex.ru/maps/?text=Победа Москва")
        print(f"   https://yandex.ru/maps/?text=Победа Санкт-Петербург")
        print("2. Проверьте на 2ГИС:")
        print(f"   https://2gis.ru/search/Победа Москва")
        print(f"   https://2gis.ru/search/Победа Санкт-Петербург")
        print("3. Сравните количество карточек с сайтом http://pbd.space")
        print("4. Проверьте точность данных: рейтинги, количество отзывов, адреса")
        
    except Exception as e:
        logger.error(f"Произошла ошибка во время теста: {e}", exc_info=True)
    finally:
        if driver:
            logger.info("Остановка драйвера...")
            driver.stop()
            logger.info("Драйвер остановлен")


if __name__ == "__main__":
    test_parsing()

