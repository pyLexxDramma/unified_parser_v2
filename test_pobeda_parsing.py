"""
Тестовый скрипт для проверки парсинга по фирме "Победа" с сайтом http://pbd.space
"""
import os
import sys
import time
import logging
from pathlib import Path

# Добавляем корневую директорию в путь
sys.path.insert(0, str(Path(__file__).parent))

from src.config.settings import Settings
from src.drivers.selenium_driver import SeleniumDriver
from src.parsers.yandex_parser import YandexParser
from src.parsers.gis_parser import GisParser

# Настраиваем логирование
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)-8s | %(message)s',
    datefmt='%d/%m/%Y %H:%M:%S'
)
logger = logging.getLogger(__name__)

def test_parsing():
    """Тестирует парсинг по фирме 'Победа' с сайтом http://pbd.space"""
    
    logger.info("=" * 80)
    logger.info("Тест парсинга: Победа, сайт http://pbd.space")
    logger.info("=" * 80)
    
    # Загружаем настройки
    settings = Settings()
    
    company_name = "Победа"
    company_site = "http://pbd.space"
    
    driver = None
    try:
        # Инициализируем драйвер
        logger.info("Инициализация Selenium драйвера...")
        driver = SeleniumDriver(settings=settings)
        driver.start()
        logger.info("Драйвер запущен успешно")
        
        # Тест Яндекс
        logger.info("\n" + "=" * 80)
        logger.info("ТЕСТ 1: Яндекс Карты")
        logger.info("=" * 80)
        
        yandex_url = f"https://yandex.ru/maps/?text={company_name}"
        logger.info(f"URL Яндекс: {yandex_url}")
        
        yandex_parser = YandexParser(driver=driver, settings=settings)
        yandex_result = yandex_parser.parse(url=yandex_url)
        
        if yandex_result and yandex_result.get("cards_data"):
            yandex_cards = yandex_result["cards_data"]
            logger.info(f"Найдено карточек Яндекс: {len(yandex_cards)}")
            
            # Фильтруем по сайту
            from src.webapp.app import _filter_cards_by_website, _normalize_website_for_comparison
            
            normalized_target = _normalize_website_for_comparison(company_site)
            logger.info(f"Целевой сайт (нормализованный): {normalized_target}")
            
            filtered_yandex = _filter_cards_by_website(yandex_cards, company_site)
            logger.info(f"После фильтрации по сайту: {len(filtered_yandex)} карточек")
            
            # Выводим информацию о карточках
            for i, card in enumerate(filtered_yandex[:5], 1):
                card_name = card.get('card_name', 'N/A')
                card_site = card.get('card_website', 'N/A')
                logger.info(f"  {i}. {card_name} | Сайт: {card_site}")
        else:
            logger.warning("Яндекс: карточки не найдены")
        
        # Тест 2GIS
        logger.info("\n" + "=" * 80)
        logger.info("ТЕСТ 2: 2GIS")
        logger.info("=" * 80)
        
        import urllib.parse
        encoded_name = urllib.parse.quote(company_name)
        encoded_site = urllib.parse.quote(company_site)
        gis_url = f"https://2gis.ru/search/{encoded_name}?search_source=main&company_website={encoded_site}"
        logger.info(f"URL 2GIS: {gis_url}")
        
        gis_parser = GisParser(driver=driver, settings=settings)
        gis_result = gis_parser.parse(url=gis_url)
        
        if gis_result and gis_result.get("cards_data"):
            gis_cards = gis_result["cards_data"]
            logger.info(f"Найдено карточек 2GIS: {len(gis_cards)}")
            
            # Фильтруем по сайту
            filtered_gis = _filter_cards_by_website(gis_cards, company_site)
            logger.info(f"После фильтрации по сайту: {len(filtered_gis)} карточек")
            
            # Выводим информацию о карточках
            for i, card in enumerate(filtered_gis[:5], 1):
                card_name = card.get('card_name', 'N/A')
                card_site = card.get('card_website', 'N/A')
                logger.info(f"  {i}. {card_name} | Сайт: {card_site}")
        else:
            logger.warning("2GIS: карточки не найдены")
        
        logger.info("\n" + "=" * 80)
        logger.info("Тест завершен")
        logger.info("=" * 80)
        
    except Exception as e:
        logger.error(f"Ошибка при тестировании: {e}", exc_info=True)
    finally:
        if driver:
            logger.info("Остановка драйвера...")
            driver.stop()
            logger.info("Драйвер остановлен")

if __name__ == "__main__":
    test_parsing()



