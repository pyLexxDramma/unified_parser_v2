#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Скрипт для парсинга всех отзывов компании "Смарт Хоум" в Санкт-Петербурге
Сайт: http://smarthome.spb.ru
Цель: собрать все 319 отзывов и сохранить в JSON
"""

import json
import os
import sys
import logging
from datetime import datetime
from typing import Dict, Any, List

# Добавляем корневую директорию в путь
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from src.drivers.selenium_driver import SeleniumDriver
from src.parsers.gis_parser import GisParser
from src.config.settings import Settings

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)-8s | %(message)s',
    datefmt='%d/%m/%Y %H:%M:%S'
)
logger = logging.getLogger(__name__)


def generate_gis_url(company_name: str, company_site: str, city: str) -> str:
    """Генерирует URL для поиска в 2GIS"""
    import urllib.parse
    # Используем только название компании для более точного поиска
    query = company_name
    encoded_query = urllib.parse.quote(query)
    return f"https://2gis.ru/spb/search/{encoded_query}"


def save_to_json(data: Dict[str, Any], filename: str):
    """Сохраняет данные в JSON файл"""
    output_dir = "output"
    os.makedirs(output_dir, exist_ok=True)
    
    filepath = os.path.join(output_dir, filename)
    
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    
    logger.info(f"✓ Данные сохранены в: {filepath}")
    return filepath


def parse_company_reviews():
    """Основная функция парсинга"""
    company_name = "Смарт Хоум"
    company_site = "smarthome.spb.ru"
    city = "Санкт-Петербург"
    target_reviews = 319  # Целевое количество отзывов
    
    logger.info("=" * 80)
    logger.info(f"ПАРСИНГ ОТЗЫВОВ: {company_name}")
    logger.info(f"Город: {city}")
    logger.info(f"Сайт: {company_site}")
    logger.info(f"Цель: собрать {target_reviews} отзывов из 2GIS")
    logger.info("=" * 80)
    
    settings = Settings()
    driver = None
    
    all_reviews = []
    all_cards = []
    
    try:
        # Инициализация драйвера
        logger.info("Инициализация драйвера...")
        driver = SeleniumDriver(settings=settings)
        driver.start()
        logger.info("✓ Драйвер запущен")
        
        # Парсинг только 2GIS
        logger.info("\n" + "=" * 80)
        logger.info("ПАРСИНГ 2GIS (ТОЛЬКО 2GIS)")
        logger.info("=" * 80)
        
        gis_url = generate_gis_url(company_name, company_site, city)
        logger.info(f"URL: {gis_url}")
        
        gis_parser = GisParser(driver=driver, settings=settings)
        # Отключаем фильтрацию по сайту для 2GIS, чтобы собрать все отзывы
        # (проблема: link.2gis.ru может вести на разные сайты, но нам нужны все отзывы)
        gis_result = gis_parser.parse(
            gis_url,
            search_query_site=None,  # Отключаем фильтрацию по сайту
            search_query_address=None
        )
        
        if gis_result and gis_result.get("cards_data"):
            gis_cards = gis_result.get("cards_data", [])
            logger.info(f"✓ 2GIS: найдено {len(gis_cards)} карточек")
            
            for card in gis_cards:
                card["source"] = "2gis"
                all_cards.append(card)
                
                # Собираем все отзывы из карточки
                reviews = card.get("detailed_reviews", [])
                for review in reviews:
                    review["card_url"] = card.get("card_url", "")
                    review["card_name"] = card.get("card_name", "")
                    review["source"] = "2gis"
                    all_reviews.append(review)
            
            gis_reviews_count = len([r for r in all_reviews if r.get('source') == '2gis'])
            logger.info(f"✓ 2GIS: собрано {gis_reviews_count} отзывов")
            
            if gis_reviews_count < target_reviews:
                logger.warning(f"⚠ ВНИМАНИЕ: собрано {gis_reviews_count} отзывов, но цель была {target_reviews}")
                logger.info("Проверяю, возможно нужно обработать больше страниц отзывов...")
        else:
            logger.warning("⚠ 2GIS: карточки не найдены")
        
        # Итоговая статистика
        logger.info("\n" + "=" * 80)
        logger.info("ИТОГОВАЯ СТАТИСТИКА")
        logger.info("=" * 80)
        logger.info(f"Всего карточек: {len(all_cards)}")
        logger.info(f"Всего отзывов: {len(all_reviews)}")
        logger.info(f"  - 2GIS: {len([r for r in all_reviews if r.get('source') == '2gis'])}")
        
        # Проверка на целевое количество отзывов
        if len(all_reviews) >= target_reviews:
            logger.info(f"✓ ЦЕЛЬ ДОСТИГНУТА: собрано {len(all_reviews)} отзывов (цель: {target_reviews})")
        else:
            logger.warning(f"⚠ ВНИМАНИЕ: собрано {len(all_reviews)} отзывов, но цель была {target_reviews}")
        
        # Формирование JSON структуры
        result_data = {
            "company": {
                "name": company_name,
                "site": company_site,
                "city": city
            },
            "parsing_info": {
                "date": datetime.now().isoformat(),
                "total_cards": len(all_cards),
                "total_reviews": len(all_reviews),
                "target_reviews": target_reviews,
                "sources": {
                    "2gis": {
                        "cards": len([c for c in all_cards if c.get("source") == "2gis"]),
                        "reviews": len([r for r in all_reviews if r.get("source") == "2gis"])
                    }
                }
            },
            "cards": all_cards,
            "reviews": all_reviews
        }
        
        # Сохранение в JSON
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"smarthome_reviews_{timestamp}.json"
        filepath = save_to_json(result_data, filename)
        
        logger.info("\n" + "=" * 80)
        logger.info("ПАРСИНГ ЗАВЕРШЕН")
        logger.info("=" * 80)
        logger.info(f"Файл сохранен: {filepath}")
        logger.info(f"Размер файла: {os.path.getsize(filepath) / 1024:.2f} KB")
        
        return result_data
        
    except Exception as e:
        logger.error(f"ОШИБКА при парсинге: {e}", exc_info=True)
        raise
    finally:
        if driver:
            try:
                driver.stop()
                logger.info("✓ Драйвер остановлен")
            except Exception as e:
                logger.warning(f"Ошибка при остановке драйвера: {e}")


if __name__ == "__main__":
    try:
        result = parse_company_reviews()
        logger.info("\n✓ Скрипт выполнен успешно!")
        sys.exit(0)
    except KeyboardInterrupt:
        logger.info("\n⚠ Парсинг прерван пользователем")
        sys.exit(1)
    except Exception as e:
        logger.error(f"\n✗ Критическая ошибка: {e}", exc_info=True)
        sys.exit(1)

