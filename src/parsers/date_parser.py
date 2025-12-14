from __future__ import annotations
import re
import logging
from typing import Optional
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

MONTHS_RU = {
    'января': 1, 'февраля': 2, 'марта': 3, 'апреля': 4,
    'мая': 5, 'июня': 6, 'июля': 7, 'августа': 8,
    'сентября': 9, 'октября': 10, 'ноября': 11, 'декабря': 12,
    'январь': 1, 'февраль': 2, 'март': 3, 'апрель': 4,
    'май': 5, 'июнь': 6, 'июль': 7, 'август': 8,
    'сентябрь': 9, 'октябрь': 10, 'ноябрь': 11, 'декабрь': 12,
}

def parse_russian_date(date_string: str, current_year: Optional[int] = None) -> Optional[datetime]:
    """
    Парсит дату в формате русского языка.
    
    Форматы:
    - "21 августа 2024" (полная дата)
    - "17 ноября" (короткая дата, текущий год)
    - "4 февраля 2025"
    - "26 января 2025"
    - "сегодня", "вчера" (относительные даты)
    """
    if not date_string or not date_string.strip():
        return None
    
    date_string = date_string.strip().lower()
    
    # Обработка относительных дат
    if 'сегодня' in date_string or 'today' in date_string:
        return datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    if 'вчера' in date_string or 'yesterday' in date_string:
        yesterday = datetime.now() - timedelta(days=1)
        return yesterday.replace(hour=0, minute=0, second=0, microsecond=0)
    
    if current_year is None:
        current_year = datetime.now().year
    
    patterns = [
        r'(\d{1,2})\s+([а-яё]+)\s+(\d{4})',
        r'(\d{1,2})\s+([а-яё]+)',
    ]
    
    for pattern in patterns:
        match = re.search(pattern, date_string, re.IGNORECASE)
        if match:
            try:
                day = int(match.group(1))
                month_name = match.group(2).lower()
                year = int(match.group(3)) if len(match.groups()) >= 3 and match.group(3).isdigit() else current_year
                
                # Исправляем даты в будущем (если год больше текущего на 1, вероятно ошибка)
                if year > datetime.now().year:
                    if year == datetime.now().year + 1:
                        year = datetime.now().year
                    elif year > datetime.now().year + 10:
                        # Если год явно неправильный (например, 3035), используем текущий год
                        logger.warning(f"Invalid year {year} in date '{date_string}', using current year")
                        year = datetime.now().year
                
                month = MONTHS_RU.get(month_name)
                if month and 1 <= day <= 31 and 2000 <= year <= datetime.now().year + 1:
                    parsed_date = datetime(year, month, day)
                    # Проверяем, что дата не в будущем (с запасом в 1 день)
                    if parsed_date > datetime.now() + timedelta(days=1):
                        # Если дата в будущем, используем предыдущий год
                        parsed_date = parsed_date.replace(year=year - 1)
                    return parsed_date
                elif month and 1 <= day <= 31:
                    # Если год вне разумных пределов, но месяц и день валидны, используем текущий год
                    logger.warning(f"Year {year} out of range in date '{date_string}', using current year")
                    year = datetime.now().year
                    parsed_date = datetime(year, month, day)
                    if parsed_date > datetime.now() + timedelta(days=1):
                        parsed_date = parsed_date.replace(year=year - 1)
                    return parsed_date
            except (ValueError, IndexError, KeyError) as e:
                logger.debug(f"Could not parse date '{date_string}': {e}")
                continue
    
    return None

def format_russian_date(dt: datetime) -> str:
    """
    Форматирует дату в русский формат: "21 августа 2024"
    """
    if not dt:
        return ""
    
    month_names = [
        'января', 'февраля', 'марта', 'апреля', 'мая', 'июня',
        'июля', 'августа', 'сентября', 'октября', 'ноября', 'декабря'
    ]
    
    return f"{dt.day} {month_names[dt.month - 1]} {dt.year}"

