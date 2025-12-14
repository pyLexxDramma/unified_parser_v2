#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Скрипт для очистки уже собранного JSON файла от лишних элементов в тексте отзывов
Убирает: "vassermanuss ​ 2 отзыва" и "Полезно?"
"""

import json
import re
import os
import sys
from typing import Dict, Any

def clean_review_text(text: str) -> str:
    """Очищает текст отзыва от лишних элементов"""
    if not text:
        return text
    
    # Убираем информацию об авторе и количестве отзывов в начале текста
    # Паттерны типа "vassermanuss ​ 2 отзыва" или "username 5 отзывов"
    text = re.sub(
        r'^[a-zA-Zа-яёА-ЯЁ0-9_\-]+\s+\d+\s+отзыв[аов]*\s*',
        '',
        text,
        flags=re.IGNORECASE
    )
    
    # Убираем служебные тексты в конце: "Полезно?", "Полезно", "Подписаться"
    text = re.sub(
        r'\s*(Полезно\??|полезно\??|Подписаться|подписаться)\s*$',
        '',
        text,
        flags=re.IGNORECASE
    )
    
    # Убираем служебные тексты в начале
    text = re.sub(
        r'^\s*(Полезно\??|полезно\??|Подписаться|подписаться)\s+',
        '',
        text,
        flags=re.IGNORECASE
    )
    
    # Очищаем от лишних пробелов
    text = ' '.join(text.split()).strip()
    
    return text

def clean_json_file(input_file: str, output_file: str = None):
    """Очищает JSON файл от лишних элементов в тексте отзывов"""
    if not os.path.exists(input_file):
        print(f"❌ Файл не найден: {input_file}")
        return
    
    print(f"📖 Читаю файл: {input_file}")
    with open(input_file, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    cleaned_count = 0
    
    # Очищаем отзывы в массиве reviews
    if 'reviews' in data:
        for review in data['reviews']:
            if 'review_text' in review and review['review_text']:
                original_text = review['review_text']
                cleaned_text = clean_review_text(original_text)
                if cleaned_text != original_text:
                    review['review_text'] = cleaned_text
                    cleaned_count += 1
    
    # Очищаем отзывы в карточках (cards)
    if 'cards' in data:
        for card in data['cards']:
            if 'detailed_reviews' in card:
                for review in card['detailed_reviews']:
                    if 'review_text' in review and review['review_text']:
                        original_text = review['review_text']
                        cleaned_text = clean_review_text(original_text)
                        if cleaned_text != original_text:
                            review['review_text'] = cleaned_text
                            cleaned_count += 1
    
    # Сохраняем очищенный файл
    if output_file is None:
        # Создаем имя файла с префиксом "cleaned_"
        base_name = os.path.basename(input_file)
        dir_name = os.path.dirname(input_file)
        name, ext = os.path.splitext(base_name)
        output_file = os.path.join(dir_name, f"cleaned_{name}{ext}")
    
    print(f"💾 Сохраняю очищенный файл: {output_file}")
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    
    print(f"✅ Очищено {cleaned_count} отзывов")
    print(f"📁 Файл сохранен: {output_file}")
    print(f"📊 Размер файла: {os.path.getsize(output_file) / 1024:.2f} KB")
    
    return output_file

if __name__ == "__main__":
    # Ищем последний JSON файл с отзывами
    output_dir = "output"
    json_files = [f for f in os.listdir(output_dir) if f.startswith("smarthome_reviews_") and f.endswith(".json")]
    
    if not json_files:
        print("❌ Не найдено JSON файлов с отзывами")
        sys.exit(1)
    
    # Берем последний файл (по дате в имени)
    latest_file = sorted(json_files)[-1]
    input_file = os.path.join(output_dir, latest_file)
    
    print("=" * 80)
    print("ОЧИСТКА JSON ФАЙЛА ОТ ЛИШНИХ ЭЛЕМЕНТОВ")
    print("=" * 80)
    print(f"Входной файл: {input_file}")
    print()
    
    output_file = clean_json_file(input_file)
    
    print()
    print("=" * 80)
    print("✅ ОЧИСТКА ЗАВЕРШЕНА")
    print("=" * 80)

