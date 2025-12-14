#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Очистка последнего JSON файла с 319 отзывами
"""

import json
import re
import os
import glob

def clean_review_text(text: str) -> str:
    """Очищает текст отзыва от лишних элементов"""
    if not text:
        return text
    
    # Убираем информацию об авторе и количестве отзывов в начале текста
    text = re.sub(
        r'^[a-zA-Zа-яёА-ЯЁ0-9_\-]+\s+\d+\s+отзыв[аов]*\s*',
        '',
        text,
        flags=re.IGNORECASE
    )
    
    # Убираем "Полезно?" в конце
    text = re.sub(
        r'\s*(Полезно\??|полезно\??)\s*$',
        '',
        text,
        flags=re.IGNORECASE
    )
    
    # Очищаем от лишних пробелов
    text = ' '.join(text.split()).strip()
    
    return text

# Находим последний файл
files = glob.glob("output/smarthome_reviews_*.json")
if not files:
    print("❌ Файлы не найдены")
    exit(1)

latest_file = max(files, key=os.path.getmtime)
print(f"📖 Обрабатываю: {latest_file}")

with open(latest_file, 'r', encoding='utf-8') as f:
    data = json.load(f)

cleaned_count = 0

# Очищаем отзывы в карточках
for card in data.get('cards', []):
    for review in card.get('detailed_reviews', []):
        if 'review_text' in review:
            original = review['review_text']
            cleaned = clean_review_text(original)
            if cleaned != original:
                review['review_text'] = cleaned
                cleaned_count += 1

# Очищаем отзывы в массиве reviews
for review in data.get('reviews', []):
    if 'review_text' in review:
        original = review['review_text']
        cleaned = clean_review_text(original)
        if cleaned != original:
            review['review_text'] = cleaned
            cleaned_count += 1

# Сохраняем
output_file = latest_file.replace('smarthome_reviews_', 'cleaned_smarthome_reviews_')
with open(output_file, 'w', encoding='utf-8') as f:
    json.dump(data, f, ensure_ascii=False, indent=2)

print(f"✅ Очищено: {cleaned_count} отзывов")
print(f"💾 Сохранено: {output_file}")

