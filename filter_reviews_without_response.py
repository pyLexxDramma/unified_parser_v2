#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Скрипт для фильтрации отзывов без ответов компании
Создает JSON только с отзывами, у которых has_response = False
"""

import json
import os
import glob
from typing import Dict, Any, List

def filter_reviews_without_response(input_file: str, output_file: str = None):
    """Фильтрует отзывы без ответов компании"""
    if not os.path.exists(input_file):
        print(f"❌ Файл не найден: {input_file}")
        return None
    
    print(f"📖 Читаю файл: {input_file}")
    with open(input_file, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    # Фильтруем отзывы без ответов
    reviews_without_response = []
    reviews_with_response = []
    
    # Обрабатываем отзывы из массива reviews
    if 'reviews' in data:
        for review in data['reviews']:
            has_response = review.get('has_response', False)
            response_text = review.get('response_text', '').strip()
            
            # Отзыв считается без ответа, если has_response = False и response_text пустой
            if not has_response and not response_text:
                reviews_without_response.append(review)
            else:
                reviews_with_response.append(review)
    
    # Обрабатываем отзывы из карточек
    cards_without_response = []
    for card in data.get('cards', []):
        if 'detailed_reviews' in card:
            filtered_reviews = []
            for review in card['detailed_reviews']:
                has_response = review.get('has_response', False)
                response_text = review.get('response_text', '').strip()
                
                if not has_response and not response_text:
                    filtered_reviews.append(review)
                    reviews_without_response.append(review)
                else:
                    reviews_with_response.append(review)
            
            # Обновляем карточку с отфильтрованными отзывами
            if filtered_reviews:
                card_copy = card.copy()
                card_copy['detailed_reviews'] = filtered_reviews
                card_copy['card_reviews_count'] = len(filtered_reviews)
                cards_without_response.append(card_copy)
    
    # Формируем новый JSON
    filtered_data = {
        "company": data.get("company", {}),
        "parsing_info": {
            **data.get("parsing_info", {}),
            "total_reviews": len(reviews_without_response),
            "total_reviews_with_response": len(reviews_with_response),
            "total_reviews_without_response": len(reviews_without_response),
            "filter_applied": "only_reviews_without_response"
        },
        "cards": cards_without_response,
        "reviews": reviews_without_response
    }
    
    # Сохраняем отфильтрованный файл
    if output_file is None:
        base_name = os.path.basename(input_file)
        dir_name = os.path.dirname(input_file)
        name, ext = os.path.splitext(base_name)
        output_file = os.path.join(dir_name, f"{name}_without_response{ext}")
    
    print(f"💾 Сохраняю отфильтрованный файл: {output_file}")
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(filtered_data, f, ensure_ascii=False, indent=2)
    
    print(f"✅ Отзывов без ответа: {len(reviews_without_response)}")
    print(f"📊 Отзывов с ответом: {len(reviews_with_response)}")
    print(f"📁 Файл сохранен: {output_file}")
    print(f"📊 Размер файла: {os.path.getsize(output_file) / 1024:.2f} KB")
    
    return output_file

if __name__ == "__main__":
    # Ищем файл с 319 отзывами
    files = glob.glob("output/smarthome_reviews_*.json")
    if not files:
        print("❌ Файлы не найдены")
        exit(1)
    
    # Ищем файл с 319 отзывами или последний очищенный
    target_file = None
    for f in files:
        if 'cleaned' in f or 'without_response' in f:
            continue
        try:
            with open(f, 'r', encoding='utf-8') as file:
                data = json.load(file)
                total = data.get('parsing_info', {}).get('total_reviews', 0)
                if total == 319:
                    target_file = f
                    break
        except:
            continue
    
    # Если не нашли файл с 319, ищем очищенный файл
    if not target_file:
        cleaned_files = glob.glob("output/cleaned_smarthome_reviews_*.json")
        if cleaned_files:
            target_file = max(cleaned_files, key=os.path.getmtime)
            print(f"⚠ Использую очищенный файл: {target_file}")
        else:
            target_file = max(files, key=os.path.getmtime)
            print(f"⚠ Файл с 319 отзывами не найден, использую последний: {target_file}")
    else:
        print(f"📖 Найден файл с 319 отзывами: {target_file}")
    
    print()
    print("=" * 80)
    print("ФИЛЬТРАЦИЯ ОТЗЫВОВ БЕЗ ОТВЕТОВ КОМПАНИИ")
    print("=" * 80)
    print()
    
    output_file = filter_reviews_without_response(target_file)
    
    print()
    print("=" * 80)
    print("✅ ФИЛЬТРАЦИЯ ЗАВЕРШЕНА")
    print("=" * 80)

