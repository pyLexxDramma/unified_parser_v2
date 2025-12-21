import json

# Загружаем исходные данные задачи
with open('output/report_company_e90faeb2_answered_card0_20251220_181201.json', encoding='utf-8') as f:
    answered = json.load(f)

# Проверяем статистику
stats = answered.get('statistics', {}).get('2gis', {})
print('=' * 60)
print('STATISTICS')
print('=' * 60)
print(f'aggregated_answered_reviews_count: {stats.get("aggregated_answered_reviews_count", "N/A")}')
print(f'aggregated_reviews_count: {stats.get("aggregated_reviews_count", "N/A")}')
print()

# Проверяем карточку
if answered.get('cards'):
    card = answered['cards'][0]
    all_revs = card.get('detailed_reviews', [])
    print('=' * 60)
    print('CARD DATA')
    print('=' * 60)
    print(f'card_answered_reviews_count: {card.get("card_answered_reviews_count", "N/A")}')
    print(f'card_reviews_count: {card.get("card_reviews_count", "N/A")}')
    print(f'total detailed_reviews: {len(all_revs)}')
    print()
    
    # Подсчитываем has_response
    has_resp = [r for r in all_revs if r.get('has_response')]
    no_resp = [r for r in all_revs if not r.get('has_response')]
    print(f'reviews with has_response=True: {len(has_resp)}')
    print(f'reviews with has_response=False: {len(no_resp)}')
    print()
    
    # Проверяем первые несколько отзывов с has_response=True
    print('=' * 60)
    print('FIRST 5 REVIEWS WITH has_response=True')
    print('=' * 60)
    for i, r in enumerate(has_resp[:5]):
        print(f'\n[{i+1}]')
        print(f'  review_id: {r.get("review_id", "N/A")}')
        print(f'  response_id: {r.get("response_id", "N/A")}')
        print(f'  has_response: {r.get("has_response")}')
        print(f'  review_text[:100]: {r.get("review_text", "")[:100]}')
        print(f'  response_text[:100]: {r.get("response_text", "")[:100]}')
        print(f'  review_text == response_text: {r.get("review_text") == r.get("response_text")}')
