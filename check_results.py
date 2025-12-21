import json

# Check answered reviews
with open('output/report_company_e90faeb2_answered_card0_20251220_181201.json', encoding='utf-8') as f:
    answered = json.load(f)

revs_answered = answered['cards'][0]['detailed_reviews']
print('=' * 60)
print('ANSWERED REVIEWS')
print('=' * 60)
print(f'Total count: {len(revs_answered)}')
print(f'Expected: ~60')
print()

if revs_answered:
    r = revs_answered[0]
    print('First review sample:')
    print(f'  review_id: {r.get("review_id", "N/A")}')
    print(f'  response_id: {r.get("response_id", "N/A")}')
    print(f'  has_response: {r.get("has_response")}')
    print(f'  review_text[:150]: {r.get("review_text", "")[:150]}')
    print(f'  response_text[:150]: {r.get("response_text", "")[:150]}')
    print(f'  review_text == response_text: {r.get("review_text") == r.get("response_text")}')
    print()
    
    # Check how many have review_id and response_id
    with_ids = sum(1 for r in revs_answered if r.get('review_id'))
    with_resp_ids = sum(1 for r in revs_answered if r.get('response_id'))
    text_matches = sum(1 for r in revs_answered if r.get('review_text') == r.get('response_text'))
    
    print('Statistics:')
    print(f'  Reviews with review_id: {with_ids}/{len(revs_answered)}')
    print(f'  Reviews with response_id: {with_resp_ids}/{len(revs_answered)}')
    print(f'  Reviews where review_text == response_text: {text_matches}/{len(revs_answered)}')
    print()
    
    print('Sample reviews (first 5):')
    for i, r in enumerate(revs_answered[:5]):
        print(f'  [{i+1}] id={r.get("review_id", "N/A")[:20]}, resp_id={r.get("response_id", "N/A")[:20]}, text_match={r.get("review_text")==r.get("response_text")}')

# Check unanswered reviews
print()
print('=' * 60)
print('UNANSWERED REVIEWS')
print('=' * 60)
with open('output/report_company_e90faeb2_unanswered_card0_20251220_181201.json', encoding='utf-8') as f:
    unanswered = json.load(f)

revs_unanswered = unanswered['cards'][0]['detailed_reviews']
print(f'Total count: {len(revs_unanswered)}')
print(f'Expected: ~269 (319 - 50)')
print()

if revs_unanswered:
    r = revs_unanswered[0]
    print('First review sample:')
    print(f'  review_id: {r.get("review_id", "N/A")}')
    print(f'  has_response: {r.get("has_response")}')
    print(f'  review_text[:150]: {r.get("review_text", "")[:150]}')
    print()
    
    with_ids = sum(1 for r in revs_unanswered if r.get('review_id'))
    print(f'  Reviews with review_id: {with_ids}/{len(revs_unanswered)}')
