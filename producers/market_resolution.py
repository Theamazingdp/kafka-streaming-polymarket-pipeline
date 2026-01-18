#market_resolution.py

from kafka import KafkaConsumer, KafkaProducer
import requests
import time
import threading
import json
from datetime import datetime, timedelta, timezone

consumer = KafkaConsumer('market-updates', 
                            bootstrap_servers='localhost:9092', 
                            auto_offset_reset='earliest', 
                            enable_auto_commit=True, 
                            group_id='market-resolution-group', 
                            value_deserializer=lambda x: json.loads(x.decode('utf-8')))

producer = KafkaProducer(bootstrap_servers='localhost:9092', value_serializer=lambda x: json.dumps(x).encode('utf-8'))

def resolve_market_when_ready(market_data):
    slug = market_data['slug']
    end_time = parse_iso_datetime(market_data['end_time'])
    now = datetime.now(timezone.utc)
    
    # sleep until market end time + buffer
    wait_seconds = (end_time - now).total_seconds() + 60  # extra buffer of 60 seconds
    time.sleep(max(0, wait_seconds))

    # poll with retries
    for attempt in range(5):
        market = get_market_by_slug(slug)

        if market is None:
            print(f"Failed to fetch market {slug}, retrying in 30 seconds...")
            time.sleep(30)
            continue

        if market['closed'] and market.get('umaResolutionStatus') == 'resolved':
            #success, publish resolution
            producer.send('market-resolutions', build_resolution_message(market))
            print(f"Published resolution for market {slug}")
            return
        else:
            print(f"Market {slug} not resolved yet, retrying in 30 seconds...")
            time.sleep(30)

    print(f"Failed to resolve market {slug} after multiple attempts.")

def parse_iso_datetime(dt_str):
    return datetime.fromisoformat(dt_str.replace('Z', '+00:00'))

def get_market_by_slug(slug):
    try:
        response = requests.get(f"https://gamma-api.polymarket.com/markets/slug/{slug}")
        if response.status_code == 200:
            return response.json()
        else:
            print(f"Failed to fetch market {slug}, status code: {response.status_code}")
            return None
    except Exception as e:
        print(f"Error fetching market {slug}: {e}")
        return None

def build_resolution_message(market):
    outcome_prices = json.loads(market.get('outcomePrices', '["0","0"]'))
    
    # Convert strings to floats for comparison
    yes_price = float(outcome_prices[0])
    no_price = float(outcome_prices[1])
    
    # Winner is the one with price = 1.0
    if no_price == 1.0: 
        resolution = 'Down'
    elif yes_price == 1.0:
        resolution = 'Up'
    else:
        resolution = 'Unknown'

    return {
        'market_id': market['id'],
        'market_slug': market['slug'],
        'winner': resolution,
        'final_yes_price': outcome_prices[0],
        'final_no_price': outcome_prices[1],
        'volume': market.get('volume'),
        'resolution_status': 'resolved',
        'resolved_at': market.get('closedTime')
    }

if __name__ == "__main__":
    print("Starting Market Resolution Consumer...")
    for message in consumer:
        market_data = message.value
        print(f"Received market update for {market_data['slug']}, starting resolution thread.")
        thread = threading.Thread(target=resolve_market_when_ready, args=(market_data,))
        thread.daemon = True
        thread.start()