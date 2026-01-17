# producers/market_discovery.py
import requests
from datetime import datetime, timedelta, timezone
import time
import json
from kafka import KafkaProducer

# Add Kafka producer
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def fetch_active_15min_markets():
    """Fetch all active 15-minute markets from Polymarket"""
    
    url = 'https://gamma-api.polymarket.com/markets'
    params = {
        'tag_id': '102467',  # 15-minute markets tag
        'closed': 'false'    # Only active markets
    }
    
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        markets = response.json()
        
        print(f"Fetched {len(markets)} total 15-min markets")
        return markets
        
    except requests.RequestException as e:
        print(f"Error fetching markets: {e}")
        return []
    
def filter_btc_markets(markets):
    """Filter for only Bitcoin markets"""
    
    btc_markets = []
    
    for market in markets:
        question = market.get('question', '').lower()
        
        # Check if it's a Bitcoin market
        if 'bitcoin' in question or 'btc' in question:
            btc_markets.append(market)
    
    print(f"Found {len(btc_markets)} active BTC markets")
    return btc_markets

def get_current_market(btc_markets):
    """Get the market for the CURRENT 15-minute window"""

    now = datetime.now(timezone.utc)
    
    for market in btc_markets:
        # Parse the event start time
        start_time_str = market.get('eventStartTime')
        if not start_time_str:
            continue
        
        # Convert to datetime
        start_time = datetime.fromisoformat(start_time_str.replace('Z', '+00:00'))

        # Check if this market's window has started but not ended
        # (15-minute window means it ends 15 minutes after start)
        end_time = start_time + timedelta(minutes=15)
        
        if start_time <= now < end_time:
            print(f"Current market: {market['question']}")
            return market
    
    print("No current BTC market found")
    return None

def parse_market_data(market):
    """Extract the data we need from a market"""

    if not market:
        return None

    # Parse outcome prices (YES, NO)
    outcome_prices = market.get('outcomePrices', ['0', '0'])

    # Handle if outcome_prices is a string (needs JSON parsing)
    if isinstance(outcome_prices, str):
        outcome_prices = json.loads(outcome_prices)

    yes_price = float(outcome_prices[0])
    no_price = float(outcome_prices[1])
    
    # Get token IDs for WebSocket subscription
    token_ids = market.get('clobTokenIds', [])
    
    return {
        'market_id': market.get('id'),
        'condition_id': market.get('conditionId'),
        'question': market.get('question'),
        'yes_price': yes_price,
        'no_price': no_price,
        'token_ids': token_ids,
        'start_time': market.get('eventStartTime'),
        'end_time': market.get('endDate'),
        'active': market.get('active', False),
        'best_bid': market.get('bestBid'),
        'best_ask': market.get('bestAsk'),
        'liquidity': market.get('liquidity'),
        'volume': market.get('volume'),
        'slug': market.get('slug')
    }

def calculate_seconds_until_next_window():
    """Calculate seconds until the next 15-minute window starts"""

    now = datetime.now(timezone.utc)
    buffer_seconds = 10  # small buffer to ensure alignment

    # Calculate the next 15-minute mark (:00, :15, :30, :45)
    current_minutes = now.minute
    current_seconds = now.second

    # Round up to next 15-minute interval
    minutes_to_add = 15 - (current_minutes % 15)
    if minutes_to_add == 15 and current_seconds == 0:
        # Already at a 15-minute mark
        next_window = now
        return 0, next_window

    # Calculate next window time
    next_window = now + timedelta(minutes=minutes_to_add)
    next_window = next_window.replace(second=0, microsecond=0)

    delta = next_window - now
    delta += timedelta(seconds=buffer_seconds)
    return int(delta.total_seconds()), next_window


if __name__ == "__main__":
    print("Discovering active BTC markets...")
    print(f"Current time: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')} UTC")

    first_run = True

    while True:
        try:
            # Calculate when next window starts (skip wait on first run)
            if first_run:
                seconds_to_wait = 0
                next_window = datetime.now(timezone.utc)
                first_run = False
            else:
                seconds_to_wait, next_window = calculate_seconds_until_next_window()
            
            print(f"\nNext market window: {next_window.strftime('%H:%M:%S')} UTC")
            print(f"Waiting {seconds_to_wait:.1f} seconds ({seconds_to_wait/60:.1f} minutes)...")
            
            # Sleep until next window
            time.sleep(seconds_to_wait)
            
            # NOW we're aligned! Discover the market
            print("\nDiscovering active BTC markets...")
            
            # Fetch all 15-min markets
            all_markets = fetch_active_15min_markets()
            
            # Filter for BTC
            btc_markets = filter_btc_markets(all_markets)
            
            # Get the current one
            current_market = get_current_market(btc_markets)
            
            if current_market:
                market_data = parse_market_data(current_market)
                
                print("\nCurrent Market Data:")
                print(f"   Question: {market_data['question']}")
                print(f"   YES price: ${market_data['yes_price']}")
                print(f"   NO price: ${market_data['no_price']}")
                print(f"   Token IDs: {len(market_data['token_ids'])} tokens")
                
                # PRODUCE to Kafka
                producer.send('market-updates', value=market_data)
                print(f"Published to Kafka: market-updates")
            else:
                print("No active BTC market found for current window")
            
        except Exception as e:
            print(f"Error in discovery loop: {e}")
            print("Retrying in 60 seconds...")
            time.sleep(60)