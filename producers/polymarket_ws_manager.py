#producers/polymarket_ws_manager.py

from kafka import KafkaProducer, KafkaConsumer
import websocket
import threading
import json
from datetime import datetime, timezone
import time

consumer = KafkaConsumer('market-updates',
                            bootstrap_servers='kafka:9093',
                            auto_offset_reset='latest',
                            enable_auto_commit=True,
                            group_id='polymarket-ws-manager',
                            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
                            )

producer = KafkaProducer(bootstrap_servers='kafka:9093',
                            value_serializer=lambda x: json.dumps(x).encode('utf-8')
                            )

# state variables
current_token_ids = []
current_market_id = None
current_market_end_time = None
token_outcome_map = {}
ws = None
ws_thread = None
is_intentional_close = False

# WebSocket event handlers
def on_message(ws, message):
    
    try:
        data = json.loads(message)

        # Handle if data is a list (batch of messages)
        if isinstance(data, list):
            for item in data:
                process_event(item)
        else:
            process_event(data)
    
    except Exception as e:
        print(f"Error processing message: {e}")
        

def process_event(data):
    if not isinstance(data, dict):
        return  # Ignore non-dict messages
    
    event_type = data.get('event_type')

    # BOOK TYPE
    if event_type == 'book':
        bids = data.get('bids', [])
        asks = data.get('asks', [])
        

        if bids and asks:
            outcome = token_outcome_map.get(data['asset_id'])
            if not outcome:
                print(f"Unknown asset_id {data['asset_id']} in book event")
                return

            # Extract best bid and ask
            best_bid_price = float(bids[0]['price'])
            best_bid_size = float(bids[0]['size'])
            best_ask_price = float(asks[0]['price'])
            best_ask_size = float(asks[0]['size'])

            # Calc total volumes
            total_bid_volume = sum(float(bid['size']) for bid in bids)
            total_ask_volume = sum(float(ask['size']) for ask in asks)

            # Find largest bid with its price
            largest_bid_order = max(bids, key=lambda b: float(b['size']))
            largest_bid_size = float(largest_bid_order['size'])
            largest_bid_price = float(largest_bid_order['price'])

            # Find largest ask with its price
            largest_ask_order = max(asks, key=lambda a: float(a['size']))
            largest_ask_size = float(largest_ask_order['size'])
            largest_ask_price = float(largest_ask_order['price'])

            # Book imbalance
            total_volume = total_bid_volume + total_ask_volume
            book_imbalance = (total_bid_volume - total_ask_volume) / total_volume if total_volume > 0 else 0

            event = {
                'type' : 'orderbook_summary',
                'market_id': current_market_id,
                'asset_id': data['asset_id'],
                'condition_id': data['market'],
                'outcome': outcome,
                'timestamp': datetime.now().isoformat(),
                'best_bid_price': best_bid_price,
                'best_bid_size': best_bid_size,
                'best_ask_price': best_ask_price,
                'best_ask_size': best_ask_size,
                'total_bid_volume': total_bid_volume,
                'total_ask_volume': total_ask_volume,
                'largest_bid_size': largest_bid_size,
                'largest_bid_price': largest_bid_price,
                'largest_ask_size': largest_ask_size,
                'largest_ask_price': largest_ask_price,
                'book_imbalance': book_imbalance
            }

            producer.send('polymarket-prices', event)

    # PRICE CHANGE TYPE
    elif event_type == 'price_change':
        for change in data['price_changes']:
            # only capture buy side
            if change['side'] == 'BUY':
                outcome = token_outcome_map.get(change['asset_id'])
                if not outcome:
                    print(f"Unknown asset_id {change['asset_id']} in price_change event")
                    continue
                event = {
                    'type': 'price_change',
                    'market_id': current_market_id,
                    'asset_id': change['asset_id'],
                    'outcome': outcome,
                    'side': change['side'],
                    'timestamp': datetime.now().isoformat(),
                    'price': float(change['price']),
                    'size': float(change['size']),
                }
                producer.send('polymarket-prices', event)

    # TRADE TYPE
    elif event_type == 'last_trade_price':
        outcome = token_outcome_map.get(data['asset_id'])
        if not outcome:
            print(f"Unknown asset_id {data['asset_id']} in trade event")
            return
        event = {
            'type': 'trade',
            'market_id': current_market_id,
            'asset_id': data['asset_id'],
            'price': float(data['price']),
            'outcome': outcome,
            'side': data['side'],
            'size': float(data['size']),
            'timestamp': datetime.now().isoformat()
        }
        producer.send('polymarket-prices', event)

    # UNKNOWN TYPE - Capture for logging
    else:
        print(f"Unknown event type received: {event_type}")
        event = {
            'type': 'unknown',
            'original_event_type': event_type,
            'market_id': current_market_id,
            'timestamp': datetime.now().isoformat(),
            'raw_data': data
        }
        producer.send('polymarket-prices', event)
        print(f"Published unknown event type: {event_type}")


def parse_datetime(dt_str):
    return datetime.fromisoformat(dt_str.replace('Z', '+00:00'))


def on_open(ws):
    # Called when the WebSocket connection is opened
    print("WebSocket connection opened")

    # Subscribe to current token IDs
    subscribe_msg = {
        "type": "market",
        "assets_ids": current_token_ids
    }

    print(f"Sending subscription message: {subscribe_msg}")
    
    ws.send(json.dumps(subscribe_msg))
    print(f"Subscribed to {len(current_token_ids)} tokens")

def on_error(ws, error):
    print(f"Websocket Error: {error}")

def on_close(ws, close_status_code, close_msg):
    global is_intentional_close
    print(f"Websocket closed: code={close_status_code}, msg={close_msg}")

    # If intentional close, do not attempt to reconnect
    if is_intentional_close:
        print("WebSocket closed intentionally. No Reconnection will be attempted.")
        is_intentional_close = False
        return
    
    # Check if market is still active before reconnecting
    if current_market_end_time:
        now = datetime.now(timezone.utc)
        if now < current_market_end_time:
            print("Market still active, attempting to reconnect WebSocket...")
            reconnect_with_backoff()
        else:
            print("Market has ended, not reconnecting WebSocket.")
    else:
        print("No active market - not reconnecting WebSocket.")

def reconnect_with_backoff():
    global ws, ws_thread

    retry_delay = 5  # start with 5 seconds
    max_retry = 30  # max 30 seconds

    for attempt in range(10):
        # Check if market is still active
        if current_market_end_time:
            now = datetime.now(timezone.utc)
            if now >= current_market_end_time:
                print("Market has ended during backoff, not reconnecting WebSocket.")
                return

        print(f"Reconnection Attempt {attempt}/10 in {retry_delay} seconds...")
        time.sleep(retry_delay)

        try:
            # Try to reconnect
            start_websocket(current_token_ids)
            print("Reconnected to WebSocket successfully.")
            return
        except Exception as e:
            print(f"Reconnection attempt {attempt} failed: {e}")
            retry_delay = min(retry_delay * 2, max_retry)  # exponential backoff
    
    print("Failed to reconnect after 10 attempts.")

# Websocket Lifecycle Management
def start_websocket(token_ids):
    global ws, ws_thread

    ws = websocket.WebSocketApp(
        "wss://ws-subscriptions-clob.polymarket.com/ws/market",
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close
    )
    # run in thread
    print("Connecting to Polymarket...")
    ws_thread = threading.Thread(target=lambda: ws.run_forever(ping_interval=20, ping_timeout=10))
    ws_thread.daemon = True
    ws_thread.start()

def stop_websocket():
    global ws, is_intentional_close

    if ws:
        print("closing WebSocket connection...")
        is_intentional_close = True
        ws.close()
        if ws_thread:
            ws_thread.join(timeout=5)

def reconnect_websocket(market_data):
    global current_token_ids, current_market_id, token_outcome_map, current_market_end_time

    # Extract token IDs from market data
    new_token_ids = market_data['token_ids']
    new_market_id = market_data['market_id']
    end_time = parse_datetime(market_data['end_time'])

    if new_token_ids != current_token_ids:
        print(f"New market detected {market_data['question']}")
        print(f"Market ends at {end_time.strftime('%H:%M:%S')} UTC")
        stop_websocket()
        current_token_ids = new_token_ids
        current_market_id = new_market_id
        current_market_end_time = end_time

        token_outcome_map = {
            new_token_ids[0]: 'YES',
            new_token_ids[1]: 'NO'
        }

        start_websocket(current_token_ids)
        schedule_websocket_close(end_time)

def schedule_websocket_close(end_time):
    now = datetime.now(timezone.utc)
    seconds_until_end = (end_time - now).total_seconds()

    def close_at_end():
        time.sleep(max(0, seconds_until_end))
        print("Market ended. Closing WebSocket connection.")
        stop_websocket()

    # run in separate thread
    close_thread = threading.Thread(target=close_at_end)
    close_thread.daemon = True
    close_thread.start()

if __name__ == "__main__":
    print("Starting Polymarket WebSocket Manager...")

    #consume from market-uodates
    for message in consumer:
        market_data = message.value
        reconnect_websocket(market_data)