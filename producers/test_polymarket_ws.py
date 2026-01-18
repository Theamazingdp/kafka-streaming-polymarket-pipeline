# test_polymarket_ws.py

import websocket
import json
import sys

def on_message(ws, message):
    try:
        data = json.loads(message)
        event_type = data.get('event_type')
    
        if event_type == 'book':
            print(f"\nORDER BOOK:")
            print(f"   Asset: {data['asset_id'][:10]}...")
            print(f"   Best Bid: {data['bids'][0] if data.get('bids') else 'N/A'}")
            print(f"   Best Ask: {data['asks'][0] if data.get('asks') else 'N/A'}")
        
        elif event_type == 'price_change':
            changes = data.get('price_changes', [])
            if changes:
                print(f"\nPRICE CHANGE:")
                for change in changes:
                    print(f"   Asset: {change['asset_id'][:10]}... Price: {change['price']} Size: {change['size']} Side: {change['side']}")
        
        elif event_type == 'last_trade_price':
            print(f"\nTRADE: ${data['price']} x {data['size']} shares")
    
    except Exception as e:
        print(f"Error processing message: {e}")

def on_open(ws):
    print("Connected!")
    
    token_ids = sys.argv[1:3]  # Get first two command line args
    
    subscribe_msg = {
        "type": "market",
        "assets_ids": token_ids
    }

    print(f"Sending subscription message: {subscribe_msg}")
    
    ws.send(json.dumps(subscribe_msg))
    print(f"Subscribed to {len(token_ids)} tokens")

def on_error(ws, error):
    print(f"Error: {error}")
    print(f"Error type: {type(error)}")

def on_close(ws, close_status_code, close_msg):
    print(f"\nConnection closed")

def on_ping(ws, message):
    print("Ping received")

def on_pong(ws, message):
    print("Pong received")

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python test_polymarket_ws.py <token_id_1> <token_id_2>")
        sys.exit(1)
    
    ws = websocket.WebSocketApp(
        "wss://ws-subscriptions-clob.polymarket.com/ws/market",
        on_message=on_message,
        on_open=on_open,
        on_error=on_error,
        on_close=on_close
    )
    
    print("Connecting to Polymarket...")
    ws.run_forever(
        ping_interval=20,  # Send ping every 20 seconds
        ping_timeout=10    # Wait 10 seconds for pong response
    )