# producers/coinbase_producer.py
import signal
import sys
from kafka import KafkaProducer
import websocket
import json
from datetime import datetime
import time

class CoinbaseProducer:
    def __init__(self, throttle_seconds=1):
        # Connect to Kafka
        print("Connecting to Kafka...")
        self.producer = KafkaProducer(
            bootstrap_servers=['kafka:9093'],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        print("Connected to Kafka!")
        
        self.message_count = 0
        self.throttle_seconds = throttle_seconds
        self.last_sent = 0
        self.latest_price = None
        self.should_reconnect = True
        self.is_intentional_close = False
    
    def on_message(self, ws, message):
        """Called when WebSocket receives a message"""
        try:
            data = json.loads(message)
            
            # Only process ticker messages
            if data.get('type') == 'ticker':
                self.latest_price = {
                    'symbol': 'BTC',
                    'price': float(data['price']),
                    'timestamp': datetime.now().isoformat(),
                    'volume': float(data.get('volume_24h', 0))
                }

                current_time = time.time()
                if current_time - self.last_sent >= self.throttle_seconds:
                    #send to kafka
                    self.producer.send('asset-prices',value=self.latest_price)
                    self.last_sent = current_time
                    self.message_count += 1

                    #print all messages now that its throttled
                    print(f"Message #{self.message_count}: BTC ${self.latest_price['price']:,.2f}")
                
        except Exception as e:
            print(f"Error: {e}")
    
    def on_open(self, ws):
        """Called when WebSocket connects"""
        print("Connected to Coinbase WebSocket!")
        
        # Subscribe to BTC-USD ticker
        subscribe_message = {
            "type": "subscribe",
            "product_ids": ["BTC-USD"],
            "channels": ["ticker"]
        }
        ws.send(json.dumps(subscribe_message))
        print("Subscribed to BTC-USD ticker")
    
    def on_error(self, ws, error):
        print(f"WebSocket Error: {error}")
    
    def on_close(self, ws, close_status_code, close_msg):
        print(f"WebSocket closed: {close_status_code}")
        if self.should_reconnect:
            print("Will attempt to reconnect...")
    
    def signal_handler(self, signum, frame):
        """Handle Ctrl+C gracefully"""
        print("\nReceived interrupt signal - shutting down gracefully...")
        self.should_reconnect = False
        self.is_intentional_close = True
        sys.exit(0)

    def start(self):
        """Start the producer"""
        print("Starting Coinbase Producer...")

        # Register signal handler for Ctrl+C
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)

        retry_delay = 5  # seconds
        max_delay = 60 # max 1 min between retries

        while self.should_reconnect:
            try:
                print("Connecting to Coinbase WebSocket...")
        
                ws = websocket.WebSocketApp(
                    "wss://ws-feed.exchange.coinbase.com",
                    on_message=self.on_message,
                    on_open=self.on_open,
                    on_error=self.on_error,
                    on_close=self.on_close
                )
        
                # Run forever (blocking)
                ws.run_forever(ping_interval=30, ping_timeout=10)

                # if we get here, connection closed
                if self.should_reconnect and not self.is_intentional_close:
                    print(f"Connection lost. Reconnecting in {retry_delay} seconds...")
                    time.sleep(retry_delay)
                    retry_delay = min(retry_delay * 2, max_delay)  # exponential backoff
                else:
                    print("Shutting down, reconnection = False.")
                    break
            except Exception as e:
                print(f"Unexpected error: {e}")
                if self.should_reconnect:
                    print(f"Retrying in {retry_delay}s...")
                    time.sleep(retry_delay)
                    retry_delay = min(retry_delay * 2, max_delay)
                else:
                    break

        print("Coinbase Producer stopped.")

if __name__ == "__main__":
    producer = CoinbaseProducer()
    producer.start()