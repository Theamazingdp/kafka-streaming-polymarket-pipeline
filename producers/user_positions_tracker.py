#producers/user_position_tracker.py

from kafka import KafkaProducer, KafkaConsumer
import psycopg2
import threading
import json
from datetime import datetime, timezone, timedelta
import time
import requests

POSITIONS_URL = "https://api.goldsky.com/api/public/project_cl6mb8i9h0003e201j6li0diw/subgraphs/positions-subgraph/0.0.7/gn"
QUERY_INTERVAL_SECONDS = 10
ERROR_THRESHOLD = 5

consumer = KafkaConsumer('market-updates',
                            bootstrap_servers='kafka:9093',
                            auto_offset_reset='latest',
                            enable_auto_commit=True,
                            group_id='goldsky-position-tracker',
                            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
                            )

producer = KafkaProducer(bootstrap_servers='kafka:9093',
                            value_serializer=lambda x: json.dumps(x).encode('utf-8')
                            )

def query_positions(condition_id):
    #Query Goldsky subgraph for all positions in a market
    
    query = """
    query GetPositions($conditionId: String!) {
      userBalances(
        where: {asset_: {condition: $conditionId}}
        first: 1000
        orderBy: balance
        orderDirection: desc
      ) {
        id
        user
        balance
        asset {
          id
          outcomeIndex
        }
      }
    }
    """
    
    response = requests.post(
        POSITIONS_URL,
        json={'query': query, 'variables': {'conditionId': condition_id}},
        timeout=10
    )
    
    result = response.json()
    
    if 'errors' in result:
        raise Exception(f"GraphQL error: {result['errors']}")
    
    return result.get('data', {}).get('userBalances', [])

def publish_positions(market_id, condition_id, positions, snapshot_time):
    #Publish individual position messages to Kafka
    
    if len(positions) == 0:
        # Empty snapshot - publish a marker
        empty_event = {
            'type': 'position_snapshot_empty',
            'market_id': market_id,
            'condition_id': condition_id,
            'snapshot_time': snapshot_time,
            'position_count': 0
        }
        producer.send('user-positions', empty_event)
        print(f"Snapshot: 0 positions")
        return
    
    # Publish each position as individual message
    published_count = 0
    for position in positions:
        balance = int(position['balance'])
        
        # Skip invalid balances
        if balance <= 0:
            continue
        
        outcome_index = int(position['asset']['outcomeIndex'])
        outcome = 'YES' if outcome_index == 0 else 'NO'
        
        event = {
            'type': 'position',
            'market_id': market_id,
            'condition_id': condition_id,
            'snapshot_time': snapshot_time,
            'user': position['user'],
            'asset_id': position['asset']['id'],
            'outcome': outcome,
            'outcome_index': outcome_index,
            'balance': balance
        }
        
        producer.send('user-positions', event)
        published_count += 1

    print(f"Snapshot: {published_count} positions published")

def track_positions(market_data):
    #Track positions for a market until it ends
    
    market_id = market_data['market_id']
    condition_id = market_data['condition_id']
    end_time = market_data['end_time']
    question = market_data.get('question', 'Unknown')
    
    print(f"Starting position tracking for market: {question}")
    print(f"   Market ID: {market_id}")
    print(f"   End time: {end_time.strftime('%Y-%m-%d %H:%M:%S')} UTC")
    
    error_count = 0
    
    while datetime.now(timezone.utc) < end_time:
        snapshot_time = datetime.now(timezone.utc).isoformat()
        
        try:
            # Query positions
            positions = query_positions(condition_id)
            
            # Publish to Kafka
            publish_positions(market_id, condition_id, positions, snapshot_time)
            
            # Reset error count on success
            error_count = 0
            
        except Exception as e:
            error_count += 1
            print(f"Error querying positions (attempt {error_count}): {e}")
            
            # Publish error after threshold
            if error_count % ERROR_THRESHOLD == 0:
                error_event = {
                    'service_name': 'user_positions_tracker',
                    'error_type': 'query_failure',
                    'error_threshold': ERROR_THRESHOLD,
                    'consecutive_errors': error_count,
                    'market_id': market_id,
                    'condition_id': condition_id,
                    'error_message': str(e),
                    'timestamp': datetime.now(timezone.utc).isoformat()
                }
                producer.send('service-errors', error_event)
                print(f"Published error event after {ERROR_THRESHOLD} failures")
        
        # Wait for next interval
        time.sleep(QUERY_INTERVAL_SECONDS)
    
    print(f"Market ended, stopped tracking: {market_id}")

def check_startup_gap():
    """Check if we restarted mid-market and log it"""
    
    try:
        conn = psycopg2.connect(
            host='postgres',
            port=5432,
            database='trading_data',
            user='postgres',
            password='postgres'
        )
        
        cursor = conn.cursor()
        cursor.execute("""
            SELECT 
                payload->>'market_id' as market_id,
                payload->>'condition_id' as condition_id,
                payload->>'start_time' as start_time,
                payload->>'end_time' as end_time
            FROM bronze.market_updates
            ORDER BY ingested_at DESC
            LIMIT 1
        """)
        
        result = cursor.fetchone()
        cursor.close()
        conn.close()
        
        if result:
            market_id, condition_id, start_time_str, end_time_str = result
            
            start_time = datetime.fromisoformat(start_time_str.replace('Z', '+00:00'))
            end_time = datetime.fromisoformat(end_time_str.replace('Z', '+00:00'))
            now = datetime.now(timezone.utc)
            
            # Check if market is still active
            if start_time <= now < end_time:
                # We restarted mid-market!
                market_duration = (end_time - start_time).total_seconds()
                elapsed = (now - start_time).total_seconds()
                remaining = (end_time - now).total_seconds()
                estimated_missed = int(elapsed / QUERY_INTERVAL_SECONDS)
                
                error_event = {
                    'service_name': 'user_positions_tracker',
                    'error_type': 'service_restart',
                    'market_id': market_id,
                    'condition_id': condition_id,
                    'market_start': start_time.isoformat(),
                    'market_end': end_time.isoformat(),
                    'restart_time': now.isoformat(),
                    'estimated_missed_snapshots': estimated_missed,
                    'timestamp': now.isoformat()
                }
                
                producer.send('service-errors', error_event)
                print(f"Service restarted mid-market!")
                print(f"   Market: {market_id}")
                print(f"   Estimated missed snapshots: {estimated_missed}")
                print(f"   Time remaining: {remaining/60:.1f} minutes")
        
    except Exception as e:
        print(f"Could not check for startup gap: {e}")

def parse_datetime(dt_str):
    """Parse ISO datetime string"""
    return datetime.fromisoformat(dt_str.replace('Z', '+00:00'))

if __name__ == "__main__":
    print("Starting User Positions Tracker...")
    
    # Check if we restarted mid-market
    check_startup_gap()
    
    print("Listening for new markets on market-updates topic...")
    
    # Consume from market-updates and spawn tracking threads
    for message in consumer:
        market_data = message.value
        
        print(f"\nNew market detected: {market_data['question']}")
        
        # Parse end time
        market_data['end_time'] = parse_datetime(market_data['end_time'])
        
        # Start tracking in a separate thread
        thread = threading.Thread(target=track_positions, args=(market_data,))
        thread.daemon = True
        thread.start()
        
        print(f"Started tracking thread for market {market_data['market_id']}")