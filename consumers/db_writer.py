from kafka import KafkaConsumer
import psycopg2
import json
import threading
from datetime import datetime, timezone
import time

# Global connection (will be managed with retry logic)
conn = None

def get_postgres_connection():
    # Get or create Postgres connection with retry logic
    global conn
    
    max_retries = 5
    retry_delay = 5
    
    for attempt in range(max_retries):
        try:
            if conn is None or conn.closed:
                print(f"Connecting to Postgres (attempt {attempt + 1}/{max_retries})...")
                conn = psycopg2.connect(
                    host='postgres',
                    port=5432,
                    database='trading_data',
                    user='postgres',
                    password='postgres'
                )
                conn.autocommit = True
                print("Connected to Postgres")
                return conn
            return conn
        
        except psycopg2.OperationalError as e:
            print(f"Postgres connection failed: {e}")
            if attempt < max_retries - 1:
                print(f"Retrying in {retry_delay}s...")
                time.sleep(retry_delay)
                retry_delay = min(retry_delay * 2, 60)  # Exponential backoff
            else:
                print("Failed to connect to Postgres after multiple attempts")
                raise

def consume_topic(topic_name, bronze_table):
    consumer = KafkaConsumer(
        topic_name,
        bootstrap_servers='kafka:9093',
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id=f'db-writer-{topic_name}',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    print(f'Started consumer: {topic_name} -> {bronze_table}')
    count = 0
    error_count = 0

    for message in consumer:
        try:
            write_to_bronze(bronze_table, message.value)
            count += 1

            if count % 1000 == 0:
                print(f"Processed {count} messages (errors: {error_count}")

        except Exception as e:
            error_count += 1
            print(f"Error writing message to {bronze_table}: {e}")
            print(f"Failed message: {str(message.value[:200])}...")  # Print first 200 chars of the message

            # Dont crash - skip message and continue
            if error_count % 100 == 0:
                print(f"Warning: {error_count} total errors on {topic_name}")

def write_to_bronze(table_name, data):
    # Generic bronze level writer
    max_retries = 5

    for attempt in range(max_retries):
        try:
            db_conn = get_postgres_connection()
            cursor = db_conn.cursor()
            
            cursor.execute(f"""
                INSERT INTO {table_name} (payload)
                VALUES (%s)
            """, (json.dumps(data),)) 
            
            cursor.close()
            return
        
        except psycopg2.OperationalError as e:
            # Connection issue - mark connection as dead and retry
            global conn
            conn = None
            print(f"Postgres connection lost (attempt {attempt + 1}/{max_retries}): {e}")
            if attempt < max_retries - 1:
                time.sleep(2 ** attempt)  # Exponential backoff
            else:
                raise  # Re-raise after max retries

        except psycopg2.Error as e:
            # Database error - bad data, constraint violation, etc.
            print(f"Database error on write to {table_name}: {e}")
            raise # Do not retry on DB errors

        except Exception as e:
            print(f"Unexpected error on write to {table_name}: {e}")
            raise
    

if __name__ == "__main__":
    print(f"Starting DB Writer...")

    try:
        get_postgres_connection()
    except Exception as e:
        print(f"Failed to establish initial Postgres connection: {e}")
        exit(1)

    threads = []
    topics = [
        ('market-updates', 'bronze.market_updates'),
        ('market-resolutions', 'bronze.market_resolutions'),
        ('asset-prices', 'bronze.btc_prices'),
        ('polymarket-prices', 'bronze.polymarket_prices'),
        ('market-resolution-failures', 'bronze.market_resolution_failures')
    ]

    for topic, table in topics:
        thread = threading.Thread(target=consume_topic, args=(topic, table))
        thread.daemon = True
        thread.start()
        threads.append(thread)

    print(f"Started {len(threads)} consumer threads.")

    # Keep the main thread alive to allow daemon threads to run
    try:
        for thread in threads:
            thread.join()
    except KeyboardInterrupt:
        print("Shutting down DB Writer...")