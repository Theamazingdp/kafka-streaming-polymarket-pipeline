from kafka import KafkaConsumer
import psycopg2
import json
import threading
from datetime import datetime, timezone

# PostgreSQL connection parameters
conn = psycopg2.connect(
    host = 'localhost',
    port = 5432,
    database = 'trading_data',
    user = 'postgres',
    password = 'postgres'
)
conn.autocommit = True # Autocommit each insert

def consume_topic(topic_name, bronze_table):
    consumer = KafkaConsumer(
        topic_name,
        bootstrap_servers='localhost:9092',
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id=f'db-writer-{topic_name}',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    print(f'Started consumer: {topic_name} -> {bronze_table}')
    count = 0

    for message in consumer:
        write_to_bronze(bronze_table, message.value)
        count += 1

        if count % 1000 == 0:
            print(f"Processed {count} messages from topic: {topic_name}")

def write_to_bronze(table_name, data):
    # Generic bronze level writer
    cursor = conn.cursor()

    cursor.execute(f"""
        INSERT INTO {table_name} (payload)
        VALUES (%s)
    """, (json.dumps(data),)) 
    
    cursor.close()

if __name__ == "__main__":
    print(f"Starting DB Writer...")

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