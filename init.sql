-- init.sql
-- BRONZE LAYER: Raw Data dumps from Kafka topics

-- create schema if not exists
CREATE SCHEMA IF NOT EXISTS bronze;

-- Raw market updates from market_discovery.py
CREATE TABLE IF NOT EXISTS bronze.market_updates(
    id SERIAL PRIMARY KEY,
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    payload JSONB NOT NULL
);

CREATE INDEX idx_bronze_market_updates_time ON bronze.market_updates(ingested_at);
CREATE INDEX idx_bronze_market_updates_market_id ON bronze.market_updates((payload->>'market_id'));

-- Raw market resolutions from market_resolution.py
CREATE TABLE IF NOT EXISTS bronze.market_resolutions(
    id SERIAL PRIMARY KEY,
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    payload JSONB NOT NULL
);

CREATE INDEX idx_bronze_resolutions_time ON bronze.market_resolutions(ingested_at);
CREATE INDEX idx_bronze_resolutions_market_id ON bronze.market_resolutions((payload->>'market_id'));

-- Raw BTC prices from coinbase producer
CREATE TABLE IF NOT EXISTS bronze.btc_prices(
    id SERIAL PRIMARY KEY,
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    payload JSONB NOT NULL
);

CREATE INDEX idx_bronze_btc_prices_time ON bronze.btc_prices(ingested_at);

-- Raw polymarket price events (orderbooks, price changes, trades) from polymarket_ws_manager.py
CREATE TABLE  IF NOT EXISTS bronze.polymarket_prices(
    id SERIAL PRIMARY KEY,
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    payload JSONB NOT NULL
);

CREATE INDEX idx_bronze_polymarket_prices_time ON bronze.polymarket_prices(ingested_at);
CREATE INDEX idx_bronze_polymarket_prices_type ON bronze.polymarket_prices((payload->>'event_type'));
CREATE INDEX idx_bronze_polymarket_prices_market_id ON bronze.polymarket_prices((payload->>'market_id'));

-- Verify the tables were created
DO $$
BEGIN
    RAISE NOTICE 'Bronze layer tables created successfully.';
END $$;