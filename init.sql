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

-- Raw table for market resolution failures
CREATE TABLE IF NOT EXISTS bronze.market_resolution_failures(
    id SERIAL PRIMARY KEY,
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    payload JSONB NOT NULL  
)

CREATE INDEX idx_bronze_resolution_failures_time ON bronze.market_resolution_failures(ingested_at);
CREATE INDEX idx_bronze_resolution_failures_slug ON bronze.market_resolution_failures((payload->>'slug'));

-- Raw user positions from user_positions_tracker.py
CREATE TABLE IF NOT EXISTS bronze.user_positions(
    id SERIAL PRIMARY KEY,
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    payload JSONB NOT NULL
);

CREATE INDEX idx_bronze_user_positions_time ON bronze.user_positions(ingested_at);
CREATE INDEX idx_bronze_user_positions_user_id ON bronze.user_positions((payload->>'user'));
CREATE INDEX idx_bronze_user_positions_market_id ON bronze.user_positions((payload->>'market_id'))

-- Raw service errors from various producers
CREATE TABLE IF NOT EXISTS bronze.service_errors(
    id SERIAL PRIMARY KEY,
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    payload JSONB NOT NULL
);

CREATE INDEX idx_bronze_service_errors_time ON bronze.service_errors(ingested_at);
CREATE INDEX idx_bronze_service_errors_service ON bronze.service_errors((payload->>'service_name'));

-- Verify the tables were created
DO $$
BEGIN
    RAISE NOTICE 'Bronze layer tables created successfully.';
END $$;