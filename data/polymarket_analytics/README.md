# Polymarket Analytics - dbt Transformations

Data transformation layer for the Polymarket real-time trading data pipeline.

## Purpose

Transforms raw event data (Bronze) into clean, typed tables (Silver) and ML-ready features (Gold) using the medallion architecture.

## Architecture
```
Bronze (Raw JSONB from Kafka)
  ↓
Silver (Cleaned, typed tables)
  ↓
Gold (ML features for model training)
```

## Current Models

### Silver Layer
- `markets` - Cleaned market metadata with typed columns

### Gold Layer
- Coming soon: ML feature engineering

## Running Transformations
```bash
# Run all models
dbt run

# Run specific model
dbt run --select markets

# Test data quality
dbt test
```

## Connection

Connects to PostgreSQL database: `trading_data`
- Bronze source: `bronze.*` schema
- Silver output: `analytics_silver.*` schema  
- Gold output: `analytics_gold.*` schema

## Development

Models are organized by layer:
- `models/bronze/` - Source definitions (read-only)
- `models/silver/` - Cleaned transformations
- `models/gold/` - Feature engineering