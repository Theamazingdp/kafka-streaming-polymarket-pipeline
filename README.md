# Polymarket Real-Time Trading Data Pipeline

A production-grade data engineering pipeline for collecting, processing, and analyzing real-time prediction market data from Polymarket's 15-minute Bitcoin markets. Built as an **educational project** to develop expertise in modern data engineering technologies (Kafka, dbt, Docker, real-time streaming) while exploring ML applications in prediction markets.

## Project Status: Data Collection Phase

Currently collecting high-fidelity market data across 6 synchronized services. System has been validated with 100% market resolution success rate and is actively tracking 1,000+ unique wallet positions per market.

### Current Metrics (Fresh Start - January 25, 2026)
**Note:** Services were restarted clean with synchronized launch to ensure data quality and alignment.

- **Markets Tracked:** 4 discovered, 3 resolved, 1 active
- **BTC Prices:** 1,944 records (1/second cadence)
- **Orderbook Events:** 423,095 records (~235 msg/sec average)
- **Position Snapshots:** 210,770 records (tracking 1,000+ unique wallets per market)
- **System Uptime:** 100% (0 service errors)
- **Data Collection Duration:** ~32 minutes of continuous operation

---

## Architecture Overview

### Tech Stack
- **Streaming:** Apache Kafka + Zookeeper
- **Database:** PostgreSQL 16
- **Orchestration:** Docker Compose
- **Language:** Python 3.11
- **Data Modeling:** Medallion Architecture (Bronze → Silver → Gold)

### Services Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                     Data Sources                             │
├─────────────────┬──────────────────┬────────────────────────┤
│ Coinbase WS     │ Polymarket API   │ Goldsky GraphQL        │
│ (BTC Prices)    │ (Markets/Orders) │ (User Positions)       │
└────────┬────────┴────────┬─────────┴──────────┬─────────────┘
         │                 │                     │
         ▼                 ▼                     ▼
┌─────────────────────────────────────────────────────────────┐
│                    Kafka Topics                              │
│  • asset-prices          • polymarket-prices                 │
│  • market-updates        • market-resolutions                │
│  • user-positions        • service-errors                    │
└────────────────────────┬────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────┐
│                  Bronze Layer (Raw Data)                     │
│              PostgreSQL JSONB Storage                        │
└─────────────────────────────────────────────────────────────┘
                         │
                         ▼ (Planned: dbt)
┌─────────────────────────────────────────────────────────────┐
│         Silver Layer (Cleaned & Joined)                      │
│         Gold Layer (ML Features)                             │
└─────────────────────────────────────────────────────────────┘
```

---

## Services

### 1. Market Discovery (`market_discovery.py`)
**Purpose:** Discovers active 15-minute Bitcoin prediction markets from Polymarket API
- **Frequency:** Checks every 15 minutes (aligned to market windows)
- **Output:** Publishes market metadata to `market-updates` topic
- **Key Fields:** market_id, condition_id, token_ids, start_time, end_time, question

### 2. Market Resolution (`market_resolution.py`)
**Purpose:** Tracks market outcomes and captures final results
- **Trigger:** Consumes from `market-updates`
- **Strategy:** Spawns thread per market, polls for resolution after market ends
- **Retry Logic:** 20 attempts with exponential backoff (up to 87 minutes)
- **Output:** Publishes resolutions to `market-resolutions` or `market-resolution-failures`

### 3. Coinbase BTC Producer (`coinbase_producer.py`)
**Purpose:** Streams real-time Bitcoin prices from Coinbase WebSocket
- **Frequency:** 1 message per second (throttled)
- **Auto-Recovery:** Exponential backoff reconnection (5s → 60s max)
- **Error Logging:** Tracks disconnects, retry attempts, and downtime
- **Output:** Publishes to `asset-prices` topic

### 4. Polymarket WebSocket Manager (`polymarket_ws_manager.py`)
**Purpose:** Captures real-time orderbook data from Polymarket CLOB
- **Trigger:** Consumes from `market-updates`, subscribes to market tokens
- **Events Captured:**
  - `orderbook_summary`: Best bid/ask, book imbalance, large orders
  - `price_change`: Price movements (BUY side only)
  - `trade`: Actual executions with size/price
- **Auto-Recovery:** Reconnects mid-market on disconnect
- **Output:** Publishes to `polymarket-prices` topic

### 5. User Positions Tracker (`user_positions_tracker.py`)
**Purpose:** Tracks wallet positions via Goldsky GraphQL subgraph
- **Frequency:** Queries every 10 seconds during active markets
- **Data Captured:** Up to 1,000 largest positions per snapshot
- **Thread Model:** Spawns isolated thread per market
- **Error Handling:** Publishes alert after 5 consecutive failures
- **Output:** Individual position messages to `user-positions` topic

### 6. Database Writer (`db_writer.py`)
**Purpose:** Persists all Kafka streams to PostgreSQL Bronze layer
- **Architecture:** Multi-threaded consumer (7 topics concurrently)
- **Error Handling:** Retries on connection loss, skips bad messages
- **Offset Management:** Auto-commit enabled for complete historical record

---

## Data Schema

### Bronze Layer (Raw JSONB Storage)

```sql
-- Market metadata
bronze.market_updates
  - market_id, condition_id, question, token_ids, start/end times

-- Market outcomes
bronze.market_resolutions
  - market_id, winner (Up/Down), final prices, volume

-- BTC price feed
bronze.btc_prices
  - symbol, price, timestamp, volume_24h

-- Orderbook events
bronze.polymarket_prices
  - type (orderbook_summary | price_change | trade)
  - market_id, asset_id, outcome, prices, sizes, imbalances

-- User positions
bronze.user_positions
  - type (position | position_snapshot_empty)
  - market_id, condition_id, user (wallet), balance, outcome

-- Service errors
bronze.service_errors
  - service_name, error_type, details, timestamp
```

### Key Insights from Current Data
- **Position Distribution:** $0.00 - $17,000 per position
- **Average Position:** $100-180 (healthy retail participation)
- **Whale Threshold:** $1,000+ positions
- **Dust Positions:** < $1 (will filter in Silver layer)

---

## Setup & Deployment

### Prerequisites
- Docker & Docker Compose
- 8GB+ RAM
- 50GB+ disk space (for data growth)

### Infrastructure Setup

```bash
# 1. Start core infrastructure (Kafka, Postgres, UI tools)
docker-compose up -d

# 2. Verify infrastructure
docker ps  # Should see 4 containers running
```

### Service Deployment

```bash
# 3. Build service images
docker-compose -f docker-compose.services.yml build

# 4. Start all data collection services
docker-compose -f docker-compose.services.yml up -d

# 5. Monitor logs
docker-compose -f docker-compose.services.yml logs -f
```

### Database Access

```bash
# Via pgAdmin (Web UI)
http://localhost:5050
Email: admin@admin.com
Password: admin

# Via psql (Command Line)
docker exec -it postgres psql -U postgres -d trading_data
```

### Kafka Monitoring

```bash
# Kafka UI (Web Interface)
http://localhost:8080
```

---

## Learning Objectives & Timeline

**Primary Goal:** Develop production-grade data engineering skills through hands-on implementation of a real-time streaming architecture.

### Phase 1: Real-Time Data Pipeline (Current - Week 1-2)
**Learning Focus:** Kafka streaming, Docker orchestration, error handling, system design
- ✅ Build 6-service real-time pipeline
- ✅ Implement error handling & auto-recovery
- ✅ Add whale position tracking via GraphQL
- ⏳ Collect 7 days of continuous data (~650 markets)

**Skills Developed:**
- Apache Kafka producer/consumer patterns
- WebSocket connection management
- Multi-threaded service architecture
- Docker containerization & orchestration
- Production error handling strategies

### Phase 2: Data Transformation (Week 3)
**Learning Focus:** dbt, SQL modeling, data quality
- Build dbt Silver layer (cleaned & joined data)
- Build dbt Gold layer (analytical features)
- Implement data quality checks
- Feature categories:
  - Price momentum & volatility
  - Orderbook imbalance signals
  - Whale position changes
  - Participant concentration metrics

**Skills Developed:**
- dbt project structure & best practices
- Incremental model design
- SQL window functions & aggregations
- Data quality testing

### Phase 3: ML Feature Engineering (Week 4)
**Learning Focus:** Feature engineering, time-series analysis, ML pipeline design
- Export Gold layer features
- Build feature engineering pipeline
- Analyze feature correlations
- Create ML training datasets

**Skills Developed:**
- Time-series feature engineering
- Feature selection techniques
- ML pipeline architecture
- Backtesting methodologies

### Phase 4: Model Development (Week 5+)
**Learning Focus:** ML model training, evaluation, deployment
- Train classification models (scikit-learn, XGBoost)
- Implement backtesting framework
- Model evaluation & iteration
- Explore deployment patterns

**Skills Developed:**
- Classification model training
- Cross-validation strategies
- Model evaluation metrics
- MLOps concepts

---

## Data Volume Projections

### Current Collection Rates
- **BTC Prices:** ~86K/day
- **Orderbook Events:** ~20M/day
- **Position Snapshots:** ~500K/day
- **Markets:** 96/day

### Storage Requirements
- **Week 1:** ~150M rows (~15GB)
- **Month 1:** ~600M rows (~60GB)
- **Considerations:** Implement data retention policy before long-term deployment

---

## Error Handling & Reliability

### Auto-Recovery Mechanisms
- **WebSocket Disconnects:** Exponential backoff (5s → 60s max)
- **API Failures:** Retry with configurable thresholds
- **Database Issues:** Connection retry with exponential backoff
- **Service Restarts:** Resume from latest state or wait for next market

### Error Logging
All services publish errors to `service-errors` topic after threshold:
- **Coinbase Producer:** Tracks disconnects, retry attempts, downtime
- **Position Tracker:** Alerts after 5 consecutive query failures
- **Service Restarts:** Logs mid-market gaps with estimated missed data

### Observability
- Real-time: Docker logs (`docker-compose logs -f`)
- Historical: `bronze.service_errors` table
- Metrics: Kafka UI for topic throughput

---

## Key Architectural Decisions

### Why Kafka?
- Decouples data collection from storage
- Multiple consumers per topic (enables hot/cold path pattern)
- Built-in fault tolerance and replay capability
- Industry-standard streaming platform (resume-worthy)

### Why Medallion Architecture?
- **Bronze (Raw):** Reprocessable source of truth
- **Silver (Cleaned):** Standardized business logic
- **Gold (Features):** Optimized for analytics/ML consumption
- Aligns with modern data warehouse best practices

### Why Individual Position Messages?
- Aligns with Bronze raw data principle
- Better query flexibility (filter by user, market, outcome)
- Avoids Kafka message size limits (1MB default)
- Natural fit for dbt aggregation in Silver layer

### Why 10-Second Position Polling?
- Blockchain confirmations take 2-5 seconds
- 10s interval captures 90 snapshots per 15-min market
- Balance between API politeness (Goldsky: 50 req/10s limit) and data granularity

---

## Known Limitations

1. **Goldsky Rate Limits:** 50 requests per 10 seconds (currently using ~2%)
2. **Subgraph Lag:** 30-60 second delay behind blockchain
3. **Position Query Limit:** Top 1,000 positions per market (captures all whales)
4. **Internet Dependency:** Home internet may cause occasional gaps (acceptable for learning project)
5. **Storage Growth:** Will need retention policy for long-term deployment

---

## Future Enhancements

### Short-Term (Learning Phase)
- [ ] Add data quality checks (Great Expectations)
- [ ] Implement dbt daily runs (cron or Airflow)
- [ ] Build monitoring dashboard (Grafana)
- [ ] Document dbt project structure
- [ ] Add unit tests for critical functions

### Long-Term (Production Concepts)
- [ ] Explore cloud deployment (AWS/GCP)
- [ ] Database replication patterns
- [ ] Advanced monitoring & alerting
- [ ] CI/CD pipeline setup
- [ ] Cost optimization strategies

---

## Tech Debt & Cleanup Items

- [ ] Add connection error logging to polymarket_ws_manager
- [ ] Create data retention/archival strategy
- [ ] Implement comprehensive logging framework
- [ ] Add configuration management (environment variables)
- [ ] Document API rate limits and quotas

---

## References & Documentation

- **Polymarket API:** https://docs.polymarket.com/
- **Goldsky Subgraphs:** GraphQL endpoints for blockchain data
- **Coinbase WebSocket:** https://docs.cloud.coinbase.com/exchange/docs/websocket-overview
- **dbt Documentation:** https://docs.getdbt.com/
- **Kafka Documentation:** https://kafka.apache.org/documentation/

---

## Project Context

This project serves as a comprehensive learning experience in modern data engineering, covering the full data lifecycle from real-time ingestion through transformation to ML feature engineering. The prediction market domain provides a rich, high-frequency data environment that mirrors real-world data engineering challenges at scale.

**Educational Focus Areas:**
- Real-time streaming architecture
- Distributed systems design
- Data modeling & warehousing
- ML pipeline development
- Production system reliability

**Author:** David 
**Project Start:** January 2026  
**Current Phase:** Data Collection  
**Status:** Active Development
