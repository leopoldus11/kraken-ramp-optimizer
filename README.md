# Kraken Ramp Optimizer

**A cloud-native analytics platform simulating Kraken's crypto exchange data infrastructure.**

## Current Status: Feature 03 - Multi-Table Exchange Simulation

This feature extends the pipeline with a complete exchange simulation including:
- **6 interconnected raw data tables** with referential integrity
- Users, Deposits, Withdrawals, Orders, Trades, and Ramp Transactions
- Proper foreign key relationships (all transactions reference real users)
- Order-to-Trade relationship (trades are generated from filled orders)

---

## Data Model Overview

\`\`\`
┌─────────────────────────────────────────────────────────────┐
│               KRAKEN EXCHANGE DATA MODEL                    │
└─────────────────────────────────────────────────────────────┘

                        ┌──────────┐
                        │  USERS   │ (PRIMARY KEY)
                        ├──────────┤
                        │ user_id  │
                        │ email    │
                        │ country  │
                        │ kyc_     │
                        │ status   │
                        └─────┬────┘
                              │
              ┌───────────────┼───────────────┬──────────────┐
              │               │               │              │
              ▼               ▼               ▼              ▼
      ┌───────────┐   ┌───────────┐   ┌───────────┐  ┌──────────────┐
      │ DEPOSITS  │   │WITHDRAWALS│   │  ORDERS   │  │RAMP_TRANS-   │
      ├───────────┤   ├───────────┤   ├───────────┤  │  ACTIONS     │
      │deposit_id │   │withdrawal │   │ order_id  │  │transaction_id│
      │ user_id───┼───┤   _id     │   │ user_id───┼──┤  user_id     │
      │ amount    │   │ user_id───┼───┤  status   │  │ fiat_amount  │
      │ currency  │   │ amount    │   │  side     │  │crypto_amount │
      └───────────┘   └───────────┘   └─────┬─────┘  └──────────────┘
                                            │
                                            │ order_id (FK)
                                            │
                                            ▼
                                      ┌──────────┐
                                      │  TRADES  │
                                      ├──────────┤
                                      │ trade_id │
                                      │ order_id │
                                      │ user_id  │
                                      │  price   │
                                      └──────────┘
\`\`\`

### Referential Integrity Rules

1. **Users → All Tables**: Every `user_id` in transactions, deposits, withdrawals, orders, and trades MUST exist in the users table
2. **Orders → Trades**: Every `order_id` in trades MUST exist in the orders table with status = 'filled' or 'partially_filled'
3. **Loading Order**: Users must be loaded FIRST, then orders, then trades (trades are generated FROM filled orders)

---

## Pipeline Data Flow

\`\`\`
EXTERNAL APIs         GENERATORS            BIGQUERY
─────────────         ──────────            ────────

CoinGecko API ──► crypto_prices ─┐
                                 │
Exchange Rate API ──► fx_rates   │
                                 ▼
                        ┌────────────────┐       ┌───────────────┐
                        │  generate_     │       │               │
                        │  mock_ramp_    │  ───► │ ramp_trans-   │
                        │  data()        │       │   actions     │
                        └────────────────┘       └───────────────┘
                                 ▲
                                 │
                        ┌────────┴────────┐
                        │   user_ids      │◄───────────────┐
                        │  from BigQuery  │                │
                        └─────────────────┘                │
                                                           │
                        ┌────────────────┐       ┌────────┴──────┐
                        │  generate_     │       │               │
                        │  mock_users()  │  ───► │    users      │
                        └────────────────┘       │ (LOADED FIRST)│
                                                 └───────────────┘
                        ┌────────────────┐       ┌───────────────┐
                        │  generate_     │       │               │
                        │mock_deposits() │  ───► │   deposits    │
                        └────────────────┘       └───────────────┘

                        ┌────────────────┐       ┌───────────────┐
                        │  generate_     │       │               │
                        │ mock_orders()  │  ───► │   orders      │──┐
                        └────────────────┘       └───────────────┘  │
                                                                    │
                        ┌────────────────┐       ┌───────────────┐  │
                        │  generate_     │       │               │  │
                        │ trades_from_   │  ───► │    trades     │◄─┘
                        │  orders()      │       │ (from filled  │
                        └────────────────┘       │   orders)     │
                                                 └───────────────┘
\`\`\`

---

## Project Vision

Build a portfolio-grade analytics stack mirroring Kraken's infrastructure, answering:
- **Acquisition:** Which user cohorts become high-volume traders?
- **Retention:** Which cohorts churn vs. stay long-term?
- **Monetization:** What is the LTV and ROAS of ramp-acquired users?

### Planned Features
- **Feature 04:** dbt transformations (staging, marts, cohort analysis)
- **Feature 05:** Looker Studio dashboards for business insights
- **Feature 06:** Apache Airflow orchestration
- **Feature 07:** Advanced analytics (retention curves, LTV modeling)

---

## Quick Start

### 1. Setup Google Cloud BigQuery

1. Create GCP project named `kraken-ramp-optimizer`
2. Enable BigQuery API
3. Create service account with **BigQuery Admin** role
4. Download JSON credentials to `credentials/bigquery-service-account.json`
5. Create dataset named `kraken_ramp_raw` (location: **US**)

### 2. Setup Python Environment

\`\`\`bash
python3.11 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
\`\`\`

### 3. Configure Credentials

\`\`\`bash
export GOOGLE_APPLICATION_CREDENTIALS="/absolute/path/to/credentials/bigquery-service-account.json"
\`\`\`

### 4. Load All Tables (First Time Setup)

\`\`\`bash
# Load all 6 tables with proper referential integrity
python -c "from src.ingestion.main import load_all_tables; load_all_tables()"
\`\`\`

This will load tables in the correct order:
1. **users** (1,000 users) - foundational table
2. **deposits** (800 deposits) - references users
3. **withdrawals** (600 withdrawals) - references users
4. **orders** (3,000 orders) - references users
5. **trades** (generated from filled orders) - references orders AND users
6. **ramp_transactions** (incremental daily)

### 5. Run Incremental Ramp Transactions

\`\`\`bash
# Generate daily ramp transactions (500 per day)
python -m src.ingestion.main

# Run again to process next day
python -m src.ingestion.main
\`\`\`

### 6. Query Your Data in BigQuery

\`\`\`sql
-- Join users with their transactions
SELECT 
  u.user_id,
  u.country,
  u.account_tier,
  COUNT(t.transaction_id) as total_transactions,
  SUM(t.fiat_amount) as total_volume
FROM `kraken-ramp-optimizer.kraken_ramp_raw.users` u
LEFT JOIN `kraken-ramp-optimizer.kraken_ramp_raw.ramp_transactions` t
  ON u.user_id = t.user_id
GROUP BY u.user_id, u.country, u.account_tier
ORDER BY total_volume DESC
LIMIT 10;

-- Verify order-to-trade integrity
SELECT 
  o.order_id,
  o.status,
  t.trade_id,
  t.base_amount
FROM `kraken-ramp-optimizer.kraken_ramp_raw.orders` o
INNER JOIN `kraken-ramp-optimizer.kraken_ramp_raw.trades` t
  ON o.order_id = t.order_id
LIMIT 10;
\`\`\`

---

## Tech Stack

| Layer | Technology | Purpose |
|-------|-----------|---------|
| **Storage** | Google BigQuery | Cloud data warehouse |
| **Transformation** | dbt (planned) | SQL-based data modeling |
| **Orchestration** | Airflow (planned) | Pipeline scheduling |
| **Visualization** | Looker Studio (planned) | Business intelligence dashboards |
| **Language** | Python 3.11 | Data generation & API calls |

---

## Project Structure

\`\`\`
kraken-ramp-optimizer/
├── src/
│   └── ingestion/
│       ├── main.py              # Pipeline orchestration & table loaders
│       ├── config.py            # Configuration constants & table names
│       ├── apis.py              # External API clients (CoinGecko, FX rates)
│       ├── generators.py        # Mock data generation for all tables
│       ├── bigquery_loader.py   # BigQuery schemas & loading functions
│       └── state_manager.py     # Incremental loading state tracking
├── data/
│   ├── raw/                     # CSV backups for all tables
│   └── metadata/                # Pipeline state (last_run.json)
├── credentials/                 # GCP service account keys (git ignored)
├── requirements.txt
└── README.md
\`\`\`

---

## Module Documentation

### `main.py` - Pipeline Orchestration
Entry point for all data loading operations. Key functions:
- `run_ingestion()` - Daily incremental ramp transaction loading
- `load_all_tables()` - One-time setup for all reference tables
- `load_users_table()` - Load users (must run first)
- `load_trades_from_orders()` - Generate trades from filled orders

### `generators.py` - Data Generation
Creates realistic mock data with proper business logic:
- **Transactions**: 1.5% fee, 85% completed / 10% failed / 5% pending
- **Users**: 70% KYC verified, account tiers (basic/intermediate/pro)
- **Orders**: 40% filled, 30% open, 20% cancelled
- **Trades**: Generated ONLY from filled/partially_filled orders

### `bigquery_loader.py` - BigQuery Operations
Handles all database operations:
- Table creation with proper schemas
- Data loading with WRITE_APPEND
- `get_user_ids_from_bigquery()` - Fetch real user_ids for referential integrity

### `config.py` - Configuration
All constants in one place:
- API endpoints (CoinGecko, Exchange Rates)
- BigQuery project/dataset/table names
- Data generation parameters (batch sizes, user counts)

### `state_manager.py` - Incremental Loading
Tracks pipeline state for idempotent runs:
- Stores last processed date in `data/metadata/last_run.json`
- Prevents duplicate processing
- Enables "run again for next day" workflow

---

## BigQuery Tables Reference

| Table | Records | Description |
|-------|---------|-------------|
| `users` | 1,000 | User accounts with KYC status, tiers |
| `deposits` | 800 | Fiat and crypto deposits |
| `withdrawals` | 600 | Fiat and crypto withdrawals |
| `orders` | 3,000 | Buy/sell orders (limit and market) |
| `trades` | ~1,200 | Executed trades (from filled orders) |
| `ramp_transactions` | 500/day | Fiat-to-crypto on-ramp purchases |

---

*Demonstrating cloud-native data engineering for crypto analytics.*
