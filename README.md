# Kraken Ramp Optimizer

**A cloud-native analytics platform for fiat-to-crypto on-ramp optimization.**

## Current Status: Feature 02 - BigQuery Integration + Incremental Loading

This feature builds on Feature 01 by adding:
- Cloud data warehouse integration with Google BigQuery
- Incremental daily batch processing (idempotent pipeline)
- State management for tracking processed dates
- Production-grade data loading patterns

### What It Does
1. Fetches live cryptocurrency prices (CoinGecko) and FX rates
2. Generates 500 realistic transactions for the next unprocessed date
3. Loads data to BigQuery with automatic schema management
4. Tracks state to enable "run again to process next day" workflow

## Project Vision

Build a portfolio-grade analytics stack mirroring Kraken's infrastructure, answering:
- **Acquisition:** Which user cohorts become high-volume traders?
- **Retention:** Which cohorts churn vs. stay long-term?
- **Monetization:** What is the LTV and ROAS of ramp-acquired users?

### Planned Features
- **Feature 03:** dbt transformations (staging, marts, cohort analysis)
- **Feature 04:** Looker Studio dashboards for business insights
- **Feature 05:** Apache Airflow orchestration
- **Feature 06:** Advanced analytics (retention curves, LTV modeling)

## Quick Start

### 1. Setup Google Cloud BigQuery

Complete the BigQuery setup steps:
- Create GCP project named `kraken-ramp-optimizer`
- Enable BigQuery API
- Create service account with BigQuery Admin role
- Download JSON credentials to `credentials/bigquery-service-account.json`
- Create dataset named `kraken_ramp_raw` in BigQuery

### 2. Setup Python Environment

\`\`\`bash
python3.11 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
\`\`\`

### 3. Configure Credentials

\`\`\`bash
export GOOGLE_APPLICATION_CREDENTIALS="/absolute/path/to/your/credentials/bigquery-service-account.json"
\`\`\`

### 4. Run Ingestion Pipeline

\`\`\`bash
# First run: generates data for 90 days ago
python -m src.ingestion.main

# Run again: processes next day
python -m src.ingestion.main

# Keep running to backfill up to today
\`\`\`

### 5. Query Your Data in BigQuery

\`\`\`sql
SELECT 
  DATE(timestamp) as date,
  COUNT(*) as transactions,
  SUM(fiat_amount) as total_volume_usd
FROM `kraken-ramp-optimizer.kraken_ramp_raw.ramp_transactions`
GROUP BY date
ORDER BY date DESC
\`\`\`

## Tech Stack

| Layer | Technology | Purpose |
|-------|-----------|---------|
| **Storage** | Google BigQuery | Cloud data warehouse |
| **Transformation** | dbt (planned) | SQL-based data modeling |
| **Orchestration** | Airflow (planned) | Pipeline scheduling |
| **Visualization** | Looker Studio (planned) | Business intelligence dashboards |
| **Language** | Python 3.11 | Data generation & API calls |

## Project Structure

\`\`\`
kraken-ramp-optimizer/
├── src/
│   └── ingestion/
│       ├── main.py              # Pipeline orchestration
│       ├── config.py            # Configuration constants
│       ├── apis.py              # External API clients
│       ├── generators.py        # Mock data generation
│       ├── bigquery_loader.py   # BigQuery operations
│       └── state_manager.py     # Incremental loading logic
├── data/
│   ├── raw/                     # CSV backups
│   └── metadata/                # Pipeline state (last_run.json)
├── credentials/                 # GCP service account keys (git ignored)
├── requirements.txt
└── README.md
\`\`\`

---

*Demonstrating cloud-native data engineering for crypto analytics.*
