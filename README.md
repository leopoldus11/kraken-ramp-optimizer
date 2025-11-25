# Kraken Ramp Optimizer

**A production-grade analytics platform for fiat-to-crypto on-ramp optimization.**

## Current Status: Feature 01 - Raw Data Ingestion

This feature simulates realistic Kraken on-ramp transaction data by:
- Fetching live cryptocurrency prices from CoinGecko API
- Fetching live foreign exchange rates
- Generating 500 mock user transactions with realistic patterns
- Saving raw data to CSV for downstream processing

## Project Vision

This project will demonstrate a modern data analytics stack for crypto exchanges, answering:
- **Acquisition:** Which user cohorts (by country, payment method, first coin) become high-volume traders?
- **Retention:** Which cohorts churn fastest vs. stay long-term?
- **Monetization:** What is the LTV (Lifetime Value) and ROAS of ramp-acquired users?

### Planned Features
- **Feature 02:** dbt + DuckDB for data transformation and modeling
- **Feature 03:** Airflow orchestration for automated pipelines
- **Feature 04:** Streamlit dashboard for business insights
- **Feature 05:** AWS deployment (Athena, S3, Lambda)

## Quick Start

**1. Setup Environment**
\`\`\`bash
python3.11 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
\`\`\`

**2. Run Data Ingestion**
\`\`\`bash
python -m src.ingestion.main
\`\`\`

**3. Verify Output**
\`\`\`bash
ls data/raw/ramp_transactions.csv
\`\`\`

## Tech Stack
- **Language:** Python 3.11
- **Data Generation:** Pandas, Faker
- **External APIs:** CoinGecko (crypto prices), Open Exchange Rates (FX rates)
- **Storage:** CSV (raw layer)

---

*Building a portfolio-grade data engineering project, one feature at a time.*
