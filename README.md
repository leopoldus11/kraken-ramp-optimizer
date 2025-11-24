# Kraken Ramp Optimizer – Cohort & LTV Tracker

**A production-grade analytics platform simulating fiat-to-crypto on-ramp optimization.**

## 🚀 Project Goal
To simulate **Kraken Ramp** (fiat → crypto on-ramp) data and build a full finance-grade analytics platform that answers critical business questions:
- **Acquisition:** Which fiat on-ramp cohorts (by country, payment method, first coin) become high-volume traders?
- **Retention:** Which cohorts churn fastest vs. which stay for the long term?
- **Monetization:** What is the LTV (Lifetime Value) and ROAS (Return on Ad Spend) of ramp-acquired users?

This project demonstrates a modern "Modern Data Stack" approach using **dbt Core**, **DuckDB**, **Airflow**, and **Streamlit**, designed to scale from local development to AWS Athena in production.

## 💼 Why This Matters for Kraken
Fiat on-ramps are the **lifeblood of any crypto exchange**. They are the primary funnel for new user acquisition and fresh capital inflow.
- **Optimizing Conversion:** Understanding which payment methods (SEPA vs. Card) fail most often reduces drop-off.
- **High-Value User Identification:** Not all users are equal. A user who on-ramps $5,000 via wire transfer is different from one who on-ramps $20 via Apple Pay. Identifying high-LTV cohorts early allows for targeted VIP retention.
- **Geographic Expansion:** Data-driven decisions on which local currencies or payment rails to support next based on realized demand and friction points.

## 🏗 Architecture
**Phase 1: Local (Current)**
\`\`\`mermaid
graph LR
    A[Raw Data Sources] -->|Python/Faker| B[Raw CSV/Parquet]
    B -->|DuckDB| C[dbt Staging Models]
    C -->|DuckDB| D[dbt Marts]
    D -->|Streamlit| E[Dashboard]
    F[Airflow/Astro] -->|Orchestrates| A
    F -->|Orchestrates| C
    F -->|Orchestrates| D
\`\`\`

## 🛠 Tech Stack
- **Ingestion:** Python 3.11, Pandas, Faker (Mock Data), CoinGecko API (Prices)
- **Warehouse:** DuckDB (Local file-based OLAP)
- **Transformation:** dbt Core (Data Build Tool)
- **Orchestration:** Airflow (via Astro CLI)
- **Visualization:** Streamlit

## ⚡ How to Run Locally in 2 Minutes

**1. Clone & Setup**
\`\`\`bash
git clone https://github.com/your-username/kraken-ramp-optimizer.git
cd kraken-ramp-optimizer
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
\`\`\`

**2. Generate Raw Data (Feature 01)**
\`\`\`bash
# Generates mock users and fetches live crypto prices
python src/ingestion/main.py
# Check output
ls data/raw/ramp_transactions.csv
\`\`\`

**3. Run dbt (Coming in Feature 02)**
*Pending implementation...*

---
*Built by [Your Name] for the Kraken Analytics Engineer Interview.*
\`\`\`

```text file=".gitignore"
# Byte-compiled / optimized / DLL files
__pycache__/
*.py[cod]
*$py.class

# C extensions
*.so

# Distribution / packaging
.Python
build/
develop-eggs/
dist/
downloads/
eggs/
.eggs/
lib/
lib64/
parts/
sdist/
var/
wheels/
share/python-wheels/
*.egg-info/
.installed.cfg
*.egg
MANIFEST

# Virtual environments
venv/
env/
ENV/

# Data (Local only, do not commit real data)
data/
!data/.gitkeep

# dbt
dbt_project/target/
dbt_project/dbt_packages/
dbt_project/logs/

# Airflow
airflow/logs/
airflow/tmp/

# IDE settings
.vscode/
.idea/
.DS_Store
