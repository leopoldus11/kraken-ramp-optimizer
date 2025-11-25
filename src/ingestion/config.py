"""
Configuration file for the data ingestion pipeline.
Defines paths, API endpoints, and business logic constants.
"""

import os
from pathlib import Path

# ============================================================================
# DIRECTORY STRUCTURE
# ============================================================================

# Get the absolute path to the project root (2 levels up from this file)
BASE_DIR = Path(__file__).resolve().parents[2]

# Define data directory structure
DATA_DIR = BASE_DIR / "data"
RAW_DATA_DIR = DATA_DIR / "raw"

# Ensure the raw data directory exists (creates it if missing)
RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)

# ============================================================================
# EXTERNAL API ENDPOINTS
# ============================================================================

# CoinGecko API: Free tier for fetching cryptocurrency prices
COINGECKO_API_URL = "https://api.coingecko.com/api/v3/simple/price"

# Open Exchange Rates API: Free tier for fiat currency conversion rates
EXCHANGERATE_API_URL = "https://open.er-api.com/v6/latest/USD"

# ============================================================================
# BUSINESS LOGIC CONFIGURATION
# ============================================================================

# Number of mock transactions to generate
NUM_MOCK_USERS = 500

# Time range for transaction generation (90 days of historical data)
START_DATE = "-90d"

# Supported fiat currencies (what users can pay with)
SUPPORTED_FIAT = ["USD", "EUR", "GBP", "CAD"]

# Supported cryptocurrencies (what users can purchase)
# Note: These IDs must match CoinGecko's API format
SUPPORTED_CRYPTO = ["bitcoin", "ethereum", "solana", "tether"]

# Payment methods available on the platform
PAYMENT_METHODS = ["credit_card", "debit_card", "ach_transfer", "sepa", "apple_pay"]

DAILY_BATCH_SIZE = 500

# ============================================================================
# BIGQUERY CONFIGURATION
# ============================================================================

# Google Cloud Project ID (replace with your project ID)
GCP_PROJECT_ID = "kraken-ramp-optimizer"

# BigQuery dataset name (created in GCP Console)
BQ_DATASET = "kraken_ramp_raw"

# BigQuery table name for raw transactions
BQ_TABLE = "ramp_transactions"

# Full table reference in BigQuery format: project.dataset.table
BQ_TABLE_FULL = f"{GCP_PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}"

# ============================================================================
# METADATA CONFIGURATION
# ============================================================================

# Directory for storing pipeline metadata (last run dates, etc.)
METADATA_DIR = DATA_DIR / "metadata"
METADATA_DIR.mkdir(parents=True, exist_ok=True)

# File path for tracking last successful ingestion date
LAST_RUN_FILE = METADATA_DIR / "last_run.json"
