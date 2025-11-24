import os
from pathlib import Path

# Paths
BASE_DIR = Path(__file__).resolve().parents[2]
DATA_DIR = BASE_DIR / "data"
RAW_DATA_DIR = DATA_DIR / "raw"

# Ensure directories exist
RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)

# API Endpoints
COINGECKO_API_URL = "https://api.coingecko.com/api/v3/simple/price"
EXCHANGERATE_API_URL = "https://open.er-api.com/v6/latest/USD"

# Configuration
NUM_MOCK_USERS = 500
START_DATE = "-90d"  # 90 days ago
SUPPORTED_FIAT = ["USD", "EUR", "GBP", "CAD"]
SUPPORTED_CRYPTO = ["bitcoin", "ethereum", "solana", "tether"]
PAYMENT_METHODS = ["credit_card", "debit_card", "ach_transfer", "sepa", "apple_pay"]
