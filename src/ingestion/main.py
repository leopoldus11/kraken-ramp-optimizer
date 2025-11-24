import pandas as pd
from pathlib import Path
from .config import RAW_DATA_DIR, NUM_MOCK_USERS
from .apis import fetch_crypto_prices, fetch_exchange_rates
from .generators import generate_mock_ramp_data

def run_ingestion():
    print("🚀 Starting Ramp Data Ingestion...")
    
    # 1. Fetch External Data
    crypto_prices = fetch_crypto_prices()
    fx_rates = fetch_exchange_rates()
    
    # 2. Generate Mock Data
    df = generate_mock_ramp_data(NUM_MOCK_USERS, crypto_prices, fx_rates)
    
    # 3. Save to Raw Layer (CSV for now, easily readable)
    output_path = RAW_DATA_DIR / "ramp_transactions.csv"
    df.to_csv(output_path, index=False)
    
    print(f"💾 Data successfully saved to: {output_path}")
    print(f"📊 Sample Data:\n{df.head(3)}")
    print("✅ Ingestion Complete.")

if __name__ == "__main__":
    run_ingestion()
