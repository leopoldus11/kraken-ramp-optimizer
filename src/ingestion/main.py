"""
Main entry point for the data ingestion pipeline.
Orchestrates fetching live data and generating mock transactions.
"""

import pandas as pd
from pathlib import Path
from .config import RAW_DATA_DIR, NUM_MOCK_USERS
from .apis import fetch_crypto_prices, fetch_exchange_rates
from .generators import generate_mock_ramp_data


def run_ingestion():
    """
    Execute the full data ingestion workflow.
    
    Pipeline Steps:
        1. Fetch live cryptocurrency prices from CoinGecko
        2. Fetch live foreign exchange rates
        3. Generate realistic mock transaction data
        4. Save data to CSV in the raw data layer
    
    Output:
        CSV file at data/raw/ramp_transactions.csv
    """
    print("🚀 Starting Ramp Data Ingestion...")
    
    # ========================================================================
    # STEP 1: Fetch External Market Data
    # ========================================================================
    # Get current crypto prices in USD (e.g., BTC = $65,000)
    crypto_prices = fetch_crypto_prices()
    
    # Get foreign exchange rates relative to USD (e.g., 1 EUR = 0.92 USD)
    fx_rates = fetch_exchange_rates()
    
    # ========================================================================
    # STEP 2: Generate Mock Transaction Data
    # ========================================================================
    # Create realistic synthetic transactions using live market data
    df = generate_mock_ramp_data(NUM_MOCK_USERS, crypto_prices, fx_rates)
    
    # ========================================================================
    # STEP 3: Save to Raw Data Layer
    # ========================================================================
    # Define output path (data/raw/ramp_transactions.csv)
    output_path = RAW_DATA_DIR / "ramp_transactions.csv"
    
    # Write DataFrame to CSV (no index column needed)
    df.to_csv(output_path, index=False)
    
    # ========================================================================
    # STEP 4: Log Results
    # ========================================================================
    print(f"💾 Data successfully saved to: {output_path}")
    print(f"📊 Sample Data:\n{df.head(3)}")  # Show first 3 rows
    print("✅ Ingestion Complete.")


# ============================================================================
# SCRIPT ENTRY POINT
# ============================================================================
if __name__ == "__main__":
    # Run the pipeline when script is executed directly
    run_ingestion()
