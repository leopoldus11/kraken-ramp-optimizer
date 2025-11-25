"""
Main entry point for the data ingestion pipeline.
Orchestrates fetching live data and generating mock transactions.
"""

import pandas as pd
from pathlib import Path
from datetime import datetime
from .config import RAW_DATA_DIR, DAILY_BATCH_SIZE, BQ_TABLE_FULL
from .apis import fetch_crypto_prices, fetch_exchange_rates
from .generators import generate_mock_ramp_data
from .bigquery_loader import (
    get_bigquery_client,
    create_table_if_not_exists,
    load_dataframe_to_bigquery
)
from .state_manager import get_next_batch_date, save_last_run_date


def run_ingestion():
    """
    Execute the full data ingestion workflow with incremental loading.
    
    Pipeline Steps:
        1. Determine next batch date to process
        2. Fetch live cryptocurrency prices from CoinGecko
        3. Fetch live foreign exchange rates
        4. Generate realistic mock transaction data for that date
        5. Load data to BigQuery
        6. Save state metadata for next run
    
    Output:
        - Data loaded to BigQuery table
        - CSV backup in data/raw/ (optional)
        - Metadata saved in data/metadata/last_run.json
    """
    print("🚀 Starting Ramp Data Ingestion (BigQuery + Incremental)...")
    
    # ========================================================================
    # STEP 1: Determine Next Batch Date
    # ========================================================================
    batch_date = get_next_batch_date()
    
    if batch_date is None:
        print("✅ Pipeline is up to date. Nothing to process.")
        return
    
    # ========================================================================
    # STEP 2: Initialize BigQuery Client
    # ========================================================================
    client = get_bigquery_client()
    print(f"✅ Connected to BigQuery project")
    
    # Ensure table exists (idempotent - safe to run every time)
    create_table_if_not_exists(client)
    
    # ========================================================================
    # STEP 3: Fetch External Market Data
    # ========================================================================
    # Get current crypto prices in USD (e.g., BTC = $65,000)
    crypto_prices = fetch_crypto_prices()
    
    # Get foreign exchange rates relative to USD (e.g., 1 EUR = 0.92 USD)
    fx_rates = fetch_exchange_rates()
    
    # ========================================================================
    # STEP 4: Generate Mock Transaction Data for Specific Date
    # ========================================================================
    print(f"📅 Generating {DAILY_BATCH_SIZE} transactions for {batch_date}...")
    df = generate_mock_ramp_data(
        DAILY_BATCH_SIZE, 
        crypto_prices, 
        fx_rates,
        target_date=batch_date  # New parameter for date-specific generation
    )
    
    # ========================================================================
    # STEP 5: Load to BigQuery
    # ========================================================================
    print(f"📤 Loading {len(df)} rows to BigQuery...")
    rows_loaded = load_dataframe_to_bigquery(client, df)
    print(f"✅ Successfully loaded {rows_loaded} rows to {BQ_TABLE_FULL}")
    
    # ========================================================================
    # STEP 6: Save CSV Backup (Optional)
    # ========================================================================
    # Save to local CSV for backup/debugging
    output_path = RAW_DATA_DIR / f"ramp_transactions_{batch_date}.csv"
    df.to_csv(output_path, index=False)
    print(f"💾 Backup saved to: {output_path}")
    
    # ========================================================================
    # STEP 7: Update State Metadata
    # ========================================================================
    save_last_run_date(batch_date)
    
    # ========================================================================
    # STEP 8: Log Results
    # ========================================================================
    print(f"📊 Sample Data:\n{df.head(3)}")  # Show first 3 rows
    print("✅ Ingestion Complete.")
    print(f"\n💡 Run again to process next day's data!")


# ============================================================================
# SCRIPT ENTRY POINT
# ============================================================================
if __name__ == "__main__":
    # Run the pipeline when script is executed directly
    run_ingestion()
