"""
Main entry point for the data ingestion pipeline.
Orchestrates fetching live data and generating mock transactions.
"""

import pandas as pd
from pathlib import Path
from datetime import datetime
from .config import (
    RAW_DATA_DIR, DAILY_BATCH_SIZE, BQ_TABLE_FULL, NUM_USERS, BQ_USERS_TABLE_FULL, 
    NUM_DEPOSITS, BQ_DEPOSITS_TABLE_FULL, NUM_WITHDRAWALS, BQ_WITHDRAWALS_TABLE_FULL,
    NUM_TRADES, BQ_TRADES_TABLE_FULL, NUM_ORDERS, BQ_ORDERS_TABLE_FULL
)
from .apis import fetch_crypto_prices, fetch_exchange_rates
from .generators import (
    generate_mock_ramp_data, generate_mock_users, generate_mock_deposits,
    generate_mock_withdrawals, generate_mock_trades, generate_mock_orders,
    generate_trades_from_orders  # Import new function
)
from .bigquery_loader import (
    get_bigquery_client,
    create_table_if_not_exists,
    load_dataframe_to_bigquery,
    create_users_table_if_not_exists,
    load_dataframe_to_table,
    create_deposits_table_if_not_exists,
    create_withdrawals_table_if_not_exists,
    create_trades_table_if_not_exists,
    create_orders_table_if_not_exists,
    get_user_ids_from_bigquery  # Import new function
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
    
    print("📊 Fetching user_ids from BigQuery...")
    user_ids = get_user_ids_from_bigquery(client)
    
    if not user_ids:
        print("\n❌ ERROR: No user_ids found in BigQuery!")
        print("💡 Please run: python -c 'from src.ingestion.main import load_users_table; load_users_table()'")
        print("💡 This will populate the users table first.\n")
        return
    
    print(f"✅ Found {len(user_ids)} users. Generating transactions using real user_ids...")
    
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
        target_date=batch_date,  # New parameter for date-specific generation
        user_ids=user_ids  # Pass real user_ids for referential integrity
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


def load_users_table():
    """
    One-time operation to populate the users table.
    Run this before generating transactions to ensure user_ids exist.
    """
    print("👥 Starting Users Table Population...")
    
    client = get_bigquery_client()
    print(f"✅ Connected to BigQuery project")
    
    # Create users table if it doesn't exist
    create_users_table_if_not_exists(client)
    
    # Generate user data
    users_df = generate_mock_users(NUM_USERS)
    
    # Load to BigQuery
    print(f"📤 Loading {len(users_df)} users to BigQuery...")
    rows_loaded = load_dataframe_to_table(client, users_df, BQ_USERS_TABLE_FULL)
    print(f"✅ Successfully loaded {rows_loaded} rows to {BQ_USERS_TABLE_FULL}")
    
    # Save backup CSV
    output_path = RAW_DATA_DIR / "users.csv"
    users_df.to_csv(output_path, index=False)
    print(f"💾 Backup saved to: {output_path}")
    
    print(f"📊 Sample Data:\n{users_df.head(3)}")
    print("✅ Users table population complete!")


def load_deposits_table():
    """
    One-time operation to populate the deposits table.
    """
    print("💰 Starting Deposits Table Population...")
    
    client = get_bigquery_client()
    print(f"✅ Connected to BigQuery project")
    
    user_ids = get_user_ids_from_bigquery(client)
    if not user_ids:
        print("❌ ERROR: Load users table first!")
        return
    
    # Create deposits table if it doesn't exist
    create_deposits_table_if_not_exists(client)
    
    # Generate deposit data
    deposits_df = generate_mock_deposits(NUM_DEPOSITS, user_ids=user_ids)  # Pass user_ids
    
    # Load to BigQuery
    print(f"📤 Loading {len(deposits_df)} deposits to BigQuery...")
    rows_loaded = load_dataframe_to_table(client, deposits_df, BQ_DEPOSITS_TABLE_FULL)
    print(f"✅ Successfully loaded {rows_loaded} rows to {BQ_DEPOSITS_TABLE_FULL}")
    
    # Save backup CSV
    output_path = RAW_DATA_DIR / "deposits.csv"
    deposits_df.to_csv(output_path, index=False)
    print(f"💾 Backup saved to: {output_path}")
    
    print(f"📊 Sample Data:\n{deposits_df.head(3)}")
    print("✅ Deposits table population complete!")


def load_withdrawals_table():
    """One-time operation to populate the withdrawals table."""
    print("💸 Starting Withdrawals Table Population...")
    
    client = get_bigquery_client()
    
    user_ids = get_user_ids_from_bigquery(client)
    if not user_ids:
        print("❌ ERROR: Load users table first!")
        return
    
    create_withdrawals_table_if_not_exists(client)
    
    withdrawals_df = generate_mock_withdrawals(NUM_WITHDRAWALS, user_ids=user_ids)  # Pass user_ids
    
    # Load to BigQuery
    print(f"📤 Loading {len(withdrawals_df)} withdrawals to BigQuery...")
    rows_loaded = load_dataframe_to_table(client, withdrawals_df, BQ_WITHDRAWALS_TABLE_FULL)
    print(f"✅ Successfully loaded {rows_loaded} rows to {BQ_WITHDRAWALS_TABLE_FULL}")
    
    # Save backup CSV
    output_path = RAW_DATA_DIR / "withdrawals.csv"
    withdrawals_df.to_csv(output_path, index=False)
    print(f"💾 Backup saved to: {output_path}")
    
    print(f"📊 Sample Data:\n{withdrawals_df.head(3)}")
    print("✅ Withdrawals table population complete!")


def load_orders_table():
    """One-time operation to populate the orders table."""
    print("📋 Starting Orders Table Population...")
    
    client = get_bigquery_client()
    
    user_ids = get_user_ids_from_bigquery(client)
    if not user_ids:
        print("❌ ERROR: Load users table first!")
        return
    
    create_orders_table_if_not_exists(client)
    
    # Fetch current crypto prices
    crypto_prices = fetch_crypto_prices()
    orders_df = generate_mock_orders(NUM_ORDERS, crypto_prices, user_ids=user_ids)  # Pass user_ids
    
    # Load to BigQuery
    print(f"📤 Loading {len(orders_df)} orders to BigQuery...")
    rows_loaded = load_dataframe_to_table(client, orders_df, BQ_ORDERS_TABLE_FULL)
    print(f"✅ Successfully loaded {rows_loaded} rows to {BQ_ORDERS_TABLE_FULL}")
    
    # Save backup CSV
    output_path = RAW_DATA_DIR / "orders.csv"
    orders_df.to_csv(output_path, index=False)
    print(f"💾 Backup saved to: {output_path}")
    
    print(f"📊 Sample Data:\n{orders_df.head(3)}")
    print("✅ Orders table population complete!")


def load_trades_from_orders():
    """
    Generates and loads trades based on filled orders in BigQuery.
    This ensures every trade has a corresponding order.
    """
    print("📈 Starting Trades Table Population from Orders...")
    
    client = get_bigquery_client()
    
    # Create trades table if it doesn't exist
    create_trades_table_if_not_exists(client)
    
    # Query filled orders from BigQuery
    query = f"""
        SELECT *
        FROM `{BQ_ORDERS_TABLE_FULL}`
        WHERE status IN ('filled', 'partially_filled')
    """
    
    print("📥 Fetching filled orders from BigQuery...")
    orders_df = client.query(query).to_dataframe()
    
    if len(orders_df) == 0:
        print("⚠️ No filled orders found. Cannot generate trades.")
        print("💡 Make sure orders table has some 'filled' status orders.")
        return
    
    print(f"✅ Found {len(orders_df)} filled orders")
    
    # Generate trades from these orders
    trades_df = generate_trades_from_orders(orders_df)
    
    # Load to BigQuery
    print(f"📤 Loading {len(trades_df)} trades to BigQuery...")
    rows_loaded = load_dataframe_to_table(client, trades_df, BQ_TRADES_TABLE_FULL)
    print(f"✅ Successfully loaded {rows_loaded} rows to {BQ_TRADES_TABLE_FULL}")
    
    # Save backup CSV
    output_path = RAW_DATA_DIR / "trades.csv"
    trades_df.to_csv(output_path, index=False)
    print(f"💾 Backup saved to: {output_path}")
    
    print(f"📊 Sample Data:\n{trades_df.head(3)}")
    print("✅ Trades table population complete!")


def load_all_tables():
    """
    Convenience function to load all static tables at once.
    Run this once to populate all reference/transaction tables.
    """
    print("🚀 Loading All Exchange Tables (with proper referential integrity)...\n")
    
    print("Step 1/5: Loading Users (foundational table)...")
    load_users_table()
    
    print("\n" + "="*60 + "\n")
    print("Step 2/5: Loading Deposits...")
    load_deposits_table()
    
    print("\n" + "="*60 + "\n")
    print("Step 3/5: Loading Withdrawals...")
    load_withdrawals_table()
    
    print("\n" + "="*60 + "\n")
    print("Step 4/5: Loading Orders...")
    load_orders_table()
    
    print("\n" + "="*60 + "\n")
    print("Step 5/5: Loading Trades (generated FROM filled orders)...")
    load_trades_from_orders()
    
    print("\n" + "="*60 + "\n")
    print("🎉 All tables loaded successfully!")
    print("\n📊 Referential Integrity Guaranteed:")
    print("   ✓ All transactions reference real users")
    print("   ✓ All trades reference real orders")
    print("   ✓ Ready for JOIN queries across tables\n")


# ============================================================================
# SCRIPT ENTRY POINT
# ============================================================================
if __name__ == "__main__":
    # Run the pipeline when script is executed directly
    run_ingestion()
