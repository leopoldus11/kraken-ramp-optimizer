"""
BigQuery client and data loading utilities.
Handles authentication, table creation, and data insertion.
"""

from google.cloud import bigquery
from google.api_core import exceptions
import pandas as pd
from .config import GCP_PROJECT_ID, BQ_DATASET, BQ_TABLE, BQ_TABLE_FULL


def get_bigquery_client():
    """
    Initialize and return authenticated BigQuery client.
    
    Authentication is handled via GOOGLE_APPLICATION_CREDENTIALS env var,
    which should point to your service account JSON key file.
    
    Returns:
        bigquery.Client: Authenticated BigQuery client instance
    """
    return bigquery.Client(project=GCP_PROJECT_ID)


def create_table_if_not_exists(client):
    """
    Create the ramp_transactions table in BigQuery if it doesn't exist.
    
    This function is idempotent - safe to run multiple times.
    
    Table Schema:
        - transaction_id: Unique identifier for each transaction
        - user_id: User who initiated the transaction
        - timestamp: When the transaction occurred
        - fiat_currency: Currency user paid with (USD, EUR, etc.)
        - fiat_amount: Amount in fiat currency
        - crypto_token: Cryptocurrency purchased
        - crypto_amount: Amount of crypto received
        - payment_method: How user paid (credit_card, ACH, etc.)
        - country: User's country code
        - status: Transaction status (completed, pending, failed)
        - fee_usd: Transaction fee in USD
    
    Args:
        client: Authenticated BigQuery client
    """
    # Define the table schema with column names, types, and descriptions
    schema = [
        bigquery.SchemaField("transaction_id", "STRING", mode="REQUIRED", description="Unique transaction identifier"),
        bigquery.SchemaField("user_id", "STRING", mode="REQUIRED", description="User identifier"),
        bigquery.SchemaField("timestamp", "TIMESTAMP", mode="REQUIRED", description="Transaction timestamp"),
        bigquery.SchemaField("fiat_currency", "STRING", mode="REQUIRED", description="Fiat currency code (USD, EUR, etc.)"),
        bigquery.SchemaField("fiat_amount", "FLOAT64", mode="REQUIRED", description="Amount in fiat currency"),
        bigquery.SchemaField("crypto_token", "STRING", mode="REQUIRED", description="Cryptocurrency purchased"),
        bigquery.SchemaField("crypto_amount", "FLOAT64", mode="REQUIRED", description="Amount of crypto received"),
        bigquery.SchemaField("payment_method", "STRING", mode="REQUIRED", description="Payment method used"),
        bigquery.SchemaField("country", "STRING", mode="REQUIRED", description="User country code"),
        bigquery.SchemaField("status", "STRING", mode="REQUIRED", description="Transaction status"),
        bigquery.SchemaField("fee_usd", "FLOAT64", mode="REQUIRED", description="Transaction fee in USD"),
    ]
    
    # Create table reference object
    table = bigquery.Table(BQ_TABLE_FULL, schema=schema)
    
    try:
        # Attempt to create table (does nothing if already exists)
        table = client.create_table(table)
        print(f"✅ Created table {BQ_TABLE_FULL}")
    except exceptions.Conflict:
        # Table already exists - this is fine, we want idempotent operations
        print(f"ℹ️  Table {BQ_TABLE_FULL} already exists")


def load_dataframe_to_bigquery(client, df: pd.DataFrame):
    """
    Load a pandas DataFrame into BigQuery table.
    
    Uses streaming insert which is immediate but has quota limits.
    For production, consider using load jobs instead.
    
    Args:
        client: Authenticated BigQuery client
        df: Pandas DataFrame with transaction data
    
    Returns:
        int: Number of rows successfully loaded
    """
    # Configure the load job
    job_config = bigquery.LoadJobConfig(
        # WRITE_APPEND adds new rows without deleting existing data
        write_disposition="WRITE_APPEND",
        
        # Automatically detect schema from DataFrame
        # (should match our defined schema)
        autodetect=False,
    )
    
    # Start the load job (asynchronous operation)
    job = client.load_table_from_dataframe(
        df, 
        BQ_TABLE_FULL, 
        job_config=job_config
    )
    
    # Wait for job to complete (synchronous)
    job.result()
    
    return len(df)


def query_last_transaction_date(client):
    """
    Query BigQuery to find the most recent transaction date.
    
    This is used to determine what date to generate next batch for.
    
    Args:
        client: Authenticated BigQuery client
    
    Returns:
        str: ISO format date (YYYY-MM-DD) of last transaction, or None if table is empty
    """
    query = f"""
        SELECT MAX(DATE(timestamp)) as last_date
        FROM `{BQ_TABLE_FULL}`
    """
    
    try:
        # Execute query and fetch results
        query_job = client.query(query)
        results = query_job.result()
        
        # Extract the single row result
        for row in results:
            if row.last_date:
                return row.last_date.isoformat()
        
        # Table exists but is empty
        return None
    
    except exceptions.NotFound:
        # Table doesn't exist yet
        return None


def create_users_table_if_not_exists(client, dataset: str = BQ_DATASET):
    """
    Create the users table in BigQuery if it doesn't exist.
    
    Table Schema:
        - user_id: Unique identifier for each user
        - email: User's email address
        - signup_date: When user created account
        - country: User's country
        - kyc_status: KYC verification status
        - account_tier: User's account tier
        - account_balance_usd: Current account balance in USD
        - is_active: Whether account is active
        - created_at: Record creation timestamp
    
    Args:
        client: Authenticated BigQuery client
        dataset: BigQuery dataset name
    """
    table_name = f"{GCP_PROJECT_ID}.{dataset}.users"
    
    schema = [
        bigquery.SchemaField("user_id", "STRING", mode="REQUIRED", description="Unique user identifier"),
        bigquery.SchemaField("email", "STRING", mode="REQUIRED", description="User email address"),
        bigquery.SchemaField("signup_date", "TIMESTAMP", mode="REQUIRED", description="Account creation date"),
        bigquery.SchemaField("country", "STRING", mode="REQUIRED", description="User country code"),
        bigquery.SchemaField("kyc_status", "STRING", mode="REQUIRED", description="KYC verification status"),
        bigquery.SchemaField("account_tier", "STRING", mode="REQUIRED", description="Account tier level"),
        bigquery.SchemaField("account_balance_usd", "FLOAT64", mode="REQUIRED", description="Current balance in USD"),
        bigquery.SchemaField("is_active", "BOOLEAN", mode="REQUIRED", description="Account active status"),
        bigquery.SchemaField("created_at", "TIMESTAMP", mode="REQUIRED", description="Record creation timestamp"),
    ]
    
    table = bigquery.Table(table_name, schema=schema)
    
    try:
        table = client.create_table(table)
        print(f"✅ Created table {table_name}")
    except exceptions.Conflict:
        print(f"ℹ️  Table {table_name} already exists")


def load_dataframe_to_table(client, df: pd.DataFrame, table_name: str):
    """
    Load a pandas DataFrame into any BigQuery table.
    
    Args:
        client: Authenticated BigQuery client
        df: Pandas DataFrame to load
        table_name: Full table name (project.dataset.table)
    
    Returns:
        int: Number of rows successfully loaded
    """
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",
        autodetect=False,
    )
    
    job = client.load_table_from_dataframe(
        df, 
        table_name, 
        job_config=job_config
    )
    
    job.result()
    return len(df)


def create_deposits_table_if_not_exists(client, dataset: str = BQ_DATASET):
    """
    Create the deposits table in BigQuery if it doesn't exist.
    
    Table Schema:
        - deposit_id: Unique identifier for each deposit
        - user_id: User making the deposit
        - timestamp: When deposit was initiated
        - deposit_type: 'fiat' or 'crypto'
        - currency: Currency code (USD, BTC, etc.)
        - amount: Deposit amount
        - payment_method: How deposit was made
        - status: Deposit status
        - blockchain_confirmations: Number of confirmations (crypto only)
        - created_at: Record creation timestamp
    
    Args:
        client: Authenticated BigQuery client
        dataset: BigQuery dataset name
    """
    table_name = f"{GCP_PROJECT_ID}.{dataset}.deposits"
    
    schema = [
        bigquery.SchemaField("deposit_id", "STRING", mode="REQUIRED", description="Unique deposit identifier"),
        bigquery.SchemaField("user_id", "STRING", mode="REQUIRED", description="User identifier"),
        bigquery.SchemaField("timestamp", "TIMESTAMP", mode="REQUIRED", description="Deposit initiation time"),
        bigquery.SchemaField("deposit_type", "STRING", mode="REQUIRED", description="Type of deposit (fiat or crypto)"),
        bigquery.SchemaField("currency", "STRING", mode="REQUIRED", description="Currency code"),
        bigquery.SchemaField("amount", "FLOAT64", mode="REQUIRED", description="Deposit amount"),
        bigquery.SchemaField("payment_method", "STRING", mode="REQUIRED", description="Payment method used"),
        bigquery.SchemaField("status", "STRING", mode="REQUIRED", description="Deposit status"),
        bigquery.SchemaField("blockchain_confirmations", "INT64", mode="NULLABLE", description="Blockchain confirmations (crypto only)"),
        bigquery.SchemaField("created_at", "TIMESTAMP", mode="REQUIRED", description="Record creation timestamp"),
    ]
    
    table = bigquery.Table(table_name, schema=schema)
    
    try:
        table = client.create_table(table)
        print(f"✅ Created table {table_name}")
    except exceptions.Conflict:
        print(f"ℹ️  Table {table_name} already exists")


def create_withdrawals_table_if_not_exists(client, dataset: str = BQ_DATASET):
    """Create the withdrawals table in BigQuery if it doesn't exist."""
    table_name = f"{GCP_PROJECT_ID}.{dataset}.withdrawals"
    
    schema = [
        bigquery.SchemaField("withdrawal_id", "STRING", mode="REQUIRED", description="Unique withdrawal identifier"),
        bigquery.SchemaField("user_id", "STRING", mode="REQUIRED", description="User identifier"),
        bigquery.SchemaField("timestamp", "TIMESTAMP", mode="REQUIRED", description="Withdrawal initiation time"),
        bigquery.SchemaField("withdrawal_type", "STRING", mode="REQUIRED", description="Type of withdrawal (fiat or crypto)"),
        bigquery.SchemaField("currency", "STRING", mode="REQUIRED", description="Currency code"),
        bigquery.SchemaField("amount", "FLOAT64", mode="REQUIRED", description="Withdrawal amount"),
        bigquery.SchemaField("fee", "FLOAT64", mode="REQUIRED", description="Withdrawal fee"),
        bigquery.SchemaField("destination_type", "STRING", mode="REQUIRED", description="Destination type"),
        bigquery.SchemaField("tx_hash", "STRING", mode="NULLABLE", description="Blockchain transaction hash"),
        bigquery.SchemaField("status", "STRING", mode="REQUIRED", description="Withdrawal status"),
        bigquery.SchemaField("created_at", "TIMESTAMP", mode="REQUIRED", description="Record creation timestamp"),
    ]
    
    table = bigquery.Table(table_name, schema=schema)
    
    try:
        table = client.create_table(table)
        print(f"✅ Created table {table_name}")
    except exceptions.Conflict:
        print(f"ℹ️  Table {table_name} already exists")


def create_trades_table_if_not_exists(client, dataset: str = BQ_DATASET):
    """Create the trades table in BigQuery if it doesn't exist."""
    table_name = f"{GCP_PROJECT_ID}.{dataset}.trades"
    
    schema = [
        bigquery.SchemaField("trade_id", "STRING", mode="REQUIRED", description="Unique trade identifier"),
        bigquery.SchemaField("order_id", "STRING", mode="REQUIRED", description="Order that generated this trade"),
        bigquery.SchemaField("user_id", "STRING", mode="REQUIRED", description="User identifier"),
        bigquery.SchemaField("timestamp", "TIMESTAMP", mode="REQUIRED", description="Trade execution time"),
        bigquery.SchemaField("trading_pair", "STRING", mode="REQUIRED", description="Trading pair (e.g. BTC/USD)"),
        bigquery.SchemaField("side", "STRING", mode="REQUIRED", description="Trade side (buy or sell)"),
        bigquery.SchemaField("base_currency", "STRING", mode="REQUIRED", description="Base currency"),
        bigquery.SchemaField("quote_currency", "STRING", mode="REQUIRED", description="Quote currency"),
        bigquery.SchemaField("base_amount", "FLOAT64", mode="REQUIRED", description="Amount in base currency"),
        bigquery.SchemaField("quote_amount", "FLOAT64", mode="REQUIRED", description="Amount in quote currency"),
        bigquery.SchemaField("price", "FLOAT64", mode="REQUIRED", description="Execution price"),
        bigquery.SchemaField("fee_amount", "FLOAT64", mode="REQUIRED", description="Trading fee amount"),
        bigquery.SchemaField("fee_currency", "STRING", mode="REQUIRED", description="Fee currency"),
        bigquery.SchemaField("order_type", "STRING", mode="REQUIRED", description="Order type (market or limit)"),
        bigquery.SchemaField("is_maker", "BOOLEAN", mode="REQUIRED", description="Whether this was a maker order"),
        bigquery.SchemaField("created_at", "TIMESTAMP", mode="REQUIRED", description="Record creation timestamp"),
    ]
    
    table = bigquery.Table(table_name, schema=schema)
    
    try:
        table = client.create_table(table)
        print(f"✅ Created table {table_name}")
    except exceptions.Conflict:
        print(f"ℹ️  Table {table_name} already exists")


def create_orders_table_if_not_exists(client, dataset: str = BQ_DATASET):
    """Create the orders table in BigQuery if it doesn't exist."""
    table_name = f"{GCP_PROJECT_ID}.{dataset}.orders"
    
    schema = [
        bigquery.SchemaField("order_id", "STRING", mode="REQUIRED", description="Unique order identifier"),
        bigquery.SchemaField("user_id", "STRING", mode="REQUIRED", description="User identifier"),
        bigquery.SchemaField("timestamp", "TIMESTAMP", mode="REQUIRED", description="Order placement time"),
        bigquery.SchemaField("trading_pair", "STRING", mode="REQUIRED", description="Trading pair (e.g. BTC/USD)"),
        bigquery.SchemaField("side", "STRING", mode="REQUIRED", description="Order side (buy or sell)"),
        bigquery.SchemaField("order_type", "STRING", mode="REQUIRED", description="Order type (market or limit)"),
        bigquery.SchemaField("base_currency", "STRING", mode="REQUIRED", description="Base currency"),
        bigquery.SchemaField("quote_currency", "STRING", mode="REQUIRED", description="Quote currency"),
        bigquery.SchemaField("base_amount", "FLOAT64", mode="REQUIRED", description="Total order amount"),
        bigquery.SchemaField("filled_amount", "FLOAT64", mode="REQUIRED", description="Amount filled"),
        bigquery.SchemaField("limit_price", "FLOAT64", mode="NULLABLE", description="Limit price (for limit orders)"),
        bigquery.SchemaField("status", "STRING", mode="REQUIRED", description="Order status"),
        bigquery.SchemaField("created_at", "TIMESTAMP", mode="REQUIRED", description="Record creation timestamp"),
    ]
    
    table = bigquery.Table(table_name, schema=schema)
    
    try:
        table = client.create_table(table)
        print(f"✅ Created table {table_name}")
    except exceptions.Conflict:
        print(f"ℹ️  Table {table_name} already exists")


def get_user_ids_from_bigquery(client, dataset: str = BQ_DATASET, limit: int = 10000):
    """
    Fetches real user_ids from the users table in BigQuery.
    This ensures referential integrity across all tables.
    
    Args:
        client: BigQuery client
        dataset: BigQuery dataset name
        limit: Maximum number of user_ids to fetch
    
    Returns:
        list: List of user_id strings
    """
    table_name = f"{GCP_PROJECT_ID}.{dataset}.users"
    
    query = f"""
        SELECT user_id 
        FROM `{table_name}`
        WHERE is_active = TRUE
        LIMIT {limit}
    """
    
    try:
        query_job = client.query(query)
        results = query_job.result()
        user_ids = [row.user_id for row in results]
        print(f"✅ Fetched {len(user_ids)} real user_ids from BigQuery")
        return user_ids
    except Exception as e:
        print(f"⚠️  Could not fetch user_ids from BigQuery: {e}")
        print(f"💡 Make sure to run load_users_table() first!")
        return []
