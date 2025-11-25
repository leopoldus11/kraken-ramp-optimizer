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
        - country: User's country
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
