"""
Mock data generation module.
Creates realistic synthetic transaction data using Faker and business logic.
"""

import random
import pandas as pd
from faker import Faker
from datetime import datetime, timedelta
from typing import List, Dict
from .config import SUPPORTED_FIAT, SUPPORTED_CRYPTO, PAYMENT_METHODS

# Initialize Faker for generating realistic fake data (names, UUIDs, countries, etc.)
fake = Faker()


def generate_mock_ramp_data(
    num_records: int, 
    crypto_prices: Dict[str, float], 
    fx_rates: Dict[str, float],
    target_date: datetime = None  # Added parameter for date-specific generation
) -> pd.DataFrame:
    """
    Generates mock fiat-to-crypto on-ramp transactions with realistic patterns.
    
    Args:
        num_records: Number of transactions to generate
        crypto_prices: Current USD prices for each crypto token
        fx_rates: Exchange rates for fiat currencies
        target_date: Optional specific date to generate transactions for (for incremental loading)
    
    Returns:
        pd.DataFrame: Synthetic transaction data ready for analysis
        
    Business Logic:
        - Transaction amounts follow a realistic distribution (mostly small, some large)
        - Timestamps are weighted towards recent activity
        - 1.5% platform fee is deducted before crypto purchase
        - 85% completed, 10% failed, 5% pending (realistic conversion rates)
    """
    records = []
    
    print(f"🔄 Generating {num_records} mock transactions...")
    
    for _ in range(num_records):
        # ====================================================================
        # TIMESTAMP GENERATION
        # ====================================================================
        if target_date:
            # If target_date is provided, generate timestamp within that specific day
            start_of_day = datetime.combine(target_date, datetime.min.time())
            # Random time throughout the 24-hour period (0-86400 seconds)
            random_seconds = random.randint(0, 86400)
            txn_date = start_of_day + timedelta(seconds=random_seconds)
        else:
            # Original behavior: Generate transaction date within last 90 days
            # Skewed towards recent activity (random uniform distribution)
            days_ago = random.randint(0, 90)
            # Add random time within that day
            txn_date = datetime.now() - timedelta(days=days_ago, minutes=random.randint(0, 1440))
        
        # ====================================================================
        # USER & GEOGRAPHIC DATA
        # ====================================================================
        # Use Faker to generate realistic country codes (e.g., 'US', 'DE', 'JP')
        country_code = fake.country_code()
        
        # ====================================================================
        # FIAT SIDE (What the user pays)
        # ====================================================================
        # Randomly select which fiat currency the user pays in
        fiat_currency = random.choice(SUPPORTED_FIAT)
        
        # Generate realistic transaction amount ($20 to $5000)
        # In production, this would be weighted towards smaller amounts
        fiat_amount = round(random.uniform(20.0, 5000.0), 2)
        
        # ====================================================================
        # CRYPTO SIDE (What the user receives)
        # ====================================================================
        # Randomly select which cryptocurrency the user wants to buy
        crypto_token = random.choice(SUPPORTED_CRYPTO)
        
        # ====================================================================
        # AMOUNT CALCULATION (2-step conversion)
        # ====================================================================
        
        # Step 1: Convert user's fiat currency to USD
        # Example: 100 EUR → 100 / 0.92 = ~108.70 USD
        usd_rate = fx_rates.get(fiat_currency, 1.0)
        amount_in_usd = fiat_amount / usd_rate
        
        # Step 2: Deduct platform fee (1.5% of USD value)
        fee_rate = 0.015
        net_amount_usd = amount_in_usd * (1 - fee_rate)
        
        # Step 3: Convert net USD amount to cryptocurrency
        # Example: $1000 USD / $50,000 BTC price = 0.02 BTC
        token_price_usd = crypto_prices.get(crypto_token, 0)
        crypto_amount = net_amount_usd / token_price_usd if token_price_usd > 0 else 0
        
        # ====================================================================
        # BUILD TRANSACTION RECORD
        # ====================================================================
        record = {
            "transaction_id": fake.uuid4(),          # Unique transaction identifier
            "user_id": fake.uuid4(),                 # Unique user identifier
            "timestamp": txn_date,                    # When transaction occurred
            "fiat_currency": fiat_currency,           # What user paid with
            "fiat_amount": fiat_amount,               # How much user paid
            "crypto_token": crypto_token,             # What user bought
            "crypto_amount": round(crypto_amount, 8), # How much crypto received (8 decimals standard)
            "payment_method": random.choice(PAYMENT_METHODS),  # How user paid
            "country": country_code,                  # User's country
            # Transaction status with weighted probabilities (realistic conversion funnel)
            "status": random.choices(
                ["completed", "failed", "pending"], 
                weights=[0.85, 0.10, 0.05]
            )[0],
            "fee_usd": round(amount_in_usd * fee_rate, 2)  # Fee charged in USD
        }
        
        records.append(record)
    
    # Convert list of dictionaries to pandas DataFrame
    df = pd.DataFrame(records)
    
    return df
