import random
import pandas as pd
from faker import Faker
from datetime import datetime, timedelta
from typing import List, Dict
from .config import SUPPORTED_FIAT, SUPPORTED_CRYPTO, PAYMENT_METHODS

fake = Faker()

def generate_mock_ramp_data(
    num_records: int, 
    crypto_prices: Dict[str, float], 
    fx_rates: Dict[str, float]
) -> pd.DataFrame:
    """
    Generates mock fiat-to-crypto on-ramp transactions.
    """
    records = []
    
    print(f"🔄 Generating {num_records} mock transactions...")
    
    for _ in range(num_records):
        # Transaction Timestamp (skewed towards recent)
        days_ago = random.randint(0, 90)
        txn_date = datetime.now() - timedelta(days=days_ago, minutes=random.randint(0, 1440))
        
        # User & Geo
        country_code = fake.country_code()
        
        # Fiat Side
        fiat_currency = random.choice(SUPPORTED_FIAT)
        fiat_amount = round(random.uniform(20.0, 5000.0), 2)
        
        # Crypto Side
        crypto_token = random.choice(SUPPORTED_CRYPTO)
        
        # Calculate Crypto Amount
        # 1. Convert Fiat to USD
        usd_rate = fx_rates.get(fiat_currency, 1.0)
        amount_in_usd = fiat_amount / usd_rate
        
        # 2. Convert USD to Crypto
        token_price_usd = crypto_prices.get(crypto_token, 0)
        # Add spread/fee simulation (e.g., 1.5% fee)
        fee_rate = 0.015
        net_amount_usd = amount_in_usd * (1 - fee_rate)
        crypto_amount = net_amount_usd / token_price_usd if token_price_usd > 0 else 0
        
        record = {
            "transaction_id": fake.uuid4(),
            "user_id": fake.uuid4(),
            "timestamp": txn_date,
            "fiat_currency": fiat_currency,
            "fiat_amount": fiat_amount,
            "crypto_token": crypto_token,
            "crypto_amount": round(crypto_amount, 8),
            "payment_method": random.choice(PAYMENT_METHODS),
            "country": country_code,
            "status": random.choices(["completed", "failed", "pending"], weights=[0.85, 0.10, 0.05])[0],
            "fee_usd": round(amount_in_usd * fee_rate, 2)
        }
        records.append(record)
        
    df = pd.DataFrame(records)
    return df
