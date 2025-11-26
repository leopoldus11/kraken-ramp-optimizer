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
    target_date: datetime = None,  # Added parameter for date-specific generation
    user_ids: List[str] = None  # Added parameter for real user_ids
) -> pd.DataFrame:
    """
    Generates mock fiat-to-crypto on-ramp transactions with realistic patterns.
    
    Args:
        num_records: Number of transactions to generate
        crypto_prices: Current USD prices for each crypto token
        fx_rates: Exchange rates for fiat currencies
        target_date: Optional specific date to generate transactions for (for incremental loading)
        user_ids: List of real user_ids from the users table (ensures referential integrity)
    
    Returns:
        pd.DataFrame: Synthetic transaction data ready for analysis
        
    Business Logic:
        - Transaction amounts follow a realistic distribution (mostly small, some large)
        - Timestamps are weighted towards recent activity
        - 1.5% platform fee is deducted before crypto purchase
        - 85% completed, 10% failed, 5% pending (realistic conversion rates)
    """
    if not user_ids:
        raise ValueError("user_ids must be provided to ensure referential integrity with users table")
    
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
            "user_id": random.choice(user_ids),     # Use real user_id from users table
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

def generate_mock_users(num_users: int, start_date: datetime = None) -> pd.DataFrame:
    """
    Generates mock user accounts with realistic patterns.
    
    Args:
        num_users: Number of user accounts to generate
        start_date: Earliest possible signup date (defaults to 2 years ago)
    
    Returns:
        pd.DataFrame: Synthetic user data
        
    Business Logic:
        - Users sign up over time (weighted towards recent)
        - KYC status: 70% verified, 20% pending, 10% rejected
        - Account tiers: 60% basic, 30% intermediate, 10% pro
        - Each user has unique email and persistent user_id
    """
    records = []
    
    if start_date is None:
        # Default: Users could have signed up anytime in last 2 years
        start_date = datetime.now() - timedelta(days=730)
    
    print(f"👥 Generating {num_users} mock users...")
    
    for _ in range(num_users):
        # Signup date - weighted towards recent signups
        days_since_start = random.randint(0, 730)
        signup_date = start_date + timedelta(days=days_since_start)
        
        # User demographics
        country_code = fake.country_code()
        
        # KYC status with realistic distribution
        kyc_status = random.choices(
            ["verified", "pending", "rejected"],
            weights=[0.70, 0.20, 0.10]
        )[0]
        
        # Account tier (higher tiers are less common)
        account_tier = random.choices(
            ["basic", "intermediate", "pro"],
            weights=[0.60, 0.30, 0.10]
        )[0]
        
        # Initial account balance (most users start with $0, some deposit immediately)
        initial_balance = random.choice([0.0] * 7 + [round(random.uniform(100, 10000), 2)] * 3)
        
        record = {
            "user_id": fake.uuid4(),
            "email": fake.email(),
            "signup_date": signup_date,
            "country": country_code,
            "kyc_status": kyc_status,
            "account_tier": account_tier,
            "account_balance_usd": initial_balance,
            "is_active": random.choice([True] * 9 + [False]),  # 90% active
            "created_at": signup_date
        }
        
        records.append(record)
    
    df = pd.DataFrame(records)
    return df

def generate_mock_deposits(
    num_deposits: int, 
    target_date: datetime = None,
    user_ids: List[str] = None  # Added parameter
) -> pd.DataFrame:
    """
    Generates mock deposit transactions (fiat and crypto).
    """
    if not user_ids:
        raise ValueError("user_ids must be provided to ensure referential integrity with users table")
    
    records = []
    
    print(f"💰 Generating {num_deposits} mock deposits...")
    
    for _ in range(num_deposits):
        # Timestamp generation
        if target_date:
            start_of_day = datetime.combine(target_date, datetime.min.time())
            random_seconds = random.randint(0, 86400)
            deposit_date = start_of_day + timedelta(seconds=random_seconds)
        else:
            days_ago = random.randint(0, 90)
            deposit_date = datetime.now() - timedelta(days=days_ago, minutes=random.randint(0, 1440))
        
        # Determine if fiat or crypto deposit
        is_fiat = random.random() < 0.7
        
        if is_fiat:
            # Fiat deposit
            currency = random.choice(SUPPORTED_FIAT)
            amount = round(random.uniform(50.0, 10000.0), 2)
            deposit_type = "fiat"
            payment_method = random.choice(["bank_transfer", "wire", "ach_transfer", "sepa"])
        else:
            # Crypto deposit
            currency = random.choice(SUPPORTED_CRYPTO)
            amount = round(random.uniform(0.001, 10.0), 8)
            deposit_type = "crypto"
            payment_method = "blockchain"
        
        # Status distribution
        status = random.choices(
            ["completed", "pending", "failed"],
            weights=[0.90, 0.07, 0.03]
        )[0]
        
        record = {
            "deposit_id": fake.uuid4(),
            "user_id": random.choice(user_ids),  # Use real user_id
            "timestamp": deposit_date,
            "deposit_type": deposit_type,
            "currency": currency,
            "amount": amount,
            "payment_method": payment_method,
            "status": status,
            "blockchain_confirmations": random.randint(1, 20) if not is_fiat else None,
            "created_at": deposit_date
        }
        
        records.append(record)
    
    df = pd.DataFrame(records)
    return df

def generate_mock_withdrawals(
    num_withdrawals: int, 
    target_date: datetime = None,
    user_ids: List[str] = None  # Added parameter
) -> pd.DataFrame:
    """
    Generates mock withdrawal transactions (fiat and crypto).
    """
    if not user_ids:
        raise ValueError("user_ids must be provided to ensure referential integrity with users table")
    
    records = []
    
    print(f"💸 Generating {num_withdrawals} mock withdrawals...")
    
    for _ in range(num_withdrawals):
        # Timestamp generation
        if target_date:
            start_of_day = datetime.combine(target_date, datetime.min.time())
            random_seconds = random.randint(0, 86400)
            withdrawal_date = start_of_day + timedelta(seconds=random_seconds)
        else:
            days_ago = random.randint(0, 90)
            withdrawal_date = datetime.now() - timedelta(days=days_ago, minutes=random.randint(0, 1440))
        
        # Determine if fiat or crypto withdrawal
        is_crypto = random.random() < 0.6
        
        if is_crypto:
            # Crypto withdrawal
            currency = random.choice(SUPPORTED_CRYPTO)
            amount = round(random.uniform(0.001, 5.0), 8)
            withdrawal_type = "crypto"
            destination_type = "wallet_address"
            tx_hash = fake.sha256() if random.random() < 0.85 else None
        else:
            # Fiat withdrawal
            currency = random.choice(SUPPORTED_FIAT)
            amount = round(random.uniform(100.0, 50000.0), 2)
            withdrawal_type = "fiat"
            destination_type = random.choice(["bank_account", "card"])
            tx_hash = None
        
        # Status distribution
        status = random.choices(
            ["completed", "pending", "failed", "rejected"],
            weights=[0.85, 0.10, 0.03, 0.02]
        )[0]
        
        # Fee (0.5% for crypto, $10 flat for fiat)
        fee = amount * 0.005 if is_crypto else 10.0
        
        record = {
            "withdrawal_id": fake.uuid4(),
            "user_id": random.choice(user_ids),  # Use real user_id
            "timestamp": withdrawal_date,
            "withdrawal_type": withdrawal_type,
            "currency": currency,
            "amount": amount,
            "fee": round(fee, 8) if is_crypto else fee,
            "destination_type": destination_type,
            "tx_hash": tx_hash,
            "status": status,
            "created_at": withdrawal_date
        }
        
        records.append(record)
    
    df = pd.DataFrame(records)
    return df


def generate_mock_trades(
    num_trades: int, 
    crypto_prices: Dict[str, float], 
    target_date: datetime = None,
    user_ids: List[str] = None  # Added parameter
) -> pd.DataFrame:
    """
    Generates mock spot trading transactions on the exchange.
    """
    if not user_ids:
        raise ValueError("user_ids must be provided to ensure referential integrity with users table")
    
    records = []
    
    print(f"📈 Generating {num_trades} mock trades...")
    
    for _ in range(num_trades):
        # Timestamp generation
        if target_date:
            start_of_day = datetime.combine(target_date, datetime.min.time())
            random_seconds = random.randint(0, 86400)
            trade_date = start_of_day + timedelta(seconds=random_seconds)
        else:
            days_ago = random.randint(0, 90)
            trade_date = datetime.now() - timedelta(days=days_ago, minutes=random.randint(0, 1440))
        
        # Determine trading pair
        base_currency = random.choice(SUPPORTED_CRYPTO)
        quote_currency = random.choice(SUPPORTED_FIAT + SUPPORTED_CRYPTO)
        
        # Ensure base != quote for crypto/crypto pairs
        while base_currency == quote_currency:
            quote_currency = random.choice(SUPPORTED_CRYPTO)
        
        # Trade side
        side = random.choice(["buy", "sell"])
        
        # Trade amount
        base_amount = round(random.uniform(0.01, 10.0), 8)
        
        # Calculate quote amount based on prices
        base_price_usd = crypto_prices.get(base_currency, 1000)
        if quote_currency in SUPPORTED_CRYPTO:
            quote_price_usd = crypto_prices.get(quote_currency, 1000)
            quote_amount = round(base_amount * base_price_usd / quote_price_usd, 8)
        else:
            # Fiat quote
            quote_amount = round(base_amount * base_price_usd, 2)
        
        # Fee calculation
        is_maker = random.choice([True, False])
        fee_rate = 0.0025 if is_maker else 0.0040
        fee_amount = round(quote_amount * fee_rate, 8)
        
        record = {
            "trade_id": fake.uuid4(),
            "user_id": random.choice(user_ids),  # Use real user_id
            "timestamp": trade_date,
            "trading_pair": f"{base_currency}/{quote_currency}",
            "side": side,
            "base_currency": base_currency,
            "quote_currency": quote_currency,
            "base_amount": base_amount,
            "quote_amount": quote_amount,
            "price": round(quote_amount / base_amount, 8),
            "fee_amount": fee_amount,
            "fee_currency": quote_currency,
            "order_type": random.choice(["market", "limit"]),
            "is_maker": is_maker,
            "created_at": trade_date
        }
        
        records.append(record)
    
    df = pd.DataFrame(records)
    return df


def generate_mock_orders(
    num_orders: int, 
    crypto_prices: Dict[str, float], 
    target_date: datetime = None,
    user_ids: List[str] = None
) -> pd.DataFrame:
    """
    Generates mock order book entries (open, filled, cancelled orders).
    
    NOTE: Orders with status="filled" will be used to generate corresponding trades.
    """
    if not user_ids:
        raise ValueError("user_ids must be provided to ensure referential integrity with users table")
    
    records = []
    
    print(f"📋 Generating {num_orders} mock orders...")
    
    for _ in range(num_orders):
        # Timestamp generation
        if target_date:
            start_of_day = datetime.combine(target_date, datetime.min.time())
            random_seconds = random.randint(0, 86400)
            order_date = start_of_day + timedelta(seconds=random_seconds)
        else:
            days_ago = random.randint(0, 90)
            order_date = datetime.now() - timedelta(days=days_ago, minutes=random.randint(0, 1440))
        
        # Determine trading pair
        base_currency = random.choice(SUPPORTED_CRYPTO)
        quote_currency = random.choice(SUPPORTED_FIAT)
        
        # Order side and type
        side = random.choice(["buy", "sell"])
        order_type = random.choice(["limit", "market"])
        
        # Order amount
        base_amount = round(random.uniform(0.01, 50.0), 8)
        
        # Price (for limit orders, varies from market price)
        base_price_usd = crypto_prices.get(base_currency, 1000)
        if order_type == "limit":
            # Limit orders are +/- 5% from market price
            price_variation = random.uniform(0.95, 1.05)
            limit_price = round(base_price_usd * price_variation, 2)
        else:
            limit_price = None
        
        # Status distribution
        status = random.choices(
            ["filled", "open", "cancelled", "partially_filled", "expired"],
            weights=[0.40, 0.30, 0.20, 0.08, 0.02]
        )[0]
        
        # Filled amount (depends on status)
        if status == "filled":
            filled_amount = base_amount
        elif status == "partially_filled":
            filled_amount = round(base_amount * random.uniform(0.1, 0.9), 8)
        else:
            filled_amount = 0.0
        
        record = {
            "order_id": fake.uuid4(),
            "user_id": random.choice(user_ids),
            "timestamp": order_date,
            "trading_pair": f"{base_currency}/{quote_currency}",
            "side": side,
            "order_type": order_type,
            "base_currency": base_currency,
            "quote_currency": quote_currency,
            "base_amount": base_amount,
            "filled_amount": filled_amount,
            "limit_price": limit_price,
            "status": status,
            "created_at": order_date
        }
        
        records.append(record)
    
    df = pd.DataFrame(records)
    return df


def generate_trades_from_orders(orders_df: pd.DataFrame) -> pd.DataFrame:
    """
    Generates trades ONLY from orders that have been filled.
    
    This ensures referential integrity: every trade references a real order.
    
    Args:
        orders_df: DataFrame of orders (must include filled orders)
    
    Returns:
        pd.DataFrame: Trades generated from filled orders
    """
    # Filter for filled and partially_filled orders only
    filled_orders = orders_df[orders_df['status'].isin(['filled', 'partially_filled'])].copy()
    
    if len(filled_orders) == 0:
        print("⚠️ No filled orders found to generate trades from")
        return pd.DataFrame(columns=[
            'trade_id', 'order_id', 'user_id', 'timestamp', 'trading_pair',
            'side', 'base_currency', 'quote_currency', 'base_amount', 'quote_amount',
            'price', 'fee_amount', 'fee_currency', 'order_type', 'is_maker', 'created_at'
        ])
    
    print(f"📈 Generating {len(filled_orders)} trades from filled orders...")
    
    trades = []
    
    for _, order in filled_orders.iterrows():
        # Use the filled_amount from the order
        base_amount = order['filled_amount']
        
        # Calculate quote amount based on order price or limit price
        if order['limit_price'] is not None:
            price = order['limit_price']
        else:
            # Market order - use a reasonable market price (would come from order book in reality)
            # For simulation, we'll use a price based on typical market rates
            price = round(random.uniform(1000, 100000), 2)  # Simplified
        
        quote_amount = round(base_amount * price, 8)
        
        # Fee calculation
        is_maker = random.choice([True, False])
        fee_rate = 0.0025 if is_maker else 0.0040
        fee_amount = round(quote_amount * fee_rate, 8)
        
        trade = {
            "trade_id": fake.uuid4(),
            "order_id": order['order_id'],  # CRITICAL: Links trade to order
            "user_id": order['user_id'],
            "timestamp": order['timestamp'],
            "trading_pair": order['trading_pair'],
            "side": order['side'],
            "base_currency": order['base_currency'],
            "quote_currency": order['quote_currency'],
            "base_amount": base_amount,
            "quote_amount": quote_amount,
            "price": price,
            "fee_amount": fee_amount,
            "fee_currency": order['quote_currency'],
            "order_type": order['order_type'],
            "is_maker": is_maker,
            "created_at": order['created_at']
        }
        
        trades.append(trade)
    
    return pd.DataFrame(trades)
