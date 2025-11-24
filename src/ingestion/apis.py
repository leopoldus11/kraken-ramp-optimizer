import requests
import pandas as pd
from typing import Dict, Any
from .config import COINGECKO_API_URL, EXCHANGERATE_API_URL, SUPPORTED_CRYPTO

def fetch_crypto_prices() -> Dict[str, float]:
    """
    Fetches current crypto prices in USD from CoinGecko.
    Returns a dictionary: {'bitcoin': 45000.0, 'ethereum': 2400.0, ...}
    """
    try:
        ids = ",".join(SUPPORTED_CRYPTO)
        response = requests.get(
            COINGECKO_API_URL,
            params={"ids": ids, "vs_currencies": "usd"}
        )
        response.raise_for_status()
        data = response.json()
        
        # Flatten structure: {'bitcoin': {'usd': 50000}} -> {'bitcoin': 50000}
        prices = {k: v['usd'] for k, v in data.items()}
        print(f"✅ Fetched live crypto prices: {prices}")
        return prices
    except Exception as e:
        print(f"⚠️ Failed to fetch crypto prices: {e}")
        print("Using fallback prices.")
        return {
            "bitcoin": 65000.0,
            "ethereum": 3500.0,
            "solana": 140.0,
            "tether": 1.0
        }

def fetch_exchange_rates() -> Dict[str, float]:
    """
    Fetches fiat exchange rates relative to USD.
    Returns a dictionary: {'EUR': 0.92, 'GBP': 0.79, ...}
    """
    try:
        response = requests.get(EXCHANGERATE_API_URL)
        response.raise_for_status()
        data = response.json()
        rates = data.get("rates", {})
        print(f"✅ Fetched live FX rates.")
        return rates
    except Exception as e:
        print(f"⚠️ Failed to fetch FX rates: {e}")
        return {"USD": 1.0, "EUR": 0.92, "GBP": 0.79, "CAD": 1.35}
