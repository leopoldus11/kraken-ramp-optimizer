"""
API integration module for fetching live market data.
Handles cryptocurrency prices and foreign exchange rates with fallback logic.
"""

import requests
import pandas as pd
from typing import Dict, Any
from .config import COINGECKO_API_URL, EXCHANGERATE_API_URL, SUPPORTED_CRYPTO


def fetch_crypto_prices() -> Dict[str, float]:
    """
    Fetches current cryptocurrency prices in USD from CoinGecko API.
    
    Returns:
        Dict[str, float]: Mapping of crypto token names to USD prices
                         Example: {'bitcoin': 45000.0, 'ethereum': 2400.0}
    
    Fallback Strategy:
        If the API call fails (network issues, rate limits, etc.), 
        returns hardcoded fallback prices to keep the pipeline running.
    """
    try:
        # Build comma-separated list of crypto IDs for API request
        ids = ",".join(SUPPORTED_CRYPTO)
        
        # Make GET request to CoinGecko
        response = requests.get(
            COINGECKO_API_URL,
            params={"ids": ids, "vs_currencies": "usd"}
        )
        
        # Raise exception if HTTP error occurred (4xx, 5xx)
        response.raise_for_status()
        
        # Parse JSON response
        data = response.json()
        
        # Flatten nested structure: {'bitcoin': {'usd': 50000}} → {'bitcoin': 50000}
        prices = {k: v['usd'] for k, v in data.items()}
        
        print(f"✅ Fetched live crypto prices: {prices}")
        return prices
        
    except Exception as e:
        # Log the error but don't crash the pipeline
        print(f"⚠️ Failed to fetch crypto prices: {e}")
        print("Using fallback prices.")
        
        # Return reasonable default prices (as of design time)
        return {
            "bitcoin": 65000.0,
            "ethereum": 3500.0,
            "solana": 140.0,
            "tether": 1.0
        }


def fetch_exchange_rates() -> Dict[str, float]:
    """
    Fetches foreign exchange rates relative to USD from Open Exchange Rates API.
    
    Returns:
        Dict[str, float]: Mapping of currency codes to conversion rates
                         Example: {'EUR': 0.92, 'GBP': 0.79, 'USD': 1.0}
                         
    How to use:
        To convert 100 EUR to USD: 100 / rates['EUR'] = ~108.70 USD
    """
    try:
        # Make GET request to exchange rate API
        response = requests.get(EXCHANGERATE_API_URL)
        response.raise_for_status()
        
        # Parse JSON and extract rates object
        data = response.json()
        rates = data.get("rates", {})
        
        print(f"✅ Fetched live FX rates.")
        return rates
        
    except Exception as e:
        # Fallback to approximate rates if API fails
        print(f"⚠️ Failed to fetch FX rates: {e}")
        return {"USD": 1.0, "EUR": 0.92, "GBP": 0.79, "CAD": 1.35}
