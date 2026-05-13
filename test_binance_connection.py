#!/usr/bin/env python3
"""
Test script to verify Binance testnet connection
"""

from binance.client import Client

# Binance testnet configuration
testnet_api_key = "YOUR_API_KEY"
testnet_api_secret = "YOUR_API_SECRET"
testnet_url = "https://testnet.binance.vision/api"

def test_binance_connection():
    """Test connection to Binance testnet"""
    try:
        # Initialize client pointing to testnet
        client = Client(
            api_key=testnet_api_key,
            api_secret=testnet_api_secret,
            testnet=True  # Use testnet
        )
        
        # Test API connection - get server time
        server_time = client.get_server_time()
        print(f"✓ Connected to Binance testnet successfully!")
        print(f"✓ Server time: {server_time}")
        
        # Get account info (only works with valid API keys)
        try:
            account = client.get_account()
            print(f"✓ Account retrieved successfully")
            print(f"✓ Number of balances: {len(account['balances'])}")
        except Exception as e:
            print(f"✗ Account retrieval failed (API keys may be invalid): {e}")
        
        return True
        
    except Exception as e:
        print(f"✗ Connection failed: {e}")
        return False

if __name__ == "__main__":
    print("Testing Binance testnet connection...")
    print("-" * 50)
    test_binance_connection()
    print("-" * 50)
    print("To use with real API keys:")
    print("1. Get testnet API keys from: https://testnet.binance.vision/")
    print("2. Replace YOUR_API_KEY and YOUR_API_SECRET above")
    print("3. Run this script with: source venv/bin/activate && python test_binance_connection.py")
