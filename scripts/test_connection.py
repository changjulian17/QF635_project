#!/usr/bin/env python3
"""Connectivity check for Binance Spot Testnet. Reads credentials from .env via config.py."""
import sys, pathlib; sys.path.insert(0, str(pathlib.Path(__file__).parent.parent))

from binance.client import Client
from config import settings


def test_binance_connection() -> bool:
    print(f"API key loaded: {'yes' if settings.BINANCE_API_KEY else 'NO — check .env'}")
    print(f"API secret loaded: {'yes' if settings.BINANCE_API_SECRET else 'NO — check .env'}")
    print(f"Testnet mode: {settings.BINANCE_TESTNET}")
    print("-" * 50)

    try:
        client = Client(
            api_key=settings.BINANCE_API_KEY,
            api_secret=settings.BINANCE_API_SECRET,
            testnet=settings.BINANCE_TESTNET,
        )

        server_time = client.get_server_time()
        print(f"✓ Connected to Binance testnet")
        print(f"✓ Server time: {server_time}")

        account = client.get_account()
        balances = [b for b in account["balances"] if float(b["free"]) > 0 or float(b["locked"]) > 0]
        print(f"✓ Account authenticated — {len(balances)} non-zero balance(s):")
        for b in balances:
            print(f"    {b['asset']:>6}: free={b['free']}, locked={b['locked']}")

        ticker = client.get_symbol_ticker(symbol=settings.SYMBOL)
        print(f"✓ {settings.SYMBOL} last price: {ticker['price']}")

        return True

    except Exception as exc:
        print(f"✗ Connection failed: {exc}")
        return False


if __name__ == "__main__":
    print("Testing Binance testnet connection...")
    print("-" * 50)
    ok = test_binance_connection()
    print("-" * 50)
    print("PASSED" if ok else "FAILED")
