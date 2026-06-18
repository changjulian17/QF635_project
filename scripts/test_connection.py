#!/usr/bin/env python3
"""Connectivity check for Binance Spot Testnet. Reads credentials from .env via config.py."""
import sys, pathlib; sys.path.insert(0, str(pathlib.Path(__file__).parent.parent))

from binance.client import Client
from config import settings


def test_binance_connection() -> bool:
    _api_key    = settings.DEMO_BINANCE_API_KEY if settings.BINANCE_DEMO else settings.BINANCE_API_KEY
    _api_secret = settings.DEMO_BINANCE_API_SECRET if settings.BINANCE_DEMO else settings.BINANCE_API_SECRET
    _mode_label = "demo" if settings.BINANCE_DEMO else "testnet" if settings.BINANCE_TESTNET else "live"
    print(f"API key loaded: {'yes' if _api_key else 'NO — check .env'}")
    print(f"API secret loaded: {'yes' if _api_secret else 'NO — check .env'}")
    print(f"Demo mode: {settings.BINANCE_DEMO}  Testnet mode: {settings.BINANCE_TESTNET}")
    print("-" * 50)

    try:
        client = Client(
            api_key    = _api_key,
            api_secret = _api_secret,
            testnet    = settings.BINANCE_TESTNET,
            demo       = settings.BINANCE_DEMO,
        )

        server_time = client.get_server_time()
        print(f"✓ Connected to Binance {_mode_label}")
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
    _label = "demo" if settings.BINANCE_DEMO else "testnet" if settings.BINANCE_TESTNET else "live"
    print(f"Testing Binance {_label} connection...")
    print("-" * 50)
    ok = test_binance_connection()
    print("-" * 50)
    print("PASSED" if ok else "FAILED")
