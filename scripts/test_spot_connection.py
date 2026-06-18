#!/usr/bin/env python3
"""Connectivity check for Binance Demo Futures using DEMO_BINANCE_API_KEY and DEMO_BINANCE_API_SECRET."""

import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).parent.parent))

from binance.client import Client

from config import settings


def test_binance_demo_futures_connection() -> bool:
    """Verify the demo futures key can authenticate and access basic account data."""
    api_key = settings.DEMO_BINANCE_API_KEY
    api_secret = settings.DEMO_BINANCE_API_SECRET

    print(f"API key loaded: {'yes' if api_key else 'NO — check .env'}")
    print(f"API secret loaded: {'yes' if api_secret else 'NO — check .env'}")
    print("Binance mode: Demo Futures (forced)")
    print("-" * 50)

    if not api_key or not api_secret:
        print("✗ Connection failed: missing DEMO_BINANCE_API_KEY or DEMO_BINANCE_API_SECRET in .env")
        return False

    try:
        client = Client(
            api_key=api_key,
            api_secret=api_secret,
            testnet=False,
            demo=True,
        )

        server_time = client.get_server_time()
        print("✓ Connected to Binance Demo Futures")
        print(f"✓ Server time: {server_time}")

        account = client.get_account()
        balances = [
            b for b in account["balances"]
            if float(b["free"]) > 0 or float(b["locked"]) > 0
        ]
        print(f"✓ Account authenticated — {len(balances)} non-zero balance(s):")
        for balance in balances:
            print(
                f"    {balance['asset']:>6}: free={balance['free']}, locked={balance['locked']}"
            )

        ticker = client.get_symbol_ticker(symbol=settings.SYMBOL)
        print(f"✓ {settings.SYMBOL} last price: {ticker['price']}")

        return True
    except Exception as exc:
        print(f"✗ Connection failed: {exc}")
        print("  Hint: this usually means the key/secret pair is not a Demo Futures key,")
        print("  the key has the wrong permissions, or the account is IP-restricted.")
        return False


if __name__ == "__main__":
    print("Testing Binance Demo Futures connection...")
    print("-" * 50)
    ok = test_binance_demo_futures_connection()
    print("-" * 50)
    print("PASSED" if ok else "FAILED")