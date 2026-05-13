#!/usr/bin/env python3
import sys, pathlib; sys.path.insert(0, str(pathlib.Path(__file__).parent.parent))
"""
End-to-end order execution test against Binance Spot Testnet.
Places a small market BUY then market SELL for BTCUSDT.
"""

import asyncio
from binance import AsyncClient
from config import settings

TRADE_QTY = 0.001  # BTC — small enough to always be affordable on testnet


async def run() -> None:
    client = await AsyncClient.create(
        api_key=settings.BINANCE_API_KEY,
        api_secret=settings.BINANCE_API_SECRET,
        testnet=settings.BINANCE_TESTNET,
    )

    try:
        # ── Balances before ──────────────────────────────────────────────────
        print("=== Balances BEFORE ===")
        await print_balances(client)

        # ── Current price ────────────────────────────────────────────────────
        ticker = await client.get_symbol_ticker(symbol=settings.SYMBOL)
        price = float(ticker["price"])
        print(f"\nCurrent {settings.SYMBOL} price: ${price:,.2f}")
        print(f"Order size: {TRADE_QTY} BTC ≈ ${price * TRADE_QTY:,.2f} USDT\n")

        # ── Market BUY ───────────────────────────────────────────────────────
        print(">>> Placing market BUY …")
        buy = await client.create_order(
            symbol=settings.SYMBOL,
            side="BUY",
            type="MARKET",
            quantity=TRADE_QTY,
        )
        print(f"    orderId  : {buy['orderId']}")
        print(f"    status   : {buy['status']}")
        print(f"    fills    : {_summarise_fills(buy)}\n")

        # ── Balances mid ─────────────────────────────────────────────────────
        print("=== Balances AFTER BUY ===")
        await print_balances(client)

        # ── Market SELL ──────────────────────────────────────────────────────
        print("\n>>> Placing market SELL …")
        sell = await client.create_order(
            symbol=settings.SYMBOL,
            side="SELL",
            type="MARKET",
            quantity=TRADE_QTY,
        )
        print(f"    orderId  : {sell['orderId']}")
        print(f"    status   : {sell['status']}")
        print(f"    fills    : {_summarise_fills(sell)}\n")

        # ── Balances after ───────────────────────────────────────────────────
        print("=== Balances AFTER SELL ===")
        await print_balances(client)

        # ── Round-trip PnL ───────────────────────────────────────────────────
        buy_cost  = sum(float(f["price"]) * float(f["qty"]) for f in buy["fills"])
        sell_proc = sum(float(f["price"]) * float(f["qty"]) for f in sell["fills"])
        buy_fee   = sum(float(f["commission"]) for f in buy["fills"])
        sell_fee  = sum(float(f["commission"]) for f in sell["fills"])
        net_pnl   = sell_proc - buy_cost
        print(f"\n{'='*40}")
        print(f"  Buy cost      : ${buy_cost:,.4f} USDT")
        print(f"  Sell proceeds : ${sell_proc:,.4f} USDT")
        print(f"  Fees          : ~${buy_fee + sell_fee:,.6f} (commission asset may vary)")
        print(f"  Net PnL       : ${net_pnl:,.4f} USDT")
        print(f"{'='*40}")
        print("\n✓ Order round-trip PASSED")

    except Exception as exc:
        print(f"\n✗ Test failed: {exc}")
        raise
    finally:
        await client.close_connection()


async def print_balances(client: AsyncClient) -> None:
    account = await client.get_account()
    interesting = {"BTC", "USDT", "BNB"}
    for b in account["balances"]:
        if b["asset"] in interesting:
            free, locked = float(b["free"]), float(b["locked"])
            print(f"    {b['asset']:>4}: free={free:.6f}  locked={locked:.6f}")


def _summarise_fills(order: dict) -> str:
    fills = order.get("fills", [])
    if not fills:
        return "no fill data"
    avg_px = sum(float(f["price"]) * float(f["qty"]) for f in fills) / sum(float(f["qty"]) for f in fills)
    total_qty = sum(float(f["qty"]) for f in fills)
    return f"avg_price=${avg_px:,.2f}  qty={total_qty}"


if __name__ == "__main__":
    asyncio.run(run())
