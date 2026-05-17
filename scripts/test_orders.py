#!/usr/bin/env python3
import sys, pathlib; sys.path.insert(0, str(pathlib.Path(__file__).parent.parent))
"""
End-to-end IOC aggressive limit order test against Binance Spot Testnet.

Places a small IOC LIMIT BUY (price = best_ask + 0.5×spread) then, if filled,
closes with an IOC LIMIT SELL (price = best_bid - 0.5×spread).
Prints fill status, slippage bps, and round-trip PnL.
"""

import asyncio
from binance import AsyncClient
from config import settings

TRADE_QTY = 0.001  # BTC — small enough to always be affordable on testnet


async def run() -> None:
    client = await AsyncClient.create(
        api_key    = settings.BINANCE_API_KEY,
        api_secret = settings.BINANCE_API_SECRET,
        testnet    = settings.BINANCE_TESTNET,
    )

    try:
        # ── Balances before ──────────────────────────────────────────────────
        print("=== Balances BEFORE ===")
        await print_balances(client)

        # ── Order book snapshot ──────────────────────────────────────────────
        book     = await client.get_order_book(symbol=settings.SYMBOL, limit=5)
        best_bid = float(book["bids"][0][0])
        best_ask = float(book["asks"][0][0])
        spread   = best_ask - best_bid
        mid      = (best_bid + best_ask) / 2.0

        print(f"\nOrder book: bid={best_bid:,.2f}  ask={best_ask:,.2f}  "
              f"spread={spread:.2f}  mid={mid:,.2f}")
        print(f"Order size: {TRADE_QTY} BTC ≈ ${mid * TRADE_QTY:,.2f} USDT\n")

        # ── IOC LIMIT BUY ────────────────────────────────────────────────────
        buy_limit = round(best_ask + spread * 0.5, 2)
        print(f">>> Placing IOC LIMIT BUY @ {buy_limit:,.2f} …")
        buy = await client.create_order(
            symbol      = settings.SYMBOL,
            side        = "BUY",
            type        = "LIMIT",
            timeInForce = "IOC",
            quantity    = TRADE_QTY,
            price       = str(buy_limit),
        )
        print(f"    orderId  : {buy['orderId']}")
        print(f"    status   : {buy['status']}")
        print(f"    execQty  : {buy['executedQty']}")

        buy_filled = float(buy.get("executedQty", "0")) > 0
        if not buy_filled:
            print("    ✗ IOC expired unfilled — no position opened, test done.\n")
            return

        buy_avg_px = _weighted_avg(buy)
        buy_slip   = (buy_avg_px - best_ask) / best_ask * 10_000
        print(f"    avg_fill : {buy_avg_px:,.2f}  slippage={buy_slip:+.2f}bps\n")

        print("=== Balances AFTER BUY ===")
        await print_balances(client)

        # ── Refresh book for exit ────────────────────────────────────────────
        book2     = await client.get_order_book(symbol=settings.SYMBOL, limit=5)
        best_bid2 = float(book2["bids"][0][0])
        best_ask2 = float(book2["asks"][0][0])
        spread2   = best_ask2 - best_bid2

        # ── IOC LIMIT SELL ───────────────────────────────────────────────────
        sell_limit = round(best_bid2 - spread2 * 0.5, 2)
        print(f"\n>>> Placing IOC LIMIT SELL @ {sell_limit:,.2f} …")
        sell = await client.create_order(
            symbol      = settings.SYMBOL,
            side        = "SELL",
            type        = "LIMIT",
            timeInForce = "IOC",
            quantity    = float(buy["executedQty"]),
            price       = str(sell_limit),
        )
        print(f"    orderId  : {sell['orderId']}")
        print(f"    status   : {sell['status']}")
        print(f"    execQty  : {sell['executedQty']}")

        sell_filled = float(sell.get("executedQty", "0")) > 0
        if not sell_filled:
            print("    ✗ Close IOC expired unfilled — position may still be open!\n")
        else:
            sell_avg_px = _weighted_avg(sell)
            sell_slip   = (best_bid2 - sell_avg_px) / best_bid2 * 10_000
            print(f"    avg_fill : {sell_avg_px:,.2f}  slippage={sell_slip:+.2f}bps\n")

            print("=== Balances AFTER SELL ===")
            await print_balances(client)

            # ── Round-trip summary ───────────────────────────────────────────
            fill_qty   = float(buy["executedQty"])
            buy_cost   = buy_avg_px  * fill_qty
            sell_proc  = sell_avg_px * float(sell["executedQty"])
            net_pnl    = sell_proc - buy_cost
            rt_slip    = buy_slip + sell_slip

            print(f"\n{'='*44}")
            print(f"  Buy  fill  : ${buy_avg_px:,.4f}  qty={fill_qty}")
            print(f"  Sell fill  : ${sell_avg_px:,.4f}  qty={sell['executedQty']}")
            print(f"  Buy  slip  : {buy_slip:+.2f} bps")
            print(f"  Sell slip  : {sell_slip:+.2f} bps")
            print(f"  Round-trip : {rt_slip:+.2f} bps total slippage")
            print(f"  Net PnL    : ${net_pnl:,.4f} USDT")
            print(f"{'='*44}")
            print("\n✓ IOC limit round-trip PASSED")

    except Exception as exc:
        print(f"\n✗ Test failed: {exc}")
        raise
    finally:
        await client.close_connection()


def _weighted_avg(order: dict) -> float:
    fills = order.get("fills", [])
    if not fills:
        return float(order.get("price", 0.0))
    total_qty = sum(float(f["qty"]) for f in fills)
    return sum(float(f["price"]) * float(f["qty"]) for f in fills) / total_qty


async def print_balances(client: AsyncClient) -> None:
    account = await client.get_account()
    for b in account["balances"]:
        if b["asset"] in {"BTC", "USDT", "BNB"}:
            free, locked = float(b["free"]), float(b["locked"])
            print(f"    {b['asset']:>4}: free={free:.6f}  locked={locked:.6f}")


if __name__ == "__main__":
    asyncio.run(run())
