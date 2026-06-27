#!/usr/bin/env python3
"""
Demonstration test fire for Binance Futures demo account.

Runs a deterministic MARKET BUY -> MARKET SELL round-trip on a fixed schedule
(first fire within the first minute of launch, then every TEST_FIRE_INTERVAL_S),
so a live demo on demo.binance.com always shows recurring order activity. Unlike
the engine's signal injector, this has no warmup / OBI / gate / exit dependencies.

Launched by start_demo.sh in single-actor mode (the engine runs with
TEST_SIGNAL_INJECT=false), so the account net position is solely this script's.
The engine's orphan watchdog can still rarely close the long before this script's
own SELL; the position-aware close handles that race and never opens a short.

Config via env (with defaults):
    TEST_FIRE_INITIAL_DELAY_S   seconds before the first fire   (default 10)
    TEST_FIRE_INTERVAL_S        seconds between fire starts      (default 120)
    TEST_FIRE_NOTIONAL          target USD notional per BUY      (default 100)

Run with:
    python scripts/demo_test_fire.py
"""
import asyncio
import logging
import math
import os
import pathlib
import sys
import time

sys.path.insert(0, str(pathlib.Path(__file__).parent.parent))

SYMBOL   = "BTCUSDT"
FUT_STEP = 0.001

logger = logging.getLogger("demo_test_fire")


def close_sell_qty(bought_qty: float, position_long_qty: float, step: float = FUT_STEP) -> float:
    """Qty to SELL to flatten only the test-fire's own long. 0.0 if not long.

    Caps the SELL at what this fire bought (so it never over-closes another
    actor's long) and returns 0.0 when the live position is flat or short (so a
    plain MARKET SELL can never open an unintended short after a watchdog race).
    """
    if position_long_qty <= 0.0:
        return 0.0
    return round(math.floor(min(bought_qty, position_long_qty) / step) * step, 3)


async def _current_long_qty(client, hedge_mode: bool) -> float:
    """Live long-position size for SYMBOL via futures_account(). 0.0 on error/flat.

    One-way mode reports a single BOTH entry with signed positionAmt; hedge mode
    reports separate LONG/SHORT entries. Mirrors order_manager.get_exchange_position.
    """
    try:
        account = await client.futures_account()
    except Exception as exc:
        logger.warning("futures_account() failed: %s — treating position as flat", exc)
        return 0.0
    for pos in account.get("positions", []):
        if pos.get("symbol") != SYMBOL:
            continue
        if hedge_mode and pos.get("positionSide") != "LONG":
            continue
        return float(pos.get("positionAmt", 0.0))
    return 0.0


async def _fire(client, hedge_mode: bool, notional: float) -> None:
    """One BUY -> SELL round-trip. Position-aware close; never opens a short."""
    book     = await client.futures_order_book(symbol=SYMBOL, limit=5)
    best_ask = float(book["asks"][0][0])
    qty      = round(math.ceil(notional / best_ask / FUT_STEP) * FUT_STEP, 3)

    buy_params: dict = {"symbol": SYMBOL, "side": "BUY", "type": "MARKET", "quantity": qty}
    if hedge_mode:
        buy_params["positionSide"] = "LONG"
    resp     = await client.futures_create_order(**buy_params)
    exec_qty = float(resp.get("executedQty", "0"))

    # Demo MARKET orders often return executedQty=0 immediately — poll once.
    if exec_qty == 0.0:
        await asyncio.sleep(2.0)
        polled   = await client.futures_get_order(symbol=SYMBOL, orderId=resp.get("orderId"))
        exec_qty = float(polled.get("executedQty", "0"))
    if exec_qty == 0.0:
        logger.warning("BUY not filled (qty=%.3f @ ~%.2f) — skipping SELL", qty, best_ask)
        return
    logger.info("BUY filled: %.3f @ ~%.2f", exec_qty, best_ask)

    # Let the position settle on demo, then close only what we actually hold long.
    await asyncio.sleep(2.0)
    long_qty = await _current_long_qty(client, hedge_mode)
    sell_qty = close_sell_qty(exec_qty, long_qty, FUT_STEP)
    if sell_qty <= 0.0:
        logger.info("position already flat (watchdog raced) — skipping SELL")
        return

    sell_params: dict = {"symbol": SYMBOL, "side": "SELL", "type": "MARKET", "quantity": sell_qty}
    if hedge_mode:
        sell_params["positionSide"] = "LONG"
    close_resp = await client.futures_create_order(**sell_params)
    logger.info("SELL closed: %.3f (status=%s)", sell_qty, close_resp.get("status", "?"))


async def run() -> None:
    # Demo endpoints — set before importing config so its validator sees BINANCE_DEMO.
    os.environ["BINANCE_DEMO"]    = "true"
    os.environ["BINANCE_TESTNET"] = "false"

    from binance import AsyncClient

    from config import settings

    initial_delay = float(os.environ.get("TEST_FIRE_INITIAL_DELAY_S", "10"))
    interval      = float(os.environ.get("TEST_FIRE_INTERVAL_S", "120"))
    notional      = float(os.environ.get("TEST_FIRE_NOTIONAL", "100"))

    if not settings.DEMO_BINANCE_API_KEY:
        logger.error("DEMO_BINANCE_API_KEY not set in .env — aborting test fire")
        return

    client = await AsyncClient.create(
        api_key    = settings.DEMO_BINANCE_API_KEY,
        api_secret = settings.DEMO_BINANCE_API_SECRET,
        testnet    = False,
        demo       = True,
    )
    try:
        pos_mode   = await client.futures_get_position_mode()
        hedge_mode = pos_mode.get("dualSidePosition", False)
        logger.info(
            "Test fire armed — %s mode, first fire in %.0fs then every %.0fs (~$%.0f/fire)",
            "HEDGE" if hedge_mode else "ONE-WAY", initial_delay, interval, notional,
        )

        await asyncio.sleep(initial_delay)
        while True:
            t0 = time.monotonic()
            try:
                await _fire(client, hedge_mode, notional)
            except Exception as exc:
                logger.error("Test fire round-trip failed: %s", exc, exc_info=True)
            await asyncio.sleep(max(0.0, interval - (time.monotonic() - t0)))
    finally:
        await client.close_connection()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%H:%M:%S",
    )
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        pass
