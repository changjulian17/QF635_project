"""
One-time backfill script: fetches missing aggTrade history from Binance USDM Futures
REST API and inserts it into data/lob_tick.db.

Usage:
    python scripts/backfill_agg_trades.py [--hours N]

Options:
    --hours N   Only backfill the last N hours instead of the full gap (faster).
                Example: --hours 2 seeds a stable _BASELINE_BIN_QTYS threshold.

Rate limit: GET /fapi/v1/aggTrades has weight 20; limit is 2400/min → max 120 req/min.
A 0.5s sleep between pages stays well within that ceiling.

Pagination uses fromId (not startTime) after the first page, so there is no
duplicate-or-gap risk from multiple trades sharing the same millisecond timestamp.
Re-runs are safe: a UNIQUE index on (ts_event, price, qty, is_buyer_maker) means
duplicate rows are silently skipped via INSERT OR IGNORE.
"""

import argparse
import sqlite3
import time
from datetime import datetime, timezone

import requests

API_URL   = "https://fapi.binance.com/fapi/v1/aggTrades"
SYMBOL    = "BTCUSDT"
DB_PATH   = "data/lob_tick.db"
PAGE_SIZE = 1000
SLEEP_S   = 0.5   # 2 req/s → 2400 weight/min at weight 20 — safe


def _ensure_unique_index(conn: sqlite3.Connection) -> None:
    conn.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_trades_unique "
        "ON agg_trades(ts_event, price, qty, is_buyer_maker)"
    )
    conn.commit()


def _get_gap_start(conn: sqlite3.Connection, hours: int | None) -> int:
    row = conn.execute("SELECT MAX(ts_event) FROM agg_trades").fetchone()
    max_ts = row[0] if row and row[0] else 0
    if hours:
        cutoff = int((time.time() - hours * 3600) * 1000)
        return max(max_ts, cutoff)
    return max_ts


def _fetch_page(params: dict) -> list[dict]:
    resp = requests.get(API_URL, params=params, timeout=10)
    resp.raise_for_status()
    return resp.json()


def _insert_page(conn: sqlite3.Connection, trades: list[dict]) -> int:
    rows = [
        (int(t["T"]), float(t["p"]), float(t["q"]), 1 if t["m"] else 0)
        for t in trades
    ]
    conn.executemany(
        "INSERT OR IGNORE INTO agg_trades (ts_event, price, qty, is_buyer_maker) VALUES (?,?,?,?)",
        rows,
    )
    conn.commit()
    return len(rows)


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill agg_trades from Binance USDM futures")
    parser.add_argument("--hours", type=int, default=None,
                        help="Only backfill last N hours (omit for full gap)")
    args = parser.parse_args()

    conn = sqlite3.connect(DB_PATH)
    _ensure_unique_index(conn)

    gap_start_ms = _get_gap_start(conn, args.hours)
    now_ms = int(time.time() * 1000)

    if gap_start_ms >= now_ms:
        print("agg_trades is already up to date.")
        conn.close()
        return

    start_dt = datetime.fromtimestamp(gap_start_ms / 1000, tz=timezone.utc)
    print(f"Backfilling from {start_dt.strftime('%Y-%m-%d %H:%M:%S UTC')} …")

    # First page: anchor by startTime to obtain initial fromId
    page = _fetch_page({"symbol": SYMBOL, "startTime": gap_start_ms, "limit": PAGE_SIZE})
    if not page:
        print("No trades found after gap start — nothing to backfill.")
        conn.close()
        return

    total = 0
    pages = 0

    while page:
        inserted = _insert_page(conn, page)
        total += inserted
        pages += 1
        if total % 10_000 < PAGE_SIZE:
            last_dt = datetime.fromtimestamp(page[-1]["T"] / 1000, tz=timezone.utc)
            print(f"  {total:>10,} rows inserted … last trade {last_dt.strftime('%H:%M:%S UTC')}")

        last_trade = page[-1]
        if last_trade["T"] >= now_ms:
            break

        next_from_id = last_trade["a"] + 1
        time.sleep(SLEEP_S)
        page = _fetch_page({"symbol": SYMBOL, "fromId": next_from_id, "limit": PAGE_SIZE})

    conn.close()
    print(f"\nDone. {total:,} rows inserted across {pages} pages.")


if __name__ == "__main__":
    main()
