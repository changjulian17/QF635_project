import asyncio
import json
import logging
import sqlite3
from collections import defaultdict, deque
from datetime import datetime, timedelta, timezone

import aiohttp
import numpy as np

from config import settings
from lob_engine import LocalOrderBook
from models import AggTrade, LOBLevel, LOBSnapshot, MicrostructureBar

logger = logging.getLogger(__name__)

_SNAPSHOT_URL = f"{settings.REST_BASE}/api/v3/depth"


class MicrostructureEngine:
    """
    Consumes the depth@100ms diff stream and the aggTrade stream.

    On every depth event (≈10 Hz):
      • applies the diff to the local order book
      • drains accumulated aggTrades for the window
      • computes all microstructure indicators
      • persists a MicrostructureBar to SQLite
      • appends to the in-memory metrics_store deque (newest first)

    Initialisation follows the exact Binance synchronisation procedure:
      1. Connect WS and start buffering depth events  (done by caller)
      2. Fetch REST snapshot (done here inside _initialise())
      3. Drain and filter buffered events
      4. Enter main loop with sequence validation
    """

    def __init__(
        self,
        lob: LocalOrderBook,
        trade_queue: asyncio.Queue,
        depth_queue: asyncio.Queue,
        metrics_store: deque,
        db_path: str = "cryptosentinel.db",
    ) -> None:
        self._lob = lob
        self._trade_queue = trade_queue
        self._depth_queue = depth_queue
        self._metrics_store = metrics_store
        self._db_path = db_path

        self._cvd: float = 0.0
        self._pending_trades: list[AggTrade] = []

        # Rolling volume history per price level (deque of qty values)
        self._level_hist: dict[float, deque] = defaultdict(lambda: deque(maxlen=50))

        # Iceberg: trades observed at each price in recent windows
        self._trade_hist: dict[float, deque] = defaultdict(lambda: deque(maxlen=20))

        # Liquidity flip: maximum qty ever seen at each price on each side
        self._peak_bid_qty: dict[float, float] = {}
        self._peak_ask_qty: dict[float, float] = {}

        # Break+protect: recent breakout events (timestamp, mid_price, direction)
        self._breakouts: list[tuple[datetime, float, str]] = []

        self._prev_snap: LOBSnapshot | None = None

    # ── Lifecycle ─────────────────────────────────────────────────────────

    async def run(self) -> None:
        asyncio.create_task(self._collect_trades())
        await self._initialise()
        await self._main_loop()

    async def _initialise(self) -> None:
        """Fetch REST snapshot and synchronise with buffered depth events."""
        self._lob.reset()
        logger.info("[MS] Fetching order book snapshot…")

        async with aiohttp.ClientSession() as session:
            params = {"symbol": settings.SYMBOL, "limit": 1000}
            async with session.get(_SNAPSHOT_URL, params=params) as resp:
                data = await resp.json()

        self._lob.set_snapshot(data)
        snap_id = data["lastUpdateId"]

        # Drain events buffered in the queue while we were fetching
        drained = 0
        while not self._depth_queue.empty():
            event = self._depth_queue.get_nowait()
            if int(event["u"]) > snap_id:
                self._lob.apply_diff(event)
                drained += 1

        logger.info(f"[MS] Initialised. Applied {drained} buffered diff events.")

    async def _main_loop(self) -> None:
        while True:
            event = await self._depth_queue.get()

            ok = self._lob.apply_diff(event)
            if not ok:
                logger.warning("[MS] Sequence gap — reinitialising LOB.")
                await self._initialise()
                continue

            snap = self._lob.get_snapshot(settings.LOB_DEPTH)
            if snap is None:
                continue

            trades = list(self._pending_trades)
            self._pending_trades.clear()

            bar = self._compute_bar(snap, trades)
            self._metrics_store.appendleft(bar)
            self._prev_snap = snap

            asyncio.get_event_loop().run_in_executor(None, self._write_db, bar)

    async def _collect_trades(self) -> None:
        while True:
            trade: AggTrade = await self._trade_queue.get()
            self._pending_trades.append(trade)
            # Record for iceberg detection keyed by rounded price (1 tick = $1 on BTC)
            rounded = round(trade.price, 0)
            self._trade_hist[rounded].append((trade.timestamp, trade.qty, trade.is_buyer_maker))

    # ── Bar computation ───────────────────────────────────────────────────

    def _compute_bar(self, snap: LOBSnapshot, trades: list[AggTrade]) -> MicrostructureBar:
        best_bid = snap.bids[0].price if snap.bids else 0.0
        best_ask = snap.asks[0].price if snap.asks else 0.0
        mid_price = (best_bid + best_ask) / 2.0
        spread = best_ask - best_bid

        # OBI
        bid_vol = sum(l.qty for l in snap.bids)
        ask_vol = sum(l.qty for l in snap.asks)
        denom = bid_vol + ask_vol
        obi = (bid_vol - ask_vol) / denom if denom > 0 else 0.0

        # Volume delta
        buy_vol = sum(t.qty for t in trades if not t.is_buyer_maker)
        sell_vol = sum(t.qty for t in trades if t.is_buyer_maker)
        delta = buy_vol - sell_vol
        self._cvd += delta

        # Update level history (used by reload + flip detectors)
        for lvl in snap.bids:
            self._level_hist[lvl.price].append(lvl.qty)
            self._peak_bid_qty[lvl.price] = max(self._peak_bid_qty.get(lvl.price, 0.0), lvl.qty)
        for lvl in snap.asks:
            self._level_hist[lvl.price].append(lvl.qty)
            self._peak_ask_qty[lvl.price] = max(self._peak_ask_qty.get(lvl.price, 0.0), lvl.qty)

        rb, ra = self._detect_reload(snap)
        ib, ia = self._detect_iceberg(snap, trades)
        su, sd = self._detect_sweep(snap, trades)
        fb, fa = self._detect_book_flip(snap, trades)
        ltr, lts = self._detect_liq_flip(snap)
        bpl, bps = self._detect_break_protect(snap, trades, mid_price, obi)

        return MicrostructureBar(
            timestamp=snap.timestamp,
            mid_price=mid_price,
            spread=spread,
            obi=obi,
            delta=delta,
            cvd=self._cvd,
            buy_volume=buy_vol,
            sell_volume=sell_vol,
            reload_bid=rb,
            reload_ask=ra,
            iceberg_bid=ib,
            iceberg_ask=ia,
            sweep_up=su,
            sweep_down=sd,
            book_flip_bid=fb,
            book_flip_ask=fa,
            liq_flip_to_res=ltr,
            liq_flip_to_sup=lts,
            break_protect_long=bpl,
            break_protect_short=bps,
            bid_levels=list(snap.bids),
            ask_levels=list(snap.asks),
        )

    # ── Signal detectors ──────────────────────────────────────────────────

    def _detect_reload(self, snap: LOBSnapshot) -> tuple[bool, bool]:
        """Anomalous volume spike at a level that was recently consumed."""
        if self._prev_snap is None:
            return False, False

        prev_bid = {l.price: l.qty for l in self._prev_snap.bids}
        prev_ask = {l.price: l.qty for l in self._prev_snap.asks}

        reload_bid = self._check_reload_side(snap.bids, prev_bid)
        reload_ask = self._check_reload_side(snap.asks, prev_ask)
        return reload_bid, reload_ask

    def _check_reload_side(
        self, curr_levels: list[LOBLevel], prev_map: dict[float, float]
    ) -> bool:
        for lvl in curr_levels:
            hist = self._level_hist[lvl.price]
            if len(hist) < 6:
                continue
            prev_qty = prev_map.get(lvl.price, 0.0)
            if prev_qty <= 0 or lvl.qty <= prev_qty:
                continue
            # Volume increased — check if the spike is statistically large
            vals = list(hist)[:-1]  # exclude the value we just added
            mean = float(np.mean(vals))
            std = float(np.std(vals)) + 1e-8
            if (lvl.qty - mean) / std > settings.RELOAD_SIGMA:
                return True
        return False

    def _detect_iceberg(
        self, snap: LOBSnapshot, trades: list[AggTrade]
    ) -> tuple[bool, bool]:
        """
        Resistance algorithm: a level is consumed by a trade, replenishes
        within ICEBERG_WINDOW_MS, then is hit again.
        We approximate this using 100 ms bars: if a level that had trades
        against it in this window still shows substantial volume (≥50% of
        previous), it likely replenished — iceberg.
        """
        if self._prev_snap is None or not trades:
            return False, False

        window = timedelta(milliseconds=settings.ICEBERG_WINDOW_MS)
        now = snap.timestamp

        iceberg_bid = iceberg_ask = False

        prev_bids = {l.price: l.qty for l in self._prev_snap.bids}
        prev_asks = {l.price: l.qty for l in self._prev_snap.asks}

        for lvl in snap.bids:
            prev_qty = prev_bids.get(lvl.price, 0.0)
            if prev_qty <= 0:
                continue
            # Were there sell-aggressive trades (hitting the bid) at this price?
            hit = any(
                abs(t.price - lvl.price) < 1.0 and t.is_buyer_maker
                for t in trades
            )
            if hit and lvl.qty >= prev_qty * 0.5:
                iceberg_bid = True
                break

        for lvl in snap.asks:
            prev_qty = prev_asks.get(lvl.price, 0.0)
            if prev_qty <= 0:
                continue
            hit = any(
                abs(t.price - lvl.price) < 1.0 and not t.is_buyer_maker
                for t in trades
            )
            if hit and lvl.qty >= prev_qty * 0.5:
                iceberg_ask = True
                break

        return iceberg_bid, iceberg_ask

    def _detect_sweep(
        self, snap: LOBSnapshot, trades: list[AggTrade]
    ) -> tuple[bool, bool]:
        """Aggressive volume in this bar exceeded the top-N levels of the book."""
        if not trades:
            return False, False

        buy_vol = sum(t.qty for t in trades if not t.is_buyer_maker)
        sell_vol = sum(t.qty for t in trades if t.is_buyer_maker)

        top_ask_vol = sum(l.qty for l in snap.asks[: settings.SWEEP_LEVELS]) or 1.0
        top_bid_vol = sum(l.qty for l in snap.bids[: settings.SWEEP_LEVELS]) or 1.0

        return buy_vol > top_ask_vol * 0.8, sell_vol > top_bid_vol * 0.8

    def _detect_book_flip(
        self, snap: LOBSnapshot, trades: list[AggTrade]
    ) -> tuple[bool, bool]:
        """
        Large limit order disappears without proportional trade consumption
        (cancellation / spoofing), immediately followed by opposite aggression.
        """
        if self._prev_snap is None:
            return False, False

        all_qtys = [l.qty for l in snap.bids + snap.asks]
        if not all_qtys:
            return False, False
        mean_qty = float(np.mean(all_qtys))
        std_qty = float(np.std(all_qtys)) + 1e-8

        curr_bid_prices = {l.price for l in snap.bids}
        curr_ask_prices = {l.price for l in snap.asks}

        flip_bid = flip_ask = False

        # Large bid vanished → check if cancellation (not consumed) + sell aggression
        for lvl in self._prev_snap.bids:
            if (lvl.qty - mean_qty) / std_qty < 3.0:
                continue
            if lvl.price in curr_bid_prices:
                continue
            consumed = sum(t.qty for t in trades if t.is_buyer_maker and abs(t.price - lvl.price) < 1.0)
            if consumed < lvl.qty * 0.3:
                sell_agg = sum(t.qty for t in trades if t.is_buyer_maker)
                if sell_agg > mean_qty * 0.5:
                    flip_bid = True
                    break

        # Large ask vanished → cancellation + buy aggression
        for lvl in self._prev_snap.asks:
            if (lvl.qty - mean_qty) / std_qty < 3.0:
                continue
            if lvl.price in curr_ask_prices:
                continue
            consumed = sum(t.qty for t in trades if not t.is_buyer_maker and abs(t.price - lvl.price) < 1.0)
            if consumed < lvl.qty * 0.3:
                buy_agg = sum(t.qty for t in trades if not t.is_buyer_maker)
                if buy_agg > mean_qty * 0.5:
                    flip_ask = True
                    break

        return flip_bid, flip_ask

    def _detect_liq_flip(self, snap: LOBSnapshot) -> tuple[bool, bool]:
        """
        A price that was historically a dominant bid level is now a dominant
        ask level (support → resistance) and vice versa.
        """
        if self._prev_snap is None:
            return False, False

        curr_ask_prices = {l.price for l in snap.asks}
        curr_bid_prices = {l.price for l in snap.bids}

        liq_to_res = any(
            price in curr_ask_prices and
            any(l.qty >= self._peak_bid_qty.get(price, 0) * 0.5
                for l in snap.asks if l.price == price)
            for price, peak in self._peak_bid_qty.items()
            if peak > 0 and price in curr_ask_prices
        )

        liq_to_sup = any(
            price in curr_bid_prices and
            any(l.qty >= self._peak_ask_qty.get(price, 0) * 0.5
                for l in snap.bids if l.price == price)
            for price, peak in self._peak_ask_qty.items()
            if peak > 0 and price in curr_bid_prices
        )

        return liq_to_res, liq_to_sup

    def _detect_break_protect(
        self,
        snap: LOBSnapshot,
        trades: list[AggTrade],
        mid_price: float,
        obi: float,
    ) -> tuple[bool, bool]:
        """
        Phase 1 – Break: large directional aggression moves price significantly.
        Phase 2 – Protect: within BREAK_PROTECT_WINDOW_MS, OBI flips decisively
                  in the direction of the move (institutional bids defending new
                  support, or asks defending new resistance).
        """
        if self._prev_snap is None or not (snap.bids and snap.asks):
            return False, False

        now = snap.timestamp
        window = timedelta(milliseconds=settings.BREAK_PROTECT_WINDOW_MS)

        # Evict stale breakouts
        self._breakouts = [(t, p, d) for t, p, d in self._breakouts if now - t < window]

        if trades:
            buy_vol = sum(t.qty for t in trades if not t.is_buyer_maker)
            sell_vol = sum(t.qty for t in trades if t.is_buyer_maker)
            prev_mid = (
                (self._prev_snap.bids[0].price + self._prev_snap.asks[0].price) / 2.0
                if self._prev_snap.bids and self._prev_snap.asks
                else mid_price
            )
            price_move = mid_price - prev_mid

            if buy_vol > sell_vol * 2.0 and price_move > 0:
                self._breakouts.append((now, mid_price, "up"))
            elif sell_vol > buy_vol * 2.0 and price_move < 0:
                self._breakouts.append((now, mid_price, "down"))

        bp_long = any(d == "up" and obi > settings.OBI_BREAK_THRESH for _, _, d in self._breakouts)
        bp_short = any(d == "down" and obi < -settings.OBI_BREAK_THRESH for _, _, d in self._breakouts)

        return bp_long, bp_short

    # ── Persistence ───────────────────────────────────────────────────────

    def _write_db(self, bar: MicrostructureBar) -> None:
        conn = sqlite3.connect(self._db_path)
        try:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("""
                CREATE TABLE IF NOT EXISTS microstructure_bars (
                    ts TEXT, mid_price REAL, spread REAL, obi REAL,
                    delta REAL, cvd REAL, buy_volume REAL, sell_volume REAL,
                    reload_bid INTEGER, reload_ask INTEGER,
                    iceberg_bid INTEGER, iceberg_ask INTEGER,
                    sweep_up INTEGER, sweep_down INTEGER,
                    book_flip_bid INTEGER, book_flip_ask INTEGER,
                    liq_flip_to_res INTEGER, liq_flip_to_sup INTEGER,
                    break_protect_long INTEGER, break_protect_short INTEGER,
                    bid_levels TEXT, ask_levels TEXT
                )
            """)
            conn.execute("""
                INSERT INTO microstructure_bars VALUES
                (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, (
                bar.timestamp.isoformat(),
                bar.mid_price, bar.spread, bar.obi,
                bar.delta, bar.cvd, bar.buy_volume, bar.sell_volume,
                int(bar.reload_bid), int(bar.reload_ask),
                int(bar.iceberg_bid), int(bar.iceberg_ask),
                int(bar.sweep_up), int(bar.sweep_down),
                int(bar.book_flip_bid), int(bar.book_flip_ask),
                int(bar.liq_flip_to_res), int(bar.liq_flip_to_sup),
                int(bar.break_protect_long), int(bar.break_protect_short),
                json.dumps([[l.price, l.qty] for l in bar.bid_levels]),
                json.dumps([[l.price, l.qty] for l in bar.ask_levels]),
            ))
            conn.commit()
            # Rolling retention: keep only the most recent LOB_HISTORY rows
            conn.execute("""
                DELETE FROM microstructure_bars
                WHERE rowid NOT IN (
                    SELECT rowid FROM microstructure_bars
                    ORDER BY ts DESC LIMIT ?
                )
            """, (settings.LOB_HISTORY,))
            conn.commit()
        except Exception as exc:
            logger.error(f"[MS] DB write error: {exc}")
        finally:
            conn.close()
