import asyncio
import logging
from collections import defaultdict, deque
from datetime import datetime, timedelta

import numpy as np

from config import settings
from core.lob_engine import LocalOrderBook
from models import AggTrade, LOBLevel, LOBSnapshot, MicrostructureBar

logger = logging.getLogger(__name__)


class MicrostructureEngine:
    """
    Consumes the depth@100ms diff stream and the aggTrade stream.

    On every depth event (≈10 Hz):
      • applies the diff to the local order book
      • drains accumulated aggTrades for the window
      • computes all microstructure indicators
      • enqueues a MicrostructureBar for DBWriter to persist
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
        ms_bar_queue: asyncio.Queue,
    ) -> None:
        self._lob = lob
        self._trade_queue = trade_queue
        self._depth_queue = depth_queue
        self._metrics_store = metrics_store
        self._ms_bar_queue = ms_bar_queue

        self._cvd: float = 0.0
        self._pending_trades: list[AggTrade] = []

        # Rolling volume history per price level (deque of qty values)
        self._level_hist: dict[float, deque] = defaultdict(lambda: deque(maxlen=50))

        # Liquidity flip: maximum qty ever seen at each price on each side
        self._peak_bid_qty: dict[float, float] = {}
        self._peak_ask_qty: dict[float, float] = {}

        # Break+protect: recent breakout events (timestamp, mid_price, direction)
        self._breakouts: list[tuple[datetime, float, str]] = []

        self._prev_snap: LOBSnapshot | None = None
        self._bar_count: int = 0

    # ── Lifecycle ─────────────────────────────────────────────────────────

    async def run(self) -> None:
        asyncio.create_task(self._collect_trades())
        try:
            await self._initialise()
        except Exception as exc:
            logger.error("[MS] Initialisation failed: %s — running in drain mode.", exc)
            await self._drain_loop()
            return
        await self._main_loop()

    async def _drain_loop(self) -> None:
        """Discard depth events when the engine cannot initialise. Prevents queue backpressure."""
        while True:
            await self._depth_queue.get()

    async def _initialise(self) -> None:
        """Wait for first valid depth20 snapshot — LOB state machine handles sync."""
        self._cvd = 0.0
        await self._lob.reset()
        logger.info("[MS] Waiting for first depth20 snapshot…")
        while True:
            event = await self._depth_queue.get()
            if await self._lob.apply_snapshot(event):
                logger.info("[MS] LOB synced. lastUpdateId=%d", event.get("lastUpdateId", 0))
                return

    async def _main_loop(self) -> None:
        while True:
            event = await self._depth_queue.get()
            await self._lob.apply_snapshot(event)

            snap = await self._lob.get_snapshot(settings.LOB_DEPTH)
            if snap is None:
                continue

            trades = list(self._pending_trades)
            self._pending_trades.clear()

            bar = self._compute_bar(snap, trades)
            self._metrics_store.appendleft(bar)
            self._prev_snap = snap

            await self._ms_bar_queue.put(bar)

    async def _collect_trades(self) -> None:
        while True:
            item = await self._trade_queue.get()
            if not isinstance(item, AggTrade):
                continue  # bookTicker dicts share this queue; spread is derived from LOB
            self._pending_trades.append(item)

    # ── Bar computation ───────────────────────────────────────────────────

    def _compute_bar(self, snap: LOBSnapshot, trades: list[AggTrade]) -> MicrostructureBar:
        best_bid = snap.bids[0].price if snap.bids else 0.0
        best_ask = snap.asks[0].price if snap.asks else 0.0
        mid_price = (best_bid + best_ask) / 2.0
        spread = best_ask - best_bid

        # OBI — computed from the tight top-N levels only, not the full stored depth
        bid_vol = sum(l.qty for l in snap.bids[: settings.LOB_OBI_DEPTH])
        ask_vol = sum(l.qty for l in snap.asks[: settings.LOB_OBI_DEPTH])
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

        self._bar_count += 1
        if self._bar_count % settings.PRICE_PRUNE_INTERVAL == 0:
            self._prune_price_dicts(mid_price)

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
        Resistance algorithm (L2 approximation):
          1. A level exceeds the minimum size floor (ICEBERG_MIN_QTY).
          2. An aggressive trade hits it within ICEBERG_PRICE_TOL.
          3. After the trade the level holds MORE qty than the natural post-fill
             remainder (prev_qty − hit_vol), confirming a hidden reserve refilled it,
             AND still meets the ICEBERG_MIN_REPLENISH size floor.

        Thresholds are intentionally strict to reduce false positives on a
        fast-moving book where normal partial fills are common.
        """
        if self._prev_snap is None or not trades:
            return False, False

        iceberg_bid = iceberg_ask = False

        prev_bids = {l.price: l.qty for l in self._prev_snap.bids}
        prev_asks = {l.price: l.qty for l in self._prev_snap.asks}

        for lvl in snap.bids:
            prev_qty = prev_bids.get(lvl.price, 0.0)
            if prev_qty < settings.ICEBERG_MIN_QTY:
                continue
            # Aggressive sell (buyer_maker=True) must have hit this exact price level
            hit_vol = sum(
                t.qty for t in trades
                if t.is_buyer_maker and abs(t.price - lvl.price) <= settings.ICEBERG_PRICE_TOL
            )
            if hit_vol <= 0:
                continue
            # Hidden reserve: level has MORE qty than natural post-fill remainder AND is still large
            recovered = lvl.qty - max(prev_qty - hit_vol, 0.0)
            if recovered > 0 and lvl.qty >= prev_qty * settings.ICEBERG_MIN_REPLENISH:
                iceberg_bid = True
                break

        for lvl in snap.asks:
            prev_qty = prev_asks.get(lvl.price, 0.0)
            if prev_qty < settings.ICEBERG_MIN_QTY:
                continue
            # Aggressive buy (buyer_maker=False) must have hit this exact price level
            hit_vol = sum(
                t.qty for t in trades
                if not t.is_buyer_maker and abs(t.price - lvl.price) <= settings.ICEBERG_PRICE_TOL
            )
            if hit_vol <= 0:
                continue
            recovered = lvl.qty - max(prev_qty - hit_vol, 0.0)
            if recovered > 0 and lvl.qty >= prev_qty * settings.ICEBERG_MIN_REPLENISH:
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

        ref_snap = self._prev_snap if self._prev_snap is not None else snap
        top_ask_vol = sum(l.qty for l in ref_snap.asks[: settings.SWEEP_LEVELS]) or 1.0
        top_bid_vol = sum(l.qty for l in ref_snap.bids[: settings.SWEEP_LEVELS]) or 1.0

        return (buy_vol > top_ask_vol * settings.SWEEP_THRESHOLD,
                sell_vol > top_bid_vol * settings.SWEEP_THRESHOLD)

    def _detect_book_flip(
        self, snap: LOBSnapshot, trades: list[AggTrade]
    ) -> tuple[bool, bool]:
        """
        Large limit order disappears without proportional trade consumption
        (cancellation / spoofing), immediately followed by opposite aggression.
        """
        if self._prev_snap is None:
            return False, False

        all_qtys = [l.qty for l in self._prev_snap.bids + self._prev_snap.asks]
        if not all_qtys:
            return False, False
        mean_qty = float(np.mean(all_qtys))
        std_qty = float(np.std(all_qtys)) + 1e-8

        curr_bid_prices = {l.price for l in snap.bids}
        curr_ask_prices = {l.price for l in snap.asks}

        flip_bid = flip_ask = False

        # Large bid vanished → check if cancellation (not consumed) + sell aggression
        for lvl in self._prev_snap.bids:
            if (lvl.qty - mean_qty) / std_qty < settings.BOOK_FLIP_SIGMA:
                continue
            if lvl.price in curr_bid_prices:
                continue
            consumed = sum(t.qty for t in trades if t.is_buyer_maker and abs(t.price - lvl.price) < 1.0)
            if consumed < lvl.qty * settings.BOOK_FLIP_MIN_CONSUMED:
                sell_agg = sum(t.qty for t in trades if t.is_buyer_maker)
                if sell_agg > mean_qty * settings.BOOK_FLIP_AGG_RATIO:
                    flip_bid = True
                    break

        # Large ask vanished → cancellation + buy aggression
        for lvl in self._prev_snap.asks:
            if (lvl.qty - mean_qty) / std_qty < settings.BOOK_FLIP_SIGMA:
                continue
            if lvl.price in curr_ask_prices:
                continue
            consumed = sum(t.qty for t in trades if not t.is_buyer_maker and abs(t.price - lvl.price) < 1.0)
            if consumed < lvl.qty * settings.BOOK_FLIP_MIN_CONSUMED:
                buy_agg = sum(t.qty for t in trades if not t.is_buyer_maker)
                if buy_agg > mean_qty * settings.BOOK_FLIP_AGG_RATIO:
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

        asks_by_price = {l.price: l.qty for l in snap.asks}
        bids_by_price = {l.price: l.qty for l in snap.bids}

        liq_to_res = any(
            asks_by_price.get(price, 0.0) >= peak * 0.5
            for price, peak in self._peak_bid_qty.items()
            if peak > 0 and price in asks_by_price
        )

        liq_to_sup = any(
            bids_by_price.get(price, 0.0) >= peak * 0.5
            for price, peak in self._peak_ask_qty.items()
            if peak > 0 and price in bids_by_price
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

            if buy_vol >= settings.BREAK_MIN_VOL and buy_vol > sell_vol * 2.0 and price_move > 0:
                self._breakouts.append((now, mid_price, "up"))
            elif sell_vol >= settings.BREAK_MIN_VOL and sell_vol > buy_vol * 2.0 and price_move < 0:
                self._breakouts.append((now, mid_price, "down"))

        bp_long = any(d == "up" and obi > settings.OBI_BREAK_THRESH for _, _, d in self._breakouts)
        bp_short = any(d == "down" and obi < -settings.OBI_BREAK_THRESH for _, _, d in self._breakouts)

        return bp_long, bp_short

    def _prune_price_dicts(self, mid_price: float) -> None:
        """Evict price keys outside ±PRICE_PRUNE_BAND of current mid to prevent unbounded growth."""
        band = settings.PRICE_PRUNE_BAND
        lo = mid_price * (1.0 - band)
        hi = mid_price * (1.0 + band)
        for d in (self._level_hist, self._peak_bid_qty, self._peak_ask_qty):
            for price in [p for p in d if not (lo <= p <= hi)]:
                del d[price]
