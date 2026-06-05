import asyncio
from dataclasses import dataclass, field
from enum import Enum
from datetime import datetime, timezone

class Direction(Enum):
    LONG = "BUY"
    SHORT = "SELL"

class CircuitBreakerStatus(Enum):
    ACTIVE = "ACTIVE"
    PAUSED = "PAUSED"
    HALTED = "HALTED"

@dataclass
class Candle:
    open_time: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float
    is_closed: bool = False

@dataclass
class Position:
    symbol: str
    side: Direction
    entry_price: float
    quantity: float
    stop_loss: float
    take_profit: float
    opened_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    unrealised_pnl: float = 0.0

@dataclass
class PortfolioState:
    equity: float
    starting_equity: float
    peak_equity: float
    positions: list[Position] = field(default_factory=list)
    daily_pnl: float = 0.0
    consecutive_losses: int = 0
    circuit_breaker: CircuitBreakerStatus = CircuitBreakerStatus.ACTIVE
    num_trades: int = 0
    num_wins: int = 0
    num_fill_samples: int = 0
    avg_slippage_bps: float = 0.0
    budget_loss_pct: float = 0.0
    usdt_balance: float = 0.0   # USDT cash at last reconcile
    btc_balance:  float = 0.0   # BTC quantity at last reconcile
    btc_price:    float = 0.0   # BTC/USDT price used for MTM at last reconcile

    @property
    def drawdown_pct(self) -> float:
        if self.peak_equity == 0:
            return 0.0
        return (self.peak_equity - self.equity) / self.peak_equity

    @property
    def daily_loss_pct(self) -> float:
        return abs(min(0, self.daily_pnl)) / self.starting_equity if self.starting_equity > 0 else 0.0


# ── Microstructure models ──────────────────────────────────────────────────

@dataclass
class AggTrade:
    timestamp: datetime
    price: float
    qty: float
    is_buyer_maker: bool  # True → seller aggressed (hit bid); False → buyer aggressed (lifted ask)

@dataclass
class LOBLevel:
    price: float
    qty: float

@dataclass
class LOBSnapshot:
    timestamp: datetime
    bids: list[LOBLevel]   # sorted descending by price
    asks: list[LOBLevel]   # sorted ascending by price
    last_update_id: int

@dataclass
class MicrostructureBar:
    timestamp: datetime
    mid_price: float
    spread: float
    obi: float              # Order Book Imbalance ∈ [-1, 1]
    delta: float            # net aggression this bar (buy vol - sell vol)
    cvd: float              # Cumulative Volume Delta (running sum)
    buy_volume: float
    sell_volume: float
    # Detected signals
    reload_bid: bool
    reload_ask: bool
    iceberg_bid: bool
    iceberg_ask: bool
    sweep_up: bool
    sweep_down: bool
    book_flip_bid: bool     # spoofing: large bid cancelled + sell aggression
    book_flip_ask: bool     # spoofing: large ask cancelled + buy aggression
    liq_flip_to_res: bool   # former support → now resistance
    liq_flip_to_sup: bool   # former resistance → now support
    break_protect_long: bool
    break_protect_short: bool
    # Raw book for heatmap rendering
    bid_levels: list[LOBLevel] = field(default_factory=list)
    ask_levels: list[LOBLevel] = field(default_factory=list)


# ── v3.0 models ───────────────────────────────────────────────────────────────

class LOBStateMachineState(Enum):
    UNINITIALISED = "UNINITIALISED"
    SYNCED        = "SYNCED"
    GAP_DETECTED  = "GAP_DETECTED"
    DISCONNECTED  = "DISCONNECTED"


@dataclass
class WallState:
    price:           float
    qty_initial:     float
    qty_current:     float
    first_seen_ts:   int       # ms epoch
    last_seen_ts:    int
    side:            str       # "bid" or "ask"
    sigma:           float     # how many σ above surrounding median
    @property
    def reload_ratio(self) -> float:
        return self.qty_current / self.qty_initial if self.qty_initial > 0 else 0.0

    @property
    def persistence_ms(self) -> int:
        return max(0, self.last_seen_ts - self.first_seen_ts)

    @property
    def is_persistent(self) -> bool:
        return self.persistence_ms >= 500


@dataclass
class FeatureVector:
    """All 15 features computed by FeatureComputer. lob_status gates entry at Gate 0."""
    lob_status:              str   = "SYNCED"   # "SYNCED" | "STALE" | "DISCONNECTED"
    price_vs_vwap:           float = 0.0
    obi_zscore:              float = 0.0
    cvd_delta:               float = 0.0
    vol_ratio:               float = 1.0
    atr_percentile:          float = 0.5
    rsi_value:               float = 50.0
    spread_bps:              float = 0.0
    pattern_r2:              float = 0.0
    vwap_reclaim:            int   = 0
    vol_climax:              int   = 0
    cvd_positive:            int   = 0
    wall_detected:           int   = 0
    wall_distance_bps:       float = 0.0
    absorption_ratio:        float = 0.0
    protection_wall_present: int   = 0

    def to_ml_array(self) -> list[float]:
        # pattern_r2 / protection_wall_present excluded — no live source / constant
        # at scoring time. Must stay aligned with scorer.FEATURE_ORDER.
        return [
            self.price_vs_vwap, self.obi_zscore, self.cvd_delta,
            self.vol_ratio, self.atr_percentile, self.rsi_value,
            self.spread_bps,
            float(self.vwap_reclaim), float(self.vol_climax),
            float(self.cvd_positive), float(self.wall_detected),
            self.wall_distance_bps, self.absorption_ratio,
        ]


@dataclass
class MicroSignal:
    """Emitted by MicrostructureDetector when a Sweep + Fresh Wall is confirmed."""
    signal_type:       str             # "SWEEP_WITH_PROTECTION"
    direction:         str             # "LONG" | "SHORT"
    timestamp_ms:      int
    consumed_wall:     WallState | None = None
    protection_wall:   WallState | None = None
    prior_absorption:  bool  = False   # Wall absorbed aggression before sweep
    cvd_std:           float = 0.0     # CVD spike in std multiples
    price_move_pct:    float = 0.0
    confidence:        float = 0.0     # filled by StrategyExecutor after scoring
    mid_price:         float = 0.0     # BTC/USDT mid at signal fire time; set by detector


@dataclass
class MicroOrderRequest:
    """Executable output of StrategyExecutor; sent to the micro-execution layer."""
    micro_signal:   "MicroSignal"
    signal_id:      str          # links back to SignalRecord for telemetry outcome updates
    order_type:     str          # "IOC_LIMIT"
    side:           str          # "BUY" | "SELL"
    limit_price:    float | None  # None = taker (execution layer resolves via LOB); float = explicit IOC limit
    ioc_timeout_ms: int
    confidence:     float
    notional_hint:  float        # risk fraction of equity = RISK_PCT × KELLY × confidence;
                                 # execution layer: qty = (equity × notional_hint) / abs(entry − protection_wall)
    fill_event:            asyncio.Event = field(default_factory=asyncio.Event)
                                         # execution layer calls .set() on confirmed fill
    position_closed_event: asyncio.Event = field(default_factory=asyncio.Event)
                                         # execution layer calls .set() when position exits (TP/SL/manual);
                                         # Gate 6 monitor exits cleanly without firing the wall-removed alert


@dataclass
class FillDetail:
    """Emitted by OrderManager to fill_queue on a confirmed IOC fill."""
    signal_id:    str
    side:         str    # "BUY" | "SELL"
    fill_price:   float
    qty:          float
    limit_price:  float
    slippage_bps: float
    order_id:     str    # Binance orderId or "DRY_RUN"


@dataclass
class KillswitchState:
    fired:              bool  = False
    trigger:            str   = ""
    fired_at:           str   = ""
    total_loss_at_fire: float = 0.0
    latency_at_fire:    float = 0.0
    slippage_at_fire:   float = 0.0


@dataclass
class SharedState:
    """Shared mutable state between ws_consumer, LOB engine, and risk components."""
    heartbeat_status: str   = "HEALTHY"   # "HEALTHY" | "DEGRADED" | "CRITICAL"
    last_delta_ms:    float = 0.0
    lob_status:       str   = "UNINITIALISED"  # mirrors LOBStateMachineState.value
