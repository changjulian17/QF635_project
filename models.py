from dataclasses import dataclass, field
from enum import Enum, auto
from datetime import datetime, timezone

class PatternType(Enum):
    RISING_WEDGE = auto()
    FALLING_WEDGE = auto()
    SYMMETRICAL_TRIANGLE = auto()
    SUPPORT_BREAKOUT = auto()
    RESISTANCE_BREAKOUT = auto()
    TRENDLINE_BOUNCE = auto()

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
class PatternSignal:
    pattern: PatternType
    direction: Direction
    confidence: float
    entry_price: float
    stop_loss: float
    take_profit: float
    detected_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    r2: float = 0.0
    volume_ratio: float = 1.0

@dataclass
class OrderRequest:
    signal: PatternSignal
    quantity: float
    approved: bool = False
    rejection_reason: str = ""

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

    @property
    def drawdown_pct(self) -> float:
        if self.peak_equity == 0:
            return 0.0
        return (self.peak_equity - self.equity) / self.peak_equity

    @property
    def daily_loss_pct(self) -> float:
        return abs(min(0, self.daily_pnl)) / self.starting_equity


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
