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
