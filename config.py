from pydantic_settings import BaseSettings
from pydantic import ConfigDict, model_validator


class Settings(BaseSettings):
    model_config = ConfigDict(env_file=".env", env_file_encoding="utf-8")

    # Binance Testnet
    BINANCE_API_KEY: str = ""
    BINANCE_API_SECRET: str = ""
    BINANCE_TESTNET: bool = True
    WS_BASE: str = "wss://stream.testnet.binance.vision"
    REST_BASE: str = "https://testnet.binance.vision"
    LOB_RECORDER_WS: str = "wss://stream.binance.com:9443"  # real Binance public stream (Rule 4)

    # Strategy
    SYMBOL: str = "BTCUSDT"
    CANDLE_INTERVAL: str = "1s"   # legacy — ws_consumer uses this
    TIMEFRAME: str = "5m"          # kline interval for pattern detection (master arch §4)
    PATTERN_LOOKBACK: int = 50
    SWING_WINDOW: int = 5
    BREAKOUT_VOL_MULT: float = 1.5
    MIN_R2: float = 0.80
    MIN_CONFIDENCE: float = 0.58   # Gate 2 confidence threshold

    # Risk
    STARTING_EQUITY: float = 10_000.0
    MAX_DRAWDOWN_PCT: float = 0.05
    DAILY_LOSS_LIMIT_PCT: float = 0.02
    # 5-tier DOV loss thresholds (§5 risk_management_plan)
    TIER_REDUCED_PCT: float = 0.0050   # ≥ 0.50% DOV loss → REDUCED
    TIER_MINIMAL_PCT: float = 0.0075   # ≥ 0.75% DOV loss → MINIMAL
    TIER_PASSIVE_PCT: float = 0.0090   # ≥ 0.90% DOV loss → PASSIVE (no new entries)
    TIER_HALTED_PCT:  float = 0.0100   # ≥ 1.00% DOV loss → HALTED
    MAX_CONSECUTIVE_LOSSES: int = 3
    RISK_PER_TRADE_PCT: float = 0.01
    KELLY_FRACTION: float = 0.25
    ATR_MULTIPLIER_SL: float = 1.5   # legacy — used by pattern_detector.py only; microstructure path uses wall-based SL
    ATR_MULTIPLIER_TP: float = 3.0
    PYRAMID_MAX_LEGS: int = 3

    # Execution
    DRY_RUN: bool = True
    IOC_TIMEOUT_MS: int = 200          # IOC order max age before cancel-no-retry
    SLIPPAGE_RESEARCH_BPS: float = 3.0  # expected slippage assumption (KS-3 baseline)
    SLIPPAGE_MULTIPLIER: float = 1.5    # KS-3 fires when rolling avg > research × multiplier

    # UI
    UI_REFRESH_INTERVAL: float = 1.0

    # Microstructure / LOB
    LOB_DEPTH: int = 1000
    LOB_OBI_DEPTH: int = 20
    LOB_HISTORY: int = 18000
    LOB_HEATMAP_BUCKET: float = 5.0
    LOB_WALL_SIGMA: float = 2.5        # σ threshold for Wall identification (§5)
    LOB_WALL_WINDOW: int = 5           # ticks each side for Wall median/std
    RELOAD_SIGMA: float = 3.0
    ICEBERG_WINDOW_MS: int = 500
    ICEBERG_MIN_REPLENISH: float = 0.80
    ICEBERG_MIN_QTY: float = 0.5
    ICEBERG_PRICE_TOL: float = 0.10
    SWEEP_LEVELS: int = 5
    BREAK_PROTECT_WINDOW_MS: int = 2000
    OBI_BREAK_THRESH: float = 0.40
    PRICE_PRUNE_INTERVAL: int = 100   # prune stale price keys every N bars (~10 s at 10 Hz)
    PRICE_PRUNE_BAND: float = 0.02    # keep prices within ±2% of current mid
    SWEEP_THRESHOLD: float = 0.80        # buy/sell vol must exceed this fraction of top-N book depth
    BOOK_FLIP_SIGMA: float = 3.0         # min z-score for a level to qualify as "large" in book-flip
    BOOK_FLIP_MIN_CONSUMED: float = 0.30 # max fraction consumed before cancellation is inferred
    BOOK_FLIP_AGG_RATIO: float = 0.50    # min aggression vol (as fraction of mean_qty) to confirm flip
    BREAK_MIN_VOL: float = 1.0           # min absolute buy/sell volume to register a breakout

    # Heartbeat monitor (§4)
    HEARTBEAT_WARN_MS: int = 200
    HEARTBEAT_CRITICAL_MS: int = 500
    HEARTBEAT_CONSEC_LIMIT: int = 3

    # Persistence
    REGISTRY_DB: str = "strategies/registry.db"
    LOB_TICK_DB: str = "data/lob_tick.db"
    BACKTEST_RESULTS_DB: str = "data/backtest_results.db"

    # Dashboard
    DASHBOARD_API_PORT: int = 8080

    @model_validator(mode="after")
    def _validate_tier_ordering(self) -> "Settings":
        if not self.DRY_RUN and (not self.BINANCE_API_KEY or not self.BINANCE_API_SECRET):
            raise ValueError(
                "BINANCE_API_KEY and BINANCE_API_SECRET must be set when DRY_RUN=False. "
                "Add them to .env or set DRY_RUN=True for paper trading."
            )
        assert self.TIER_REDUCED_PCT < self.TIER_MINIMAL_PCT, \
            f"TIER_REDUCED_PCT ({self.TIER_REDUCED_PCT}) must be < TIER_MINIMAL_PCT ({self.TIER_MINIMAL_PCT})"
        assert self.TIER_MINIMAL_PCT < self.TIER_PASSIVE_PCT, \
            f"TIER_MINIMAL_PCT ({self.TIER_MINIMAL_PCT}) must be < TIER_PASSIVE_PCT ({self.TIER_PASSIVE_PCT})"
        assert self.TIER_PASSIVE_PCT < self.TIER_HALTED_PCT, \
            f"TIER_PASSIVE_PCT ({self.TIER_PASSIVE_PCT}) must be < TIER_HALTED_PCT ({self.TIER_HALTED_PCT})"
        assert self.TIER_HALTED_PCT <= self.DAILY_LOSS_LIMIT_PCT, \
            f"TIER_HALTED_PCT ({self.TIER_HALTED_PCT}) must be <= DAILY_LOSS_LIMIT_PCT ({self.DAILY_LOSS_LIMIT_PCT})"
        assert self.MAX_DRAWDOWN_PCT >= self.DAILY_LOSS_LIMIT_PCT, \
            f"MAX_DRAWDOWN_PCT ({self.MAX_DRAWDOWN_PCT}) must be >= DAILY_LOSS_LIMIT_PCT ({self.DAILY_LOSS_LIMIT_PCT})"
        assert 0.0 < self.KELLY_FRACTION <= 0.5, \
            f"KELLY_FRACTION ({self.KELLY_FRACTION}) must be in (0.0, 0.5]"
        assert self.ATR_MULTIPLIER_TP > self.ATR_MULTIPLIER_SL, \
            f"ATR_MULTIPLIER_TP ({self.ATR_MULTIPLIER_TP}) must be > ATR_MULTIPLIER_SL ({self.ATR_MULTIPLIER_SL})"
        return self


settings = Settings()
