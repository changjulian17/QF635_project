from pydantic_settings import BaseSettings
from pydantic import ConfigDict

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
    MAX_CONSECUTIVE_LOSSES: int = 3
    RISK_PER_TRADE_PCT: float = 0.01
    KELLY_FRACTION: float = 0.25
    ATR_MULTIPLIER_SL: float = 1.5
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

    # Heartbeat monitor (§4)
    HEARTBEAT_WARN_MS: int = 200
    HEARTBEAT_CRITICAL_MS: int = 500
    HEARTBEAT_CONSEC_LIMIT: int = 3

    # Persistence
    REGISTRY_DB: str = "strategies/registry.db"
    LOB_TICK_DB: str = "data/lob_tick.db"

settings = Settings()
