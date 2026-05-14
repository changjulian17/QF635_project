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

    # Strategy
    SYMBOL: str = "BTCUSDT"
    CANDLE_INTERVAL: str = "1s"
    PATTERN_LOOKBACK: int = 50
    SWING_WINDOW: int = 5
    BREAKOUT_VOL_MULT: float = 1.5
    MIN_R2: float = 0.80

    # Risk
    MAX_DRAWDOWN_PCT: float = 0.05
    DAILY_LOSS_LIMIT_PCT: float = 0.02
    MAX_CONSECUTIVE_LOSSES: int = 3
    RISK_PER_TRADE_PCT: float = 0.01
    KELLY_FRACTION: float = 0.25
    ATR_MULTIPLIER_SL: float = 1.5
    ATR_MULTIPLIER_TP: float = 3.0

    # Execution
    DRY_RUN: bool = True

    # UI
    UI_REFRESH_INTERVAL: float = 1.0

    # Microstructure / LOB
    LOB_DEPTH: int = 1000                   # levels fetched per snapshot (stored in heatmap)
    LOB_OBI_DEPTH: int = 20                 # top-N levels used for OBI computation
    LOB_HISTORY: int = 18000                # bars to keep in DB (5 h at 1 bar/s cadence)
    LOB_HEATMAP_BUCKET: float = 5.0         # $ bucket size for heatmap price aggregation
    RELOAD_SIGMA: float = 3.0               # σ threshold for reload detection
    ICEBERG_WINDOW_MS: int = 500            # look-back window for iceberg resistance algorithm
    ICEBERG_MIN_REPLENISH: float = 0.80     # level must refill to ≥80 % of previous qty
    ICEBERG_MIN_QTY: float = 0.5            # minimum level qty (BTC) — filters thin noise levels
    ICEBERG_PRICE_TOL: float = 0.10         # max $ distance for a trade to count as hitting a level
    SWEEP_LEVELS: int = 5                   # ask/bid levels whose volume is compared against trade size
    BREAK_PROTECT_WINDOW_MS: int = 2000     # ms after a breakout to look for protective liquidity
    OBI_BREAK_THRESH: float = 0.40          # OBI magnitude required to confirm break+protect

settings = Settings()
