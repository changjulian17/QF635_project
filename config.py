from pydantic_settings import BaseSettings
from pydantic import Field

class Settings(BaseSettings):
    # Binance Testnet
    BINANCE_API_KEY: str = Field("", env="BINANCE_API_KEY")
    BINANCE_API_SECRET: str = Field("", env="BINANCE_API_SECRET")
    BINANCE_TESTNET: bool = True
    WS_BASE: str = "wss://stream.testnet.binance.vision/ws"
    REST_BASE: str = "https://testnet.binance.vision"

    # Strategy
    SYMBOL: str = "BTCUSDT"
    CANDLE_INTERVAL: str = "1m"
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

    # UI
    UI_REFRESH_INTERVAL: float = 1.0

    # Microstructure / LOB
    LOB_DEPTH: int = 20                     # levels to use for OBI and heatmap
    LOB_HISTORY: int = 600                  # bars to keep in DB (60 s at 100 ms cadence)
    RELOAD_SIGMA: float = 3.0               # σ threshold for reload detection
    ICEBERG_WINDOW_MS: int = 500            # look-back window for iceberg resistance algorithm
    SWEEP_LEVELS: int = 5                   # ask/bid levels whose volume is compared against trade size
    BREAK_PROTECT_WINDOW_MS: int = 2000     # ms after a breakout to look for protective liquidity
    OBI_BREAK_THRESH: float = 0.40          # OBI magnitude required to confirm break+protect

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"

settings = Settings()
