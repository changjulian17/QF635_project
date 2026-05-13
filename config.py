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

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"

settings = Settings()
