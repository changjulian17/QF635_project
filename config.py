from typing import Literal

from pydantic_settings import BaseSettings
from pydantic import ConfigDict, model_validator


class Settings(BaseSettings):
    model_config = ConfigDict(env_file=".env", env_file_encoding="utf-8")

    # Binance Testnet
    BINANCE_API_KEY: str = ""
    BINANCE_API_SECRET: str = ""
    BINANCE_TESTNET: bool = True
    BINANCE_DEMO: bool = False
    TRADING_MODE: Literal["testnet", "demo", "live"] = "testnet"
    DEMO_BINANCE_API_KEY: str = ""
    DEMO_BINANCE_API_SECRET: str = ""
    WS_BASE: str = "wss://stream.binancefuture.com"
    REST_BASE: str = "https://testnet.binancefuture.com"
    LOB_RECORDER_WS: str = "wss://stream.binancefuture.com"  # futures testnet stream; fstream.binance.com does not deliver aggTrade on this connection
    LOB_RECORDER_REST: str = "https://testnet.binancefuture.com"  # REST base matching LOB_RECORDER_WS; must stay in sync

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
    STARTING_EQUITY: float = 1_000_000.0  # design account size (1M USDT testnet); overwritten by reconciler
    MAX_DRAWDOWN_PCT: float = 0.05
    DAILY_LOSS_LIMIT_PCT: float = 0.02
    # 5-tier DOV loss thresholds (§5 risk_management_plan)
    TIER_REDUCED_PCT: float = 0.0050   # ≥ 0.50% DOV loss → REDUCED
    TIER_MINIMAL_PCT: float = 0.0075   # ≥ 0.75% DOV loss → MINIMAL
    TIER_PASSIVE_PCT: float = 0.0090   # ≥ 0.90% DOV loss → PASSIVE (no new entries)
    TIER_HALTED_PCT:  float = 0.0100   # ≥ 1.00% DOV loss → HALTED
    MAX_CONSECUTIVE_LOSSES: int = 3
    RISK_PER_TRADE_PCT: float = 0.001  # 0.1% — sized for ~10 bps microstructure stops
    KELLY_FRACTION: float = 0.25
    ATR_MULTIPLIER_SL: float = 1.5   # legacy/backtest default; microstructure path uses wall-based SL
    ATR_MULTIPLIER_TP: float = 3.0

    # Execution
    DRY_RUN: bool = True
    # When True, the startup reconciler sells all BTC → USDT to start "clean in USDT".
    # MUST stay False for a strategy that takes SHORT entries: on a spot account a SHORT
    # is a SELL of held BTC, so liquidating all BTC at startup makes every SHORT entry
    # fail the "insufficient BTC" pre-flight. Default False keeps BTC inventory tradeable.
    LIQUIDATE_BTC_ON_STARTUP: bool = False
    IOC_TIMEOUT_MS: int = 200          # IOC order max age before cancel-no-retry
    QTY_STEP_SIZE: float = 0.001        # BTCUSDT perpetual futures LOT_SIZE stepSize
    PRICE_TICK_SIZE: float = 0.10       # BTCUSDT perpetual futures price tick
    MIN_NOTIONAL: float = 100.0        # BTCUSDT NOTIONAL filter minimum (USD)
    MAX_ORDER_NOTIONAL_PCT: float = 0.90  # gate rejects signals whose estimated notional exceeds 90% of equity
    SLIPPAGE_RESEARCH_BPS: float = 3.0  # expected slippage assumption (KS-3 baseline)
    SLIPPAGE_MULTIPLIER: float = 1.5    # KS-3 fires when rolling avg > research × multiplier
    WS_RECV_TIMEOUT_S: float = 20.0     # Max seconds between WS messages before reconnect
    WS_PING_TIMEOUT_S: float = 20.0     # Max seconds for WS pong before reconnect
    WS_PING_INTERVAL_S: float = 20.0    # Seconds between WS pings

    # UI
    UI_REFRESH_INTERVAL: float = 1.0

    # Microstructure / LOB
    LOB_DEPTH: int = 1000
    LOB_OBI_DEPTH: int = 20
    LOB_HISTORY: int = 18000
    LOB_HEATMAP_BUCKET: float = 1.0
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
    MICRO_PRICE_MOVE_FLOOR_BPS: float = 3.0
    MICRO_PRICE_MOVE_WINDOW: int = 300
    MICRO_PRICE_MOVE_PERCENTILE: float = 0.90
    MICRO_PRICE_MOVE_MIN_SAMPLES: int = 50
    PROTECTION_MAX_DISTANCE_BPS: float = 25.0
    # Minimum mid→protection-wall distance. A protective wall closer than this to mid is
    # not a meaningful structural level — it implies a near-zero stop and therefore an
    # unbounded position size (qty = risk / stop_distance). Filters degenerate signals
    # (e.g. on the thin Binance testnet book, where walls sit ~0.001 bps from mid) and
    # floors the stop distance used for sizing. Must be < PROTECTION_MAX_DISTANCE_BPS.
    PROTECTION_MIN_DISTANCE_BPS: float = 1.0
    MICRO_MAX_HOLD_MS: int = 60_000
    MICRO_EXIT_SPREAD_HARD_CAP_BPS: float = 12.0
    LOB_FRESH_WALL_MS: int = 3_000    # protection wall must appear within this window
    LOB_STALE_WALL_MS: int = 30_000   # prune wall states not seen for this long

    # LOB gap handling
    LOB_GAP_RECONNECT_MIN_CONSECUTIVE: int = 3  # reconnect only after this many consecutive gaps

    # Heartbeat monitor (§4)
    HEARTBEAT_WARN_MS: int = 200
    HEARTBEAT_CRITICAL_MS: int = 500
    HEARTBEAT_LOB_CRITICAL_MS: int = 30000   # depth@500ms stream; higher due to aggregation window
    HEARTBEAT_CONSEC_LIMIT: int = 3
    HEARTBEAT_KS2_ENABLED: bool = True   # set False in demo/dev to suppress KS-2 on poor WS links
    HEARTBEAT_DEGRADED_RATE_THRESH: float = 0.5    # ≥50% of 10-msg window → enter DEGRADED
    HEARTBEAT_DEGRADED_RECOVERY_THRESH: float = 0.3 # <30% of window → exit (hysteresis)
    HEARTBEAT_SUSTAINED_MS: int = 10_000            # ms in DEGRADED before SUSTAINED_DEGRADED

    # Persistence
    REGISTRY_DB: str = "strategies/registry.db"
    LOB_TICK_DB: str = "data/lob_tick.db"
    BACKTEST_RESULTS_DB: str = "data/backtest_results.db"

    # Dashboard
    DASHBOARD_API_PORT: int = 8080

    # Alerting
    ALERT_WEBHOOK_URL: str = ""         # optional; empty = disabled

    # Logging
    LOG_LEVEL: str = "INFO"             # set LOG_LEVEL=DEBUG in .env for full gate-input trace
    LOG_FILE: str = "logs/cryptosentinel.log"
    LOG_MAX_BYTES: int = 5_000_000      # 5 MB per file
    LOG_BACKUP_COUNT: int = 5           # keep 5 rotated files (~25 MB total)

    # Speed bumps
    MIN_SIGNAL_INTERVAL_MS: int = 0     # 0 = disabled; e.g. 500 enforces ≤2 approvals/sec

    # Test harness — inject synthetic signals to validate execution pipeline without
    # waiting for a natural Sweep+Protection event (set in .env, never in prod)
    TEST_SIGNAL_INJECT: bool = False
    TEST_INJECT_INTERVAL_MS: int = 30_000   # ms between injected signals

    @model_validator(mode="after")
    def _apply_mode_presets(self) -> "Settings":
        """
        Apply mode-specific defaults for any field not explicitly set by the caller.
        Explicit env vars / .env entries always win — only unset fields are touched.
        Runs before _validate_tier_ordering so preset values are visible to validation.
        """
        # fmt: off
        _PRESETS: dict[str, dict[str, object]] = {
            # ── testnet (start_test.sh) ───────────────────────────────────────────
            # WS: wss://stream.binancefuture.com (~10ms baseline). Real orders, fake money.
            "testnet": {
                "BINANCE_TESTNET":            False,
                "BINANCE_DEMO":               True,
                "DRY_RUN":                    False,
                "HEARTBEAT_WARN_MS":          5000,
                "HEARTBEAT_CRITICAL_MS":      30000,
                "HEARTBEAT_LOB_CRITICAL_MS":  30000,
                "HEARTBEAT_CONSEC_LIMIT":     20,
                "WS_RECV_TIMEOUT_S":          30.0,
                "WS_PING_TIMEOUT_S":          30.0,
                "MIN_CONFIDENCE":             0.1,
                "TEST_SIGNAL_INJECT":         True,
                "TIMEFRAME":                  "1m",
            },
            # ── demo (start_demo.sh) ──────────────────────────────────────────────
            # WS: wss://fstream.binance.com (live server, p50=181ms p95=428ms).
            # Heartbeat calibrated for ~200ms baseline + periodic 3–5s TCP stalls.
            "demo": {
                "WS_BASE":                    "wss://fstream.binance.com",
                "REST_BASE":                  "https://fapi.binance.com",
                "BINANCE_TESTNET":            False,
                "BINANCE_DEMO":               True,
                "DRY_RUN":                    False,
                "HEARTBEAT_WARN_MS":          5000,
                "HEARTBEAT_CRITICAL_MS":      30000,
                "HEARTBEAT_LOB_CRITICAL_MS":  30000,
                "HEARTBEAT_CONSEC_LIMIT":     20,
                "HEARTBEAT_KS2_ENABLED":      False,
                "WS_RECV_TIMEOUT_S":          60.0,
                "WS_PING_TIMEOUT_S":          60.0,
                "MIN_CONFIDENCE":             0.1,
                "TEST_SIGNAL_INJECT":         True,
                "TIMEFRAME":                  "1m",
            },
            # ── live (start.sh) ───────────────────────────────────────────────────
            # Same server as demo. Real money. Full risk management on.
            "live": {
                "WS_BASE":                    "wss://fstream.binance.com",
                "REST_BASE":                  "https://fapi.binance.com",
                "BINANCE_TESTNET":            False,
                "BINANCE_DEMO":               False,
                "DRY_RUN":                    False,
                "HEARTBEAT_WARN_MS":          500,
                "HEARTBEAT_CRITICAL_MS":      3000,
                "HEARTBEAT_LOB_CRITICAL_MS":  5000,
                "HEARTBEAT_CONSEC_LIMIT":     5,
                "HEARTBEAT_KS2_ENABLED":      True,
                "MIN_CONFIDENCE":             0.58,
                "TEST_SIGNAL_INJECT":         False,
                "TIMEFRAME":                  "5m",
            },
        }
        # fmt: on
        for field, value in _PRESETS.get(self.TRADING_MODE, {}).items():
            if field not in self.model_fields_set:
                object.__setattr__(self, field, value)
        return self

    @model_validator(mode="after")
    def _validate_tier_ordering(self) -> "Settings":
        if self.BINANCE_TESTNET and self.BINANCE_DEMO:
            raise ValueError(
                "BINANCE_TESTNET and BINANCE_DEMO cannot both be True. "
                "Set BINANCE_TESTNET=false when using demo.binance.com."
            )
        if not self.DRY_RUN and not self.BINANCE_DEMO and (not self.BINANCE_API_KEY or not self.BINANCE_API_SECRET):
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
        assert self.MICRO_PRICE_MOVE_FLOOR_BPS > 0.0, \
            "MICRO_PRICE_MOVE_FLOOR_BPS must be > 0"
        assert self.MICRO_PRICE_MOVE_WINDOW > 0, \
            "MICRO_PRICE_MOVE_WINDOW must be > 0"
        assert 0.0 < self.MICRO_PRICE_MOVE_PERCENTILE < 1.0, \
            "MICRO_PRICE_MOVE_PERCENTILE must be in (0, 1)"
        assert 0 < self.MICRO_PRICE_MOVE_MIN_SAMPLES <= self.MICRO_PRICE_MOVE_WINDOW, \
            "MICRO_PRICE_MOVE_MIN_SAMPLES must be in [1, MICRO_PRICE_MOVE_WINDOW]"
        assert 0.0 < self.PROTECTION_MIN_DISTANCE_BPS < self.PROTECTION_MAX_DISTANCE_BPS, \
            (f"PROTECTION_MIN_DISTANCE_BPS ({self.PROTECTION_MIN_DISTANCE_BPS}) must be in "
             f"(0, PROTECTION_MAX_DISTANCE_BPS={self.PROTECTION_MAX_DISTANCE_BPS})")
        assert self.MICRO_MAX_HOLD_MS > 0, \
            "MICRO_MAX_HOLD_MS must be > 0"
        assert self.MICRO_EXIT_SPREAD_HARD_CAP_BPS > 0.0, \
            "MICRO_EXIT_SPREAD_HARD_CAP_BPS must be > 0"
        if self.MIN_SIGNAL_INTERVAL_MS > 0:
            assert self.MIN_SIGNAL_INTERVAL_MS < self.IOC_TIMEOUT_MS, (
                f"MIN_SIGNAL_INTERVAL_MS ({self.MIN_SIGNAL_INTERVAL_MS}ms) must be < "
                f"IOC_TIMEOUT_MS ({self.IOC_TIMEOUT_MS}ms) — otherwise approved signals "
                f"expire before the next approval window opens"
            )
        return self


settings = Settings()
