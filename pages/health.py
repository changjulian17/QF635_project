"""Health checker page — shows live status of every CryptoSentinel component."""
import sqlite3
import time
from datetime import datetime, timezone, timedelta

import psutil
import streamlit as st

from config import settings

st.set_page_config(page_title="Health · CryptoSentinel", layout="wide", page_icon="🩺")
st.title("🩺 System Health")


# ── helpers ───────────────────────────────────────────────────────────────────

def _ok(label: str, detail: str = "") -> dict:
    return {"label": label, "status": "ok", "detail": detail}

def _warn(label: str, detail: str = "") -> dict:
    return {"label": label, "status": "warn", "detail": detail}

def _err(label: str, detail: str = "") -> dict:
    return {"label": label, "status": "err", "detail": detail}


def _process_running(script_name: str) -> bool:
    """Return True if any Python process has script_name in its command line."""
    for proc in psutil.process_iter(["cmdline"]):
        try:
            if any(script_name in arg for arg in (proc.info["cmdline"] or [])):
                return True
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            pass
    return False


def _db_freshness(table: str, ts_col: str, stale_seconds: int = 10) -> tuple[bool, str]:
    """Return (is_fresh, human-readable age string)."""
    try:
        conn = sqlite3.connect("cryptosentinel.db")
        row = conn.execute(
            f"SELECT {ts_col} FROM {table} ORDER BY {ts_col} DESC LIMIT 1"
        ).fetchone()
        conn.close()
        if not row:
            return False, "no data"
        last = datetime.fromisoformat(row[0])
        if last.tzinfo is None:
            last = last.replace(tzinfo=timezone.utc)
        age = (datetime.now(timezone.utc) - last).total_seconds()
        age_str = f"{age:.1f}s ago"
        return age <= stale_seconds, age_str
    except Exception as exc:
        return False, str(exc)


def _row_count(table: str) -> int:
    try:
        conn = sqlite3.connect("cryptosentinel.db")
        n = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        conn.close()
        return n
    except Exception:
        return -1


def _binance_ping() -> tuple[bool, str]:
    try:
        from binance.client import Client
        t0 = time.perf_counter()
        client = Client(
            api_key=settings.BINANCE_API_KEY,
            api_secret=settings.BINANCE_API_SECRET,
            testnet=settings.BINANCE_TESTNET,
        )
        client.get_server_time()
        latency = (time.perf_counter() - t0) * 1000
        return True, f"{latency:.0f} ms"
    except Exception as exc:
        return False, str(exc)


def _db_accessible() -> tuple[bool, str]:
    try:
        conn = sqlite3.connect("cryptosentinel.db")
        tables = {
            r[0] for r in
            conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
        }
        conn.close()
        expected = {"candles", "signals", "portfolio", "microstructure_bars"}
        missing = expected - tables
        if missing:
            return False, f"missing tables: {missing}"
        return True, f"{len(tables)} tables found"
    except Exception as exc:
        return False, str(exc)


# ── checks ────────────────────────────────────────────────────────────────────

def run_checks() -> list[dict]:
    results = []

    # ── Processes ─────────────────────────────────────────────────────────
    if _process_running("main.py"):
        results.append(_ok("Trading engine (main.py)", "process running"))
    else:
        results.append(_err("Trading engine (main.py)", "process not found — run: python main.py"))

    if _process_running("streamlit"):
        results.append(_ok("Dashboard (Streamlit)", "process running"))
    else:
        results.append(_warn("Dashboard (Streamlit)", "process not detected"))

    # ── Binance REST ──────────────────────────────────────────────────────
    ok, detail = _binance_ping()
    if ok:
        results.append(_ok("Binance REST (testnet)", f"reachable · {detail}"))
    else:
        results.append(_err("Binance REST (testnet)", detail))

    # ── SQLite ────────────────────────────────────────────────────────────
    ok, detail = _db_accessible()
    if ok:
        results.append(_ok("SQLite database", detail))
    else:
        results.append(_err("SQLite database", detail))

    # ── WebSocket / Microstructure feed ───────────────────────────────────
    fresh, age = _db_freshness("microstructure_bars", "ts", stale_seconds=10)
    n = _row_count("microstructure_bars")
    if fresh:
        results.append(_ok("WebSocket + Microstructure engine", f"last bar {age} · {n} bars stored"))
    elif n == 0:
        results.append(_err("WebSocket + Microstructure engine", "no data — is main.py running?"))
    else:
        results.append(_warn("WebSocket + Microstructure engine", f"stale — last bar {age}"))

    # ── Candle stream / Pattern detector ─────────────────────────────────
    fresh, age = _db_freshness("candles", "open_time", stale_seconds=5)
    n_candles = _row_count("candles")
    if fresh:
        status = _ok if n_candles >= 20 else _warn
        note = "" if n_candles >= 20 else f" (need 20+ for pattern detection, have {n_candles})"
        results.append(status("Candle stream (kline_1s)", f"last candle {age} · {n_candles} stored{note}"))
    elif n_candles == 0:
        results.append(_err("Candle stream (kline_1s)", "no candles — is main.py running?"))
    else:
        results.append(_warn("Candle stream (kline_1s)", f"stale — last candle {age}"))

    # ── Pattern signals ───────────────────────────────────────────────────
    n_signals = _row_count("signals")
    if n_signals > 0:
        results.append(_ok("Pattern detector", f"{n_signals} signal(s) detected"))
    elif n_candles >= 20:
        results.append(_warn("Pattern detector", f"running ({n_candles} candles) — no signals yet"))
    else:
        results.append(_warn("Pattern detector", f"waiting for 20 candles (have {max(n_candles, 0)})"))

    # ── Risk engine / Portfolio state ─────────────────────────────────────
    fresh, age = _db_freshness("portfolio", "ts", stale_seconds=15)
    if fresh:
        results.append(_ok("Risk engine + DB writer", f"portfolio updated {age}"))
    elif _row_count("portfolio") == 0:
        results.append(_warn("Risk engine + DB writer", "no portfolio rows yet (updates every 5 s)"))
    else:
        results.append(_warn("Risk engine + DB writer", f"portfolio stale — last update {age}"))

    # ── DRY_RUN flag ──────────────────────────────────────────────────────
    if settings.DRY_RUN:
        results.append(_warn("Order manager", "DRY_RUN=True — orders logged only, not submitted"))
    else:
        results.append(_ok("Order manager", "DRY_RUN=False — live order submission enabled"))

    return results


# ── rendering ─────────────────────────────────────────────────────────────────

STATUS_ICON  = {"ok": "✅", "warn": "⚠️", "err": "❌"}
STATUS_COLOR = {"ok": "green", "warn": "orange", "err": "red"}


@st.fragment(run_every=10)
def health_panel() -> None:
    st.button("Refresh now")

    checks = run_checks()

    n_ok   = sum(1 for c in checks if c["status"] == "ok")
    n_warn = sum(1 for c in checks if c["status"] == "warn")
    n_err  = sum(1 for c in checks if c["status"] == "err")

    # Summary bar
    s1, s2, s3 = st.columns(3)
    s1.metric("Healthy", n_ok)
    s2.metric("Warnings", n_warn)
    s3.metric("Errors", n_err)

    if n_err == 0 and n_warn == 0:
        st.success("All systems operational")
    elif n_err == 0:
        st.warning(f"{n_warn} warning(s) — system functional")
    else:
        st.error(f"{n_err} error(s) detected — action required")

    st.divider()

    # Per-component rows
    for check in checks:
        icon  = STATUS_ICON[check["status"]]
        col_a, col_b = st.columns([1, 3])
        col_a.markdown(f"**{icon} {check['label']}**")
        col_b.caption(check["detail"])

    st.caption(f"Last checked: {datetime.now(timezone.utc).strftime('%H:%M:%S UTC')} · auto-refreshes every 10 s")


health_panel()
