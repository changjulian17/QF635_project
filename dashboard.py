import time

import pandas as pd
import plotly.graph_objects as go
import sqlite3
import streamlit as st
from binance.client import Client

from config import settings
from db_writer import init_db

st.set_page_config(page_title="CryptoSentinel", layout="wide", page_icon="📈")
st.title("CryptoSentinel — BTCUSDT Testnet")

init_db()

# ── Live account (Binance API) ─────────────────────────────────────────────
st.subheader("Live Account")
try:
    client = Client(
        api_key=settings.BINANCE_API_KEY,
        api_secret=settings.BINANCE_API_SECRET,
        testnet=settings.BINANCE_TESTNET,
    )
    account = client.get_account()
    TRACKED = {"BTC", "USDT", "BNB"}
    balances = [b for b in account["balances"] if b["asset"] in TRACKED]
    bal_cols = st.columns(len(balances))
    for col, b in zip(bal_cols, balances):
        col.metric(b["asset"], f"{float(b['free']):,.6f}", help="free balance")

    open_orders = client.get_open_orders(symbol=settings.SYMBOL)
    if open_orders:
        st.caption(f"{len(open_orders)} open order(s) on {settings.SYMBOL}")
        order_rows = [
            {
                "orderId": o["orderId"],
                "side": o["side"],
                "type": o["type"],
                "price": o["price"],
                "origQty": o["origQty"],
                "status": o["status"],
                "time": pd.to_datetime(o["time"], unit="ms"),
            }
            for o in open_orders
        ]
        st.dataframe(pd.DataFrame(order_rows), use_container_width=True, hide_index=True)
    else:
        st.caption(f"No open orders on {settings.SYMBOL}")

except Exception as exc:
    st.warning(f"Could not fetch live account: {exc}")

st.divider()

# ── Portfolio (paper trading, from SQLite) ─────────────────────────────────
st.subheader("Paper Portfolio")
try:
    conn = sqlite3.connect("cryptosentinel.db")

    portfolio_row = pd.read_sql(
        "SELECT * FROM portfolio ORDER BY ts DESC LIMIT 1", conn
    )
    if not portfolio_row.empty:
        p = portfolio_row.iloc[0]
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Equity", f"${p['equity']:,.2f}")
        c2.metric("Daily PnL", f"${p['daily_pnl']:,.2f}")
        c3.metric("Drawdown", f"{p['drawdown_pct']:.2%}")
        c4.metric("Circuit Breaker", p["circuit_breaker"])
    else:
        st.caption("No portfolio data yet — start main.py to begin.")

    # ── Candlestick chart + signal markers ────────────────────────────────
    st.subheader("BTCUSDT Chart")
    candles = pd.read_sql(
        "SELECT * FROM candles ORDER BY open_time DESC LIMIT 200", conn
    ).sort_values("open_time")

    fig = go.Figure()
    if not candles.empty:
        fig.add_trace(go.Candlestick(
            x=candles["open_time"],
            open=candles["open"], high=candles["high"],
            low=candles["low"], close=candles["close"],
            name="OHLC",
        ))

    signals = pd.read_sql(
        "SELECT * FROM signals ORDER BY detected_at DESC LIMIT 20", conn
    )
    if not signals.empty:
        for _, row in signals.iterrows():
            is_long = row.get("direction") == "LONG"
            fig.add_trace(go.Scatter(
                x=[row.get("detected_at")],
                y=[row.get("entry_price")],
                mode="markers",
                marker=dict(
                    symbol="triangle-up" if is_long else "triangle-down",
                    size=14,
                    color="lime" if is_long else "red",
                ),
                name=row.get("pattern"),
            ))

    fig.update_layout(
        xaxis_rangeslider_visible=False,
        height=480,
        template="plotly_dark",
        margin=dict(l=0, r=0, t=0, b=0),
    )
    st.plotly_chart(fig, use_container_width=True)

    # ── Recent signals table ───────────────────────────────────────────────
    if not signals.empty:
        st.subheader("Recent Signals")
        st.dataframe(
            signals[["detected_at", "pattern", "direction",
                      "confidence", "entry_price", "stop_loss",
                      "take_profit", "r2", "volume_ratio"]].round(4),
            use_container_width=True,
            hide_index=True,
        )

    conn.close()

except Exception as exc:
    st.error(f"Dashboard error: {exc}")

# ── Auto-refresh ───────────────────────────────────────────────────────────
time.sleep(settings.UI_REFRESH_INTERVAL)
st.rerun()
