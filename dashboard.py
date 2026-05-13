import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import sqlite3, time

from config import settings

st.set_page_config(page_title="CryptoSentinel", layout="wide", page_icon="📈")
st.title("📈 CryptoSentinel — BTCUSDT Testnet")

placeholder = st.empty()

while True:
    with placeholder.container():
        try:
            conn = sqlite3.connect("cryptosentinel.db")

            col1, col2, col3, col4 = st.columns(4)
            try:
                portfolio_row = pd.read_sql("SELECT * FROM portfolio ORDER BY ts DESC LIMIT 1", conn)
            except Exception:
                portfolio_row = pd.DataFrame()

            if not portfolio_row.empty:
                col1.metric("Equity", f"${portfolio_row['equity'].iloc[0]:,.2f}")
                col2.metric("Daily PnL", f"${portfolio_row['daily_pnl'].iloc[0]:,.2f}")
                col3.metric("Drawdown", f"{portfolio_row['drawdown_pct'].iloc[0]:.2%}")
                col4.metric("Circuit Breaker", portfolio_row['circuit_breaker'].iloc[0])

            try:
                candles = pd.read_sql("SELECT * FROM candles ORDER BY open_time DESC LIMIT 200", conn).sort_values("open_time")
            except Exception:
                candles = pd.DataFrame()

            fig = go.Figure()
            if not candles.empty:
                fig.add_trace(go.Candlestick(
                    x=candles["open_time"], open=candles["open"], high=candles["high"],
                    low=candles["low"], close=candles["close"], name="OHLC"
                ))

            try:
                signals = pd.read_sql("SELECT * FROM signals ORDER BY detected_at DESC LIMIT 20", conn)
            except Exception:
                signals = pd.DataFrame()

            if not signals.empty:
                for _, row in signals.iterrows():
                    symbol = "triangle-up" if row.get("direction") == "LONG" else "triangle-down"
                    color = "lime" if row.get("direction") == "LONG" else "red"
                    fig.add_trace(go.Scatter(x=[row.get("detected_at")], y=[row.get("entry_price")],
                                             mode="markers", marker=dict(symbol=symbol, size=12, color=color),
                                             name=row.get("pattern")))

            fig.update_layout(xaxis_rangeslider_visible=False, height=480, template="plotly_dark")
            st.plotly_chart(fig, use_container_width=True)

            conn.close()
        except Exception as exc:
            st.error(f"Dashboard error: {exc}")
    time.sleep(settings.UI_REFRESH_INTERVAL)
