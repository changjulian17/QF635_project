import json
import sqlite3

import numpy as np
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import streamlit as st

from config import settings

st.set_page_config(page_title="CryptoSentinel", layout="wide", page_icon="📈")
st.title("📈 CryptoSentinel — BTCUSDT Testnet")
st.caption("[🩺 System Health](/health) · use the sidebar to navigate between pages")

# Sidebar stays static — only re-renders on user interaction, not on fragment refresh
with st.sidebar:
    st.header("Settings")
    heatmap_bars = st.slider("Heatmap window (bars @ 100 ms)", 50, 400, 200)
    heatmap_tick_range = st.slider("Price range ± ticks", 10, 100, 40)
    obi_window = st.slider("OBI / CVD history (bars)", 100, 600, 300)


@st.fragment(run_every=30)
def live_dashboard() -> None:
    st.button("Refresh now")  # any click reruns this fragment immediately

    try:
        conn = sqlite3.connect("cryptosentinel.db")

        # ── Portfolio metrics ──────────────────────────────────────────────
        col1, col2, col3, col4 = st.columns(4)
        try:
            pf = pd.read_sql("SELECT * FROM portfolio ORDER BY ts DESC LIMIT 1", conn)
        except Exception:
            pf = pd.DataFrame()

        if not pf.empty:
            col1.metric("Equity", f"${pf['equity'].iloc[0]:,.2f}")
            col2.metric("Daily PnL", f"${pf['daily_pnl'].iloc[0]:,.2f}")
            col3.metric("Drawdown", f"{pf['drawdown_pct'].iloc[0]:.2%}")
            col4.metric("Circuit Breaker", pf["circuit_breaker"].iloc[0])
        else:
            st.caption("No portfolio data yet — start main.py to begin.")

        # ── Microstructure metrics row ─────────────────────────────────────
        try:
            ms_latest = pd.read_sql(
                "SELECT * FROM microstructure_bars ORDER BY ts DESC LIMIT 1", conn
            )
        except Exception:
            ms_latest = pd.DataFrame()

        if not ms_latest.empty:
            r = ms_latest.iloc[0]
            mc1, mc2, mc3, mc4, mc5, mc6 = st.columns(6)
            mc1.metric("Mid Price", f"${r['mid_price']:,.2f}")
            mc2.metric("Spread", f"${r['spread']:.2f}")
            obi_val = r["obi"]
            mc3.metric("OBI", f"{obi_val:+.3f}", delta=f"{'Buy' if obi_val > 0 else 'Sell'} pressure")
            mc4.metric("CVD", f"{r['cvd']:+.4f}")
            signal_flags = {
                "Reload↑": bool(r["reload_bid"]),   "Reload↓": bool(r["reload_ask"]),
                "Iceberg↑": bool(r["iceberg_bid"]),  "Iceberg↓": bool(r["iceberg_ask"]),
                "Sweep↑": bool(r["sweep_up"]),        "Sweep↓": bool(r["sweep_down"]),
                "Flip↑": bool(r["book_flip_bid"]),    "Flip↓": bool(r["book_flip_ask"]),
                "B+P Long": bool(r["break_protect_long"]),
                "B+P Short": bool(r["break_protect_short"]),
            }
            active = [k for k, v in signal_flags.items() if v]
            mc5.metric("Active signals", len(active))
            mc6.write("**Signals:** " + (", ".join(active) if active else "—"))

        st.divider()

        # ── Load microstructure history ────────────────────────────────────
        try:
            ms = pd.read_sql(
                f"SELECT * FROM microstructure_bars ORDER BY ts DESC LIMIT {max(heatmap_bars, obi_window)}",
                conn,
            ).sort_values("ts")
            ms["ts"] = pd.to_datetime(ms["ts"])
        except Exception:
            ms = pd.DataFrame()

        # ── Heatmap + OBI / CVD / Spread — shared x-axis ─────────────────
        st.subheader("Liquidity Heatmap + OBI / CVD / Spread")
        if not ms.empty:
            # Use the wider of the two windows so each panel shows its full history
            obi_df = ms.tail(obi_window).copy()
            hm_df  = ms.tail(heatmap_bars).copy()

            fig = make_subplots(
                rows=4, cols=1, shared_xaxes=True,
                row_heights=[0.50, 0.17, 0.17, 0.16],
                vertical_spacing=0.03,
                subplot_titles=("Liquidity Heatmap", "OBI", "CVD", "Spread"),
            )

            # ── Row 1: heatmap ────────────────────────────────────────────
            current_mid = float(hm_df["mid_price"].iloc[-1])
            price_lo = current_mid - heatmap_tick_range
            price_hi = current_mid + heatmap_tick_range
            tick_size = 1.0
            price_ticks = np.arange(price_lo, price_hi + tick_size, tick_size)
            n_times, n_prices = len(hm_df), len(price_ticks)

            bid_matrix = np.zeros((n_prices, n_times))
            ask_matrix = np.zeros((n_prices, n_times))
            for col_idx, (_, row) in enumerate(hm_df.iterrows()):
                try:
                    for p, q in json.loads(row["bid_levels"]):
                        ri = int(round((p - price_lo) / tick_size))
                        if 0 <= ri < n_prices:
                            bid_matrix[ri, col_idx] += q
                except Exception:
                    pass
                try:
                    for p, q in json.loads(row["ask_levels"]):
                        ri = int(round((p - price_lo) / tick_size))
                        if 0 <= ri < n_prices:
                            ask_matrix[ri, col_idx] += q
                except Exception:
                    pass

            fig.add_trace(go.Heatmap(
                z=ask_matrix - bid_matrix,
                x=hm_df["ts"], y=price_ticks,
                colorscale=[
                    [0.0, "rgba(0,180,80,0.9)"],
                    [0.5, "rgba(10,10,30,0.3)"],
                    [1.0, "rgba(220,30,30,0.9)"],
                ],
                zmid=0, showscale=False,
                hovertemplate="Time: %{x}<br>Price: %{y}<br>Net qty: %{z:.4f}<extra></extra>",
            ), row=1, col=1)

            fig.add_trace(go.Scatter(
                x=hm_df["ts"], y=hm_df["mid_price"], mode="lines",
                line=dict(color="white", width=1.5), name="Mid price", hoverinfo="skip",
            ), row=1, col=1)

            for vol_col, color, label in [
                ("buy_volume",  "rgba(0,255,100,0.6)",  "Buy aggression"),
                ("sell_volume", "rgba(255,60,60,0.6)",  "Sell aggression"),
            ]:
                mask = hm_df[vol_col] > 0
                if mask.any():
                    fig.add_trace(go.Scatter(
                        x=hm_df.loc[mask, "ts"], y=hm_df.loc[mask, "mid_price"],
                        mode="markers",
                        marker=dict(size=np.clip(hm_df.loc[mask, vol_col] * 40, 4, 24),
                                    color=color, line=dict(width=0)),
                        name=label,
                        hovertemplate=f"{label}: %{{text}}<extra></extra>",
                        text=hm_df.loc[mask, vol_col].round(4).astype(str),
                    ), row=1, col=1)

            for col_name, label, sym, clr, sz in [
                ("sweep_up",            "↑ Sweep",  "triangle-up",   "cyan",    5),
                ("sweep_down",          "↓ Sweep",  "triangle-down", "orange",  5),
                ("iceberg_bid",         "Iceberg↑", "diamond",       "lime",    6),
                ("iceberg_ask",         "Iceberg↓", "diamond",       "red",     6),
                ("book_flip_bid",       "Flip↑",    "x",             "yellow",  8),
                ("book_flip_ask",       "Flip↓",    "x",             "magenta", 8),
                ("break_protect_long",  "B+P↑",     "star",          "white",  10),
                ("break_protect_short", "B+P↓",     "star",          "silver", 10),
            ]:
                mask = hm_df[col_name].astype(bool)
                if mask.any():
                    fig.add_trace(go.Scatter(
                        x=hm_df.loc[mask, "ts"], y=hm_df.loc[mask, "mid_price"],
                        mode="markers",
                        marker=dict(symbol=sym, size=sz, color=clr,
                                    line=dict(width=1, color="black")),
                        name=label,
                    ), row=1, col=1)

            # ── Rows 2-4: OBI / CVD / Spread ─────────────────────────────
            fig.add_trace(go.Scatter(
                x=obi_df["ts"], y=obi_df["obi"], mode="lines",
                line=dict(color="cyan", width=1.2), name="OBI",
                fill="tozeroy", fillcolor="rgba(0,200,200,0.15)",
            ), row=2, col=1)
            fig.add_hline(y=settings.OBI_BREAK_THRESH,  line=dict(dash="dot", color="lime", width=1), row=2, col=1)
            fig.add_hline(y=-settings.OBI_BREAK_THRESH, line=dict(dash="dot", color="red",  width=1), row=2, col=1)
            fig.add_hline(y=0, line=dict(color="white", width=0.5), row=2, col=1)

            fig.add_trace(go.Scatter(
                x=obi_df["ts"], y=obi_df["cvd"], mode="lines",
                line=dict(color="orange", width=1.2), name="CVD",
                fill="tozeroy", fillcolor="rgba(255,165,0,0.12)",
            ), row=3, col=1)
            fig.add_hline(y=0, line=dict(color="white", width=0.5), row=3, col=1)

            fig.add_trace(go.Scatter(
                x=obi_df["ts"], y=obi_df["spread"], mode="lines",
                line=dict(color="violet", width=1), name="Spread",
            ), row=4, col=1)

            fig.update_layout(
                height=860, template="plotly_dark",
                yaxis_title="Price (USDT)",
                legend=dict(orientation="h", y=-0.06, font=dict(size=10)),
                margin=dict(t=40, b=20),
                showlegend=True,
            )
            fig.update_xaxes(showticklabels=False, row=1, col=1)
            fig.update_xaxes(showticklabels=False, row=2, col=1)
            fig.update_xaxes(showticklabels=False, row=3, col=1)
            fig.update_xaxes(title_text="Time", row=4, col=1)
            st.plotly_chart(fig, width="stretch")
        else:
            st.info("Waiting for microstructure data…")

        # ── Price chart + pattern signals ─────────────────────────────────
        st.subheader("Price + Pattern Signals")
        try:
            candles = pd.read_sql(
                "SELECT * FROM candles ORDER BY open_time DESC LIMIT 200", conn
            ).sort_values("open_time")
        except Exception:
            candles = pd.DataFrame()

        fig_price = go.Figure()
        if not candles.empty:
            fig_price.add_trace(go.Candlestick(
                x=candles["open_time"],
                open=candles["open"], high=candles["high"],
                low=candles["low"], close=candles["close"],
                name="OHLC",
            ))

        try:
            signals = pd.read_sql(
                "SELECT * FROM signals ORDER BY detected_at DESC LIMIT 20", conn
            )
        except Exception:
            signals = pd.DataFrame()

        if not signals.empty:
            for _, row in signals.iterrows():
                is_long = row.get("direction") == "LONG"
                fig_price.add_trace(go.Scatter(
                    x=[row.get("detected_at")], y=[row.get("entry_price")],
                    mode="markers",
                    marker=dict(symbol="triangle-up" if is_long else "triangle-down",
                                size=12, color="lime" if is_long else "red"),
                    name=row.get("pattern"),
                ))

        fig_price.update_layout(
            xaxis_rangeslider_visible=False, height=360,
            template="plotly_dark", margin=dict(t=20, b=20),
        )
        st.plotly_chart(fig_price, width="stretch")

        conn.close()

    except Exception as exc:
        st.error(f"Dashboard error: {exc}")


live_dashboard()
