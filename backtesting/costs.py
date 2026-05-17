"""
backtesting/costs.py
====================
Transaction cost model for CryptoSentinel backtesting.

Cost Components
---------------
Every backtest must account for the full round-trip cost of a trade.
Ignoring costs systematically overstates returns and can make
unprofitable strategies appear viable.

For BTCUSDT on Binance Spot (standard account, no BNB discount):

  Taker fee   : 0.10 %   (market orders — used for breakout entries)
  Maker fee   : 0.10 %   (limit orders  — used for TP exits)
  Slippage    : 0.05 %   (conservative estimate for a liquid pair)
  ─────────────────────
  Round trip  : 0.30 %   (entry taker + exit maker + 2× slippage)

At a 2:1 R:R with ATR × 1.5 SL and a typical 0.4% move to stop,
the 0.30% round-trip cost is ~75% of the stop distance — material.

Usage
-----
>>> cost = TransactionCostModel()
>>> entry_cost = cost.entry_cost(price=67_000, quantity=0.001)
>>> exit_cost  = cost.exit_cost(price=68_000,  quantity=0.001)
>>> print(f"Round trip: {cost.round_trip_pct:.3%}")
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass
class TransactionCostModel:
    """
    Deterministic transaction cost model.

    All rates are expressed as fractions (e.g. 0.001 = 0.1%).

    Attributes
    ----------
    taker_fee_rate  : Fee charged when taking liquidity (market orders).
    maker_fee_rate  : Fee charged when making liquidity (limit orders).
    slippage_rate   : One-way slippage estimate (applied per order side).
    """

    taker_fee_rate: float = 0.001   # 0.10 %
    maker_fee_rate: float = 0.001   # 0.10 %
    slippage_rate:  float = 0.0005  # 0.05 %

    def entry_cost(
        self,
        price:     float,
        quantity:  float,
        is_market: bool = True,
    ) -> float:
        """
        Total dollar cost of entering a position.

        Entry is assumed to be a market (taker) order by default,
        matching the live system's breakout entry strategy.

        Parameters
        ----------
        price     : Entry fill price.
        quantity  : Position size in base asset.
        is_market : True → taker fee; False → maker fee.

        Returns
        -------
        Cost in quote currency (USDT).
        """
        fee_rate   = self.taker_fee_rate if is_market else self.maker_fee_rate
        notional   = price * quantity
        fee        = notional * fee_rate
        slippage   = notional * self.slippage_rate
        return fee + slippage

    def exit_cost(
        self,
        price:     float,
        quantity:  float,
        is_market: bool = False,
    ) -> float:
        """
        Total dollar cost of exiting a position.

        Exit is assumed to be a limit (maker) order by default,
        matching the OCO take-profit leg in the live system.

        Parameters
        ----------
        price     : Exit fill price.
        quantity  : Position size in base asset.
        is_market : True → taker fee (SL hit); False → maker fee (TP hit).

        Returns
        -------
        Cost in quote currency (USDT).
        """
        fee_rate = self.taker_fee_rate if is_market else self.maker_fee_rate
        notional = price * quantity
        fee      = notional * fee_rate
        slippage = notional * self.slippage_rate
        return fee + slippage

    @property
    def round_trip_pct(self) -> float:
        """
        Total percentage cost for one complete entry + exit cycle.

        Assumes:  market entry (taker) + limit exit (maker) + 2× slippage.
        """
        return (
            self.taker_fee_rate
            + self.maker_fee_rate
            + 2 * self.slippage_rate
        )
