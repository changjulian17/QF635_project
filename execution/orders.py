"""
Order domain classes — one class per order shape.

Each subclass wraps the parameters for a specific Binance order type and
exposes them via to_entry_params(), returning the kwargs dict for the
matching AsyncClient method:

  IOCLimitOrder → client.create_order(**order.to_entry_params())
  OCOOrder      → client.create_oco_order(**order.to_entry_params())

Adding a new order type (MarketOrder, PostOnlyLimit, TWAP bracket …)
requires only a new subclass — OrderManager is not modified.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass


@dataclass
class Order(ABC):
    """Abstract base for all exchange orders."""
    symbol:   str
    side:     str      # "BUY" | "SELL"
    quantity: float

    @abstractmethod
    def to_entry_params(self) -> dict:
        """Return the keyword-argument dict for the Binance AsyncClient call."""


@dataclass
class IOCLimitOrder(Order):
    """
    Aggressive IOC limit order.

    Used for entries (half-spread crossing) and emergency closes.
    Priced to cross the spread so it gains queue priority; expires immediately
    if unmatched — no retry, no residual resting order.
    """
    price: float = 0.0

    def to_entry_params(self) -> dict:
        return {
            "symbol":      self.symbol,
            "side":        self.side,
            "type":        "LIMIT",
            "timeInForce": "IOC",
            "quantity":    self.quantity,
            "price":       str(round(self.price, 2)),
        }


@dataclass
class OCOOrder(Order):
    """
    One-Cancels-the-Other bracket: TP limit + SL stop-limit.

    sl_limit is set slightly inside sl_price so the stop-limit leg
    fills immediately on trigger rather than posting passively:
      SELL bracket: sl_limit = sl_price × 0.999  (below → fills on downmove)
      BUY  bracket: sl_limit = sl_price × 1.001  (above → fills on upmove)
    """
    tp_price: float = 0.0
    sl_price: float = 0.0
    sl_limit: float = 0.0    # maps to belowPrice (SELL bracket) or abovePrice (BUY bracket)

    def to_entry_params(self) -> dict:
        # python-binance ≥1.0.28 targets the new /api/v3/orderList/oco endpoint
        # which uses aboveType/belowType instead of the old stopPrice/stopLimitPrice flat params.
        # SELL bracket (exit LONG): TP limit is above price, SL stop-limit is below.
        # BUY  bracket (exit SHORT): SL stop-limit is above price, TP limit is below.
        base: dict = {
            "symbol":   self.symbol,
            "side":     self.side,
            "quantity": self.quantity,
        }
        if self.side == "SELL":
            base.update({
                "aboveType":        "LIMIT_MAKER",
                "abovePrice":       str(round(self.tp_price, 2)),
                "belowType":        "STOP_LOSS_LIMIT",
                "belowStopPrice":   str(round(self.sl_price, 2)),
                "belowPrice":       str(round(self.sl_limit, 2)),
                "belowTimeInForce": "GTC",
            })
        else:
            base.update({
                "aboveType":        "STOP_LOSS_LIMIT",
                "aboveStopPrice":   str(round(self.sl_price, 2)),
                "abovePrice":       str(round(self.sl_limit, 2)),
                "aboveTimeInForce": "GTC",
                "belowType":        "LIMIT_MAKER",
                "belowPrice":       str(round(self.tp_price, 2)),
            })
        return base
