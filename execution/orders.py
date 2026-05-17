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
    sl_limit: float = 0.0    # stopLimitPrice

    def to_entry_params(self) -> dict:
        return {
            "symbol":               self.symbol,
            "side":                 self.side,
            "quantity":             self.quantity,
            "price":                str(round(self.tp_price, 2)),
            "stopPrice":            str(round(self.sl_price, 2)),
            "stopLimitPrice":       str(round(self.sl_limit, 2)),
            "stopLimitTimeInForce": "GTC",
        }
