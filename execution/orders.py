"""
Order domain classes — one class per order shape.

Each subclass wraps the parameters for a specific Binance Futures order type and
exposes them via to_entry_params(), returning the kwargs dict for the
matching AsyncClient method:

  IOCLimitOrder  → client.futures_create_order(**order.to_entry_params())
  FuturesTPOrder → client.futures_create_order(**order.to_entry_params())
  FuturesSLOrder → client.futures_create_order(**order.to_entry_params())

Adding a new order type requires only a new subclass — OrderManager is not modified.
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
            "price":       str(round(self.price, 1)),
        }


@dataclass
class FuturesTPOrder(Order):
    """
    Futures take-profit limit bracket order (reduceOnly). type=TAKE_PROFIT.

    Uses the standard /fapi/v1/order endpoint (not the algo endpoint), so it
    returns a numeric orderId that can be cancelled with futures_cancel_order.
    sl_limit is set slightly inside stop_price to guarantee a fill on trigger:
      SELL TP (exit LONG): limit_price == stop_price (price is above market)
      BUY  TP (exit SHORT): limit_price == stop_price (price is below market)
    """
    stop_price:  float = 0.0
    limit_price: float = 0.0

    def to_entry_params(self) -> dict:
        return {
            "symbol":      self.symbol,
            "side":        self.side,
            "type":        "TAKE_PROFIT",
            "timeInForce": "GTC",
            "quantity":    self.quantity,
            "price":       str(round(self.limit_price, 1)),
            "stopPrice":   str(round(self.stop_price, 1)),
            "reduceOnly":  "true",
        }


@dataclass
class FuturesSLOrder(Order):
    """
    Futures stop-loss limit bracket order (reduceOnly). type=STOP.

    Uses the standard /fapi/v1/order endpoint (not the algo endpoint), so it
    returns a numeric orderId that can be cancelled with futures_cancel_order.
    limit_price is set slightly inside stop_price to fill immediately on trigger:
      SELL SL (exit LONG): limit_price = stop_price × 0.999
      BUY  SL (exit SHORT): limit_price = stop_price × 1.001
    """
    stop_price:  float = 0.0
    limit_price: float = 0.0

    def to_entry_params(self) -> dict:
        return {
            "symbol":      self.symbol,
            "side":        self.side,
            "type":        "STOP",
            "timeInForce": "GTC",
            "quantity":    self.quantity,
            "price":       str(round(self.limit_price, 1)),
            "stopPrice":   str(round(self.stop_price, 1)),
            "reduceOnly":  "true",
        }
