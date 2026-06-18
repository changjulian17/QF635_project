"""
Unit tests for execution/orders.py — Order domain classes.
"""

import pytest

from execution.orders import FuturesSLOrder, FuturesTPOrder, IOCLimitOrder, Order


# ── 1. IOCLimitOrder ──────────────────────────────────────────────────────────

def test_ioc_limit_order_to_entry_params_buy():
    order = IOCLimitOrder(symbol="BTCUSDT", side="BUY", quantity=0.001, price=95_015.50)
    params = order.to_entry_params()
    assert params["symbol"]      == "BTCUSDT"
    assert params["side"]        == "BUY"
    assert params["type"]        == "LIMIT"
    assert params["timeInForce"] == "IOC"
    assert params["quantity"]    == 0.001
    assert params["price"]       == "95015.5"


def test_ioc_limit_order_to_entry_params_sell():
    order = IOCLimitOrder(symbol="BTCUSDT", side="SELL", quantity=0.002, price=94_994.75)
    params = order.to_entry_params()
    assert params["side"]  == "SELL"
    assert params["price"] == "94994.8"


def test_ioc_limit_order_price_rounded_to_1dp():
    order = IOCLimitOrder(symbol="BTCUSDT", side="BUY", quantity=0.001, price=95_000.123456)
    assert order.to_entry_params()["price"] == "95000.1"


def test_ioc_limit_order_is_order_subclass():
    order = IOCLimitOrder(symbol="BTCUSDT", side="BUY", quantity=0.001, price=100.0)
    assert isinstance(order, Order)


# ── 2. FuturesTPOrder ─────────────────────────────────────────────────────────

def test_futures_tp_order_sell_exit():
    """TP for a LONG position: SELL TAKE_PROFIT when price reaches stop_price."""
    order = FuturesTPOrder(
        symbol      = "BTCUSDT",
        side        = "SELL",
        quantity    = 0.001,
        stop_price  = 96_000.0,
        limit_price = 96_000.0,
    )
    params = order.to_entry_params()
    assert params["symbol"]      == "BTCUSDT"
    assert params["side"]        == "SELL"
    assert params["type"]        == "TAKE_PROFIT"
    assert params["timeInForce"] == "GTC"
    assert params["stopPrice"]   == "96000.0"
    assert params["price"]       == "96000.0"
    assert params["reduceOnly"]  == "true"


def test_futures_tp_order_buy_exit():
    """TP for a SHORT position: BUY TAKE_PROFIT when price drops to stop_price."""
    order = FuturesTPOrder(
        symbol      = "BTCUSDT",
        side        = "BUY",
        quantity    = 0.001,
        stop_price  = 94_000.0,
        limit_price = 94_000.0,
    )
    params = order.to_entry_params()
    assert params["side"]       == "BUY"
    assert params["type"]       == "TAKE_PROFIT"
    assert params["stopPrice"]  == "94000.0"
    assert params["reduceOnly"] == "true"


def test_futures_tp_order_prices_rounded():
    order = FuturesTPOrder(
        symbol="BTCUSDT", side="SELL", quantity=0.001,
        stop_price=96_000.1234, limit_price=96_000.1234,
    )
    params = order.to_entry_params()
    assert params["stopPrice"] == "96000.1"
    assert params["price"]     == "96000.1"


def test_futures_tp_order_is_order_subclass():
    order = FuturesTPOrder(symbol="BTCUSDT", side="SELL", quantity=0.001,
                           stop_price=96_000.0, limit_price=96_000.0)
    assert isinstance(order, Order)


# ── 3. FuturesSLOrder ─────────────────────────────────────────────────────────

def test_futures_sl_order_sell_exit():
    """SL for a LONG position: SELL STOP when price falls to stop_price."""
    order = FuturesSLOrder(
        symbol      = "BTCUSDT",
        side        = "SELL",
        quantity    = 0.001,
        stop_price  = 94_000.0,
        limit_price = 93_906.0,
    )
    params = order.to_entry_params()
    assert params["symbol"]      == "BTCUSDT"
    assert params["side"]        == "SELL"
    assert params["type"]        == "STOP"
    assert params["timeInForce"] == "GTC"
    assert params["stopPrice"]   == "94000.0"
    assert params["price"]       == "93906.0"
    assert params["reduceOnly"]  == "true"


def test_futures_sl_order_buy_exit():
    """SL for a SHORT position: BUY STOP when price rises to stop_price."""
    order = FuturesSLOrder(
        symbol      = "BTCUSDT",
        side        = "BUY",
        quantity    = 0.001,
        stop_price  = 96_000.0,
        limit_price = 96_096.0,
    )
    params = order.to_entry_params()
    assert params["side"]       == "BUY"
    assert params["type"]       == "STOP"
    assert params["stopPrice"]  == "96000.0"
    assert params["price"]      == "96096.0"
    assert params["reduceOnly"] == "true"


def test_futures_sl_order_prices_rounded():
    order = FuturesSLOrder(
        symbol="BTCUSDT", side="SELL", quantity=0.001,
        stop_price=94_000.5678, limit_price=93_906.9999,
    )
    params = order.to_entry_params()
    assert params["stopPrice"] == "94000.6"
    assert params["price"]     == "93907.0"


def test_futures_sl_order_is_order_subclass():
    order = FuturesSLOrder(symbol="BTCUSDT", side="SELL", quantity=0.001,
                           stop_price=94_000.0, limit_price=93_906.0)
    assert isinstance(order, Order)


# ── 4. Order base is abstract ─────────────────────────────────────────────────

def test_order_base_cannot_be_instantiated():
    with pytest.raises(TypeError):
        Order(symbol="BTCUSDT", side="BUY", quantity=0.001)  # type: ignore[abstract]


# ── 5. Polymorphic dispatch via to_entry_params ───────────────────────────────

def test_polymorphic_dispatch():
    """A list of heterogeneous Order subclasses each produce distinct param dicts."""
    orders: list[Order] = [
        IOCLimitOrder(symbol="BTCUSDT", side="BUY",  quantity=0.001, price=95_015.0),
        FuturesTPOrder(symbol="BTCUSDT", side="SELL", quantity=0.001,
                       stop_price=96_000.0, limit_price=96_000.0),
        FuturesSLOrder(symbol="BTCUSDT", side="SELL", quantity=0.001,
                       stop_price=94_000.0, limit_price=93_906.0),
    ]
    param_sets = [o.to_entry_params() for o in orders]

    # IOC limit: timeInForce=IOC, no stopPrice
    assert param_sets[0]["timeInForce"] == "IOC"
    assert "stopPrice" not in param_sets[0]

    # TP: type=TAKE_PROFIT, reduceOnly
    assert param_sets[1]["type"]       == "TAKE_PROFIT"
    assert param_sets[1]["reduceOnly"] == "true"

    # SL: type=STOP, reduceOnly
    assert param_sets[2]["type"]       == "STOP"
    assert param_sets[2]["reduceOnly"] == "true"
