"""
Unit tests for execution/orders.py — Order domain classes.
"""

import pytest

from execution.orders import IOCLimitOrder, OCOOrder, Order


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
    assert params["price"] == "94994.75"


def test_ioc_limit_order_price_rounded_to_2dp():
    order = IOCLimitOrder(symbol="BTCUSDT", side="BUY", quantity=0.001, price=95_000.123456)
    assert order.to_entry_params()["price"] == "95000.12"


def test_ioc_limit_order_is_order_subclass():
    order = IOCLimitOrder(symbol="BTCUSDT", side="BUY", quantity=0.001, price=100.0)
    assert isinstance(order, Order)


# ── 2. OCOOrder ───────────────────────────────────────────────────────────────

def test_oco_order_to_entry_params_sell_exit():
    """Closing a LONG: exit side is SELL, TP above fill (aboveType=LIMIT_MAKER), SL below (belowType=STOP_LOSS_LIMIT)."""
    order = OCOOrder(
        symbol   = "BTCUSDT",
        side     = "SELL",
        quantity = 0.001,
        tp_price = 96_000.0,
        sl_price = 94_000.0,
        sl_limit = 93_906.0,
    )
    params = order.to_entry_params()
    assert params["symbol"]           == "BTCUSDT"
    assert params["side"]             == "SELL"
    assert params["quantity"]         == 0.001
    assert params["aboveType"]        == "LIMIT_MAKER"
    assert params["abovePrice"]       == "96000.0"   # TP limit
    assert params["belowType"]        == "STOP_LOSS_LIMIT"
    assert params["belowStopPrice"]   == "94000.0"   # SL trigger
    assert params["belowPrice"]       == "93906.0"   # SL limit (inside)
    assert params["belowTimeInForce"] == "GTC"


def test_oco_order_to_entry_params_buy_exit():
    """Closing a SHORT: exit side is BUY, SL above fill (aboveType=STOP_LOSS_LIMIT), TP below (belowType=LIMIT_MAKER)."""
    order = OCOOrder(
        symbol   = "BTCUSDT",
        side     = "BUY",
        quantity = 0.001,
        tp_price = 94_000.0,
        sl_price = 96_000.0,
        sl_limit = 96_096.0,
    )
    params = order.to_entry_params()
    assert params["side"]             == "BUY"
    assert params["aboveType"]        == "STOP_LOSS_LIMIT"
    assert params["aboveStopPrice"]   == "96000.0"   # SL trigger
    assert params["abovePrice"]       == "96096.0"   # SL limit (inside)
    assert params["aboveTimeInForce"] == "GTC"
    assert params["belowType"]        == "LIMIT_MAKER"
    assert params["belowPrice"]       == "94000.0"   # TP limit


def test_oco_order_prices_rounded_to_2dp_sell():
    order = OCOOrder(
        symbol="BTCUSDT", side="SELL", quantity=0.001,
        tp_price=96_000.1234, sl_price=94_000.5678, sl_limit=93_906.9999,
    )
    params = order.to_entry_params()
    assert params["abovePrice"]     == "96000.12"
    assert params["belowStopPrice"] == "94000.57"
    assert params["belowPrice"]     == "93907.0"


def test_oco_order_prices_rounded_to_2dp_buy():
    order = OCOOrder(
        symbol="BTCUSDT", side="BUY", quantity=0.001,
        tp_price=93_900.1234, sl_price=96_000.5678, sl_limit=96_096.9999,
    )
    params = order.to_entry_params()
    assert params["aboveStopPrice"] == "96000.57"
    assert params["abovePrice"]     == "96097.0"
    assert params["belowPrice"]     == "93900.12"


def test_oco_order_is_order_subclass():
    order = OCOOrder(
        symbol="BTCUSDT", side="SELL", quantity=0.001,
        tp_price=96_000.0, sl_price=94_000.0, sl_limit=93_906.0,
    )
    assert isinstance(order, Order)


# ── 3. Order base is abstract ─────────────────────────────────────────────────

def test_order_base_cannot_be_instantiated():
    with pytest.raises(TypeError):
        Order(symbol="BTCUSDT", side="BUY", quantity=0.001)  # type: ignore[abstract]


# ── 4. Polymorphic dispatch via to_entry_params ───────────────────────────────

def test_polymorphic_dispatch():
    """A list of heterogeneous Order subclasses each produce distinct param dicts."""
    orders: list[Order] = [
        IOCLimitOrder(symbol="BTCUSDT", side="BUY",  quantity=0.001, price=95_015.0),
        OCOOrder(symbol="BTCUSDT", side="SELL", quantity=0.001,
                 tp_price=96_000.0, sl_price=94_000.0, sl_limit=93_906.0),
    ]
    param_sets = [o.to_entry_params() for o in orders]

    # IOC limit: has timeInForce=IOC
    assert param_sets[0]["timeInForce"] == "IOC"
    assert "stopPrice" not in param_sets[0]

    # OCO: has aboveType/belowType, no top-level timeInForce
    assert "aboveType" in param_sets[1]
    assert "belowType" in param_sets[1]
    assert "timeInForce" not in param_sets[1]
