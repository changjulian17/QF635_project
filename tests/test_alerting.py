"""Tests for core/alerting.py — AlertDispatcher."""
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from core.alerting import AlertDispatcher


def test_no_request_when_url_empty():
    """AlertDispatcher with empty URL must never make an HTTP request."""
    dispatcher = AlertDispatcher("")

    with patch("aiohttp.ClientSession") as mock_session_cls:
        asyncio.run(dispatcher.notify_killswitch("TEST", 10_000.0))
        mock_session_cls.assert_not_called()


def test_post_on_killswitch():
    """notify_killswitch must POST with correct event type and fields."""
    dispatcher = AlertDispatcher("http://localhost:9999/hook")

    mock_response = MagicMock()
    mock_post = AsyncMock(return_value=mock_response)
    mock_session = MagicMock()
    mock_session.__aenter__ = AsyncMock(return_value=mock_session)
    mock_session.__aexit__ = AsyncMock(return_value=False)
    mock_session.post = mock_post

    with patch("aiohttp.ClientSession", return_value=mock_session):
        asyncio.run(dispatcher.notify_killswitch("KILLSWITCH_BUDGET", 9_500.0))

    mock_post.assert_called_once()
    _, kwargs = mock_post.call_args
    body = kwargs["json"]
    assert body["event"] == "KILLSWITCH_FIRED"
    assert body["reason"] == "KILLSWITCH_BUDGET"
    assert body["equity"] == 9_500.0
    assert "ts" in body


def test_tier_recovery_does_not_alert():
    """notify_tier_change from a worse tier back to FULL must not POST."""
    dispatcher = AlertDispatcher("http://localhost:9999/hook")

    with patch("aiohttp.ClientSession") as mock_session_cls:
        asyncio.run(dispatcher.notify_tier_change("MINIMAL", "FULL", 0.003))
        mock_session_cls.assert_not_called()
