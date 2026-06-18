"""Tests for UserDataStreamConsumer listen-key bootstrap handling."""
import os
import sys
from unittest.mock import AsyncMock, MagicMock

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.user_data_stream import UserDataStreamConsumer, _FATAL, _extract_listen_key


def test_extract_listen_key_accepts_string_response():
    assert _extract_listen_key("abc123") == "abc123"


def test_extract_listen_key_accepts_dict_response():
    assert _extract_listen_key({"listenKey": "xyz789"}) == "xyz789"


def test_extract_listen_key_rejects_invalid_response():
    assert _extract_listen_key({"no_listen_key": "x"}) is None
    assert _extract_listen_key({}) is None
    assert _extract_listen_key(123) is None


@pytest.mark.asyncio
async def test_obtain_listen_key_accepts_string_response():
    client = MagicMock()
    client.futures_stream_get_listen_key = AsyncMock(return_value="listen-key-string")
    consumer = UserDataStreamConsumer(client=client)

    result = await consumer._obtain_listen_key()

    assert result == "listen-key-string"


@pytest.mark.asyncio
async def test_obtain_listen_key_accepts_dict_response():
    client = MagicMock()
    client.futures_stream_get_listen_key = AsyncMock(return_value={"listenKey": "listen-key-dict"})
    consumer = UserDataStreamConsumer(client=client)

    result = await consumer._obtain_listen_key()

    assert result == "listen-key-dict"


@pytest.mark.asyncio
async def test_obtain_listen_key_returns_fatal_on_410():
    client = MagicMock()
    client.futures_stream_get_listen_key = AsyncMock(side_effect=Exception("410 Gone"))
    consumer = UserDataStreamConsumer(client=client)

    result = await consumer._obtain_listen_key()

    assert result is _FATAL
