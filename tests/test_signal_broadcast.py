"""Tests for the signal realtime stream: _record_to_event_payload + SignalTelemetry broadcast."""
import asyncio
import os
import sys
from unittest.mock import AsyncMock, MagicMock

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


# ── _record_to_event_payload ──────────────────────────────────────────────


def test_event_payload_shape():
    """The WS payload must carry the fields the dashboard tape renders."""
    from core.signal_telemetry import SignalRecord, _record_to_event_payload
    record = SignalRecord(
        signal_id="abc-123",
        timestamp="2026-06-01T17:00:00+00:00",
        micro_signal="SWEEP_WITH_PROTECTION",
        gate_passed="GATE_2_FAIL",
        rejection_reason="confidence below 0.55",
        direction="LONG",
        confidence=0.42,
    )
    payload = _record_to_event_payload(record)
    assert payload == {
        "type":             "signal_event",
        "ts":               "2026-06-01T17:00:00+00:00",
        "signal_id":        "abc-123",
        "micro_signal":     "SWEEP_WITH_PROTECTION",
        "gate_passed":      "GATE_2_FAIL",
        "rejection_reason": "confidence below 0.55",
        "direction":        "LONG",
        "confidence":       0.42,
    }


def test_event_payload_handles_approved_with_no_rejection_reason():
    """Approved signals have empty rejection_reason — the field is still serialised."""
    from core.signal_telemetry import SignalRecord, _record_to_event_payload
    record = SignalRecord(
        signal_id="ok-1", gate_passed="APPROVED", direction="SHORT",
        confidence=0.78, rejection_reason="",
    )
    payload = _record_to_event_payload(record)
    assert payload["gate_passed"]      == "APPROVED"
    assert payload["rejection_reason"] == ""


# ── SignalTelemetry._drain_loop broadcasts ────────────────────────────────


@pytest.mark.asyncio
async def test_telemetry_broadcasts_on_dequeue(tmp_path):
    """When a record is dequeued, the hub receives a typed signal_event payload."""
    from core.signal_telemetry import SignalTelemetry, SignalRecord

    queue     = asyncio.Queue()
    hub       = MagicMock()
    hub.broadcast = AsyncMock()
    telemetry = SignalTelemetry(queue, db_path=str(tmp_path / "tel.db"), hub=hub)
    telemetry._conn = telemetry._open_db()

    record = SignalRecord(
        signal_id="t1", micro_signal="SWEEP_WITH_PROTECTION",
        gate_passed="GATE_2_FAIL", direction="LONG", confidence=0.42,
        rejection_reason="confidence below threshold",
    )
    await queue.put(record)

    task = asyncio.create_task(telemetry._drain_loop())
    await asyncio.sleep(0.15)   # one iteration is plenty
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass

    hub.broadcast.assert_awaited_once()
    payload = hub.broadcast.await_args[0][0]
    assert payload["type"]        == "signal_event"
    assert payload["signal_id"]   == "t1"
    assert payload["gate_passed"] == "GATE_2_FAIL"
    assert payload["direction"]   == "LONG"

    await telemetry.close()


@pytest.mark.asyncio
async def test_telemetry_no_hub_runs_normally(tmp_path):
    """Without a hub the drain loop still buffers and flushes — no crash."""
    from core.signal_telemetry import SignalTelemetry, SignalRecord

    queue     = asyncio.Queue()
    telemetry = SignalTelemetry(queue, db_path=str(tmp_path / "tel.db"), hub=None)
    telemetry._conn = telemetry._open_db()

    # Non-APPROVED gate: APPROVED records flush to DB immediately (phase-3 behavior),
    # emptying _buf — here we assert the record lands in the buffer.
    await queue.put(SignalRecord(signal_id="t1", gate_passed="GATE_2_FAIL"))

    task = asyncio.create_task(telemetry._drain_loop())
    await asyncio.sleep(0.15)
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass

    assert len(telemetry._buf) == 1   # record landed in the buffer despite no hub

    await telemetry.close()


@pytest.mark.asyncio
async def test_telemetry_hub_failure_does_not_stop_persistence(tmp_path):
    """A failing hub.broadcast must not abort the drain loop — telemetry persistence
    is critical and must survive a flaky subscriber."""
    from core.signal_telemetry import SignalTelemetry, SignalRecord

    queue     = asyncio.Queue()
    hub       = MagicMock()
    hub.broadcast = AsyncMock(side_effect=RuntimeError("subscriber went away"))
    telemetry = SignalTelemetry(queue, db_path=str(tmp_path / "tel.db"), hub=hub)
    telemetry._conn = telemetry._open_db()

    # Non-APPROVED gate: APPROVED records flush to DB immediately (phase-3 behavior),
    # emptying _buf — here we assert the record lands in the buffer.
    await queue.put(SignalRecord(signal_id="t1", gate_passed="GATE_2_FAIL"))

    task = asyncio.create_task(telemetry._drain_loop())
    await asyncio.sleep(0.15)
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass

    # Hub failed, but the record still made it into the buffer.
    assert len(telemetry._buf) == 1
    hub.broadcast.assert_awaited_once()

    await telemetry.close()


# ── update_signal_tape ─────────────────────────────────────────────────────


def _event(ts, gate="GATE_2_FAIL", direction="LONG"):
    return {"type": "signal_event", "ts": ts, "gate_passed": gate, "direction": direction}


def test_tape_appends_newest_first():
    """Tape buffer is ordered newest-first so the UI can slice the head without reversing."""
    from dashboard._logic import update_signal_tape
    buf = update_signal_tape([],  _event("t1"), max_events=10)
    buf = update_signal_tape(buf, _event("t2"), max_events=10)
    buf = update_signal_tape(buf, _event("t3"), max_events=10)
    assert [e["ts"] for e in buf] == ["t3", "t2", "t1"]


def test_tape_trims_to_max_keeping_newest():
    """Old events drop off the tail when the cap is hit."""
    from dashboard._logic import update_signal_tape
    buf = []
    for i in range(5):
        buf = update_signal_tape(buf, _event(f"t{i}"), max_events=3)
    assert [e["ts"] for e in buf] == ["t4", "t3", "t2"]


def test_tape_rejects_non_signal_messages():
    """Snapshot / portfolio / malformed messages pass through unchanged."""
    from dashboard._logic import update_signal_tape
    buf = [_event("t1")]
    assert update_signal_tape(buf, {"type": "snapshot", "ts": "t2"}, max_events=10) is buf
    assert update_signal_tape(buf, {"type": "portfolio", "ts": "t2"}, max_events=10) is buf
    assert update_signal_tape(buf, "not a dict", max_events=10) is buf


def test_tape_rejects_payloads_missing_required_fields():
    """gate_passed and ts are both required — missing either → reject."""
    from dashboard._logic import update_signal_tape
    buf = [_event("t1")]
    assert update_signal_tape(buf, {"type": "signal_event", "ts": "t2"}, max_events=10) is buf  # no gate
    assert update_signal_tape(buf, {"type": "signal_event", "gate_passed": "G"}, max_events=10) is buf  # no ts
