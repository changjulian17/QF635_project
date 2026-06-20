"""Binance diff-depth seed continuity checks (shared by ws_consumer + lob_recorder).

Spot seeding (per Binance Spot spec): after a REST snapshot with lastUpdateId, the first
applied diff must bridge it (U <= lastUpdateId+1 <= u) and every subsequent diff must be
contiguous (each U == previous u + 1).

USD-M Futures seeding follows a different spec — the bridge omits the +1 and contiguity is
checked via the event's `pu` (previous final update id) rather than `U`:
    bridge:      U <= lastUpdateId <= u
    contiguity:  event.pu == previous event.u
Use the futures_* variants on the @depth futures stream.

In either case a violation means events were missed — the local book has a gap and must be
reseeded rather than trusted.
"""
import asyncio
import time
from collections.abc import Callable


class SeedDiscontinuity(Exception):
    """Raised when buffered diffs don't form a gapless bridge from the snapshot."""


def seed_bridge_ok(first_U: int, first_u: int, last_update_id: int) -> bool:
    """First applied diff must straddle lastUpdateId+1."""
    return first_U <= last_update_id + 1 <= first_u


def is_contiguous(prev_u: int, next_U: int) -> bool:
    """Successive diffs must be gapless: each U == previous u + 1."""
    return next_U == prev_u + 1


def futures_seed_bridge_ok(first_U: int, first_u: int, last_update_id: int) -> bool:
    """USD-M Futures: first applied diff must straddle lastUpdateId (no +1)."""
    return first_U <= last_update_id <= first_u


def futures_is_contiguous(prev_u: int, next_pu: int) -> bool:
    """USD-M Futures: each event's `pu` must equal the previous event's `u`."""
    return next_pu == prev_u


async def seed_futures_book(
    *,
    last_update_id: int,
    pending: list[dict],
    apply_diff: Callable[[dict], None],
    bridge_wait_s: float,
    poll_s: float,
) -> int:
    """Wait for a bridge diff, then apply the bridge + pu-chain from `pending`.

    USD-M Futures: the snapshot's lastUpdateId is *between* events, so a real bridge
    event (U <= lastUpdateId <= u) must be applied before the book is trusted — then the
    live `pu` chain validates. Returns the last applied event `u` (a real id, never the
    snapshot id). Raises SeedDiscontinuity if no bridge arrives in time or the chain gaps.

    The wait gate uses strict `u > last_update_id` to match the apply-loop skip
    `u <= last_update_id`, so a snapshot landing exactly on an event boundary cannot pass
    the gate and then be skipped (the prior `>=`/`<=` boundary race).

    `pending` is read live: callers pass the same buffer the receive loop keeps appending
    to while UNSYNCED, so the wait observes diffs as they arrive. `apply_diff` mutates the
    caller's book (already rebuilt from the snapshot before this call).
    """
    deadline = time.monotonic() + bridge_wait_s
    while not any(int(d.get("u", 0)) > last_update_id for d in pending):   # strict >
        if time.monotonic() >= deadline:
            raise SeedDiscontinuity(
                f"no diff reached lastUpdateId={last_update_id} within {bridge_wait_s}s"
            )
        await asyncio.sleep(poll_s)

    prev_u, bridged = last_update_id, False
    for event in pending:
        u = int(event.get("u", 0))
        if u <= last_update_id:
            continue
        U = int(event.get("U", 0))
        if not bridged:
            if not futures_seed_bridge_ok(U, u, last_update_id):
                raise SeedDiscontinuity(f"bridge fail U={U} u={u} lastUpdateId={last_update_id}")
            bridged = True
        else:
            pu = int(event.get("pu", 0))
            if not futures_is_contiguous(prev_u, pu):
                raise SeedDiscontinuity(f"gap pu={pu} != prev_u={prev_u}")
        apply_diff(event)
        prev_u = u

    if not bridged:   # defensive — unreachable now the gate is strict `>`
        raise SeedDiscontinuity(f"no bridge event for lastUpdateId={last_update_id}")
    return prev_u
