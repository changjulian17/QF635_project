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
