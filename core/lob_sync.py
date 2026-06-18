"""Binance diff-depth seed continuity checks (shared by ws_consumer + lob_recorder).

Correct seeding (per Binance spec): after a REST snapshot with lastUpdateId, the first
applied diff must bridge it (U <= lastUpdateId+1 <= u) and every subsequent diff must be
contiguous (each U == previous u + 1). A violation means events were missed — the local
book has a gap and must be reseeded rather than trusted.
"""


class SeedDiscontinuity(Exception):
    """Raised when buffered diffs don't form a gapless bridge from the snapshot."""


def seed_bridge_ok(first_U: int, first_u: int, last_update_id: int) -> bool:
    """First applied diff must straddle lastUpdateId+1."""
    return first_U <= last_update_id + 1 <= first_u


def is_contiguous(prev_u: int, next_U: int) -> bool:
    """Successive diffs must be gapless: each U == previous u + 1."""
    return next_U == prev_u + 1
