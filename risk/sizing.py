"""Position-sizing helpers shared by the strategy executor and the execution layer."""


def clamp_stop_bps(raw_bps: float, min_bps: float, max_bps: float) -> float:
    """Clamp a stop distance (in bps) into [min_bps, max_bps].

    The floor is the important half: position size is ``risk / stop_distance``, so a
    near-zero stop (a protection wall sitting ~at mid) would otherwise produce an
    unbounded notional. Flooring the distance bounds the size. The cap mirrors the
    existing PROTECTION_MAX_DISTANCE_BPS behaviour.
    """
    return min(max(raw_bps, min_bps), max_bps)
