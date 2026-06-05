"""Position-sizing helpers shared by the strategy executor and the execution layer."""


def clamp_stop_bps(raw_bps: float, min_bps: float, max_bps: float) -> float:
    """Clamp a stop distance (in bps) into [min_bps, max_bps].

    The floor is the important half: position size is ``risk / stop_distance``, so a
    near-zero stop (a protection wall sitting ~at mid) would otherwise produce an
    unbounded notional. Flooring the distance bounds the size. The cap mirrors the
    existing PROTECTION_MAX_DISTANCE_BPS behaviour.
    """
    return min(max(raw_bps, min_bps), max_bps)


def cap_risk_fraction(notional_hint: float, equity: float, budget_remaining: float) -> float:
    """Cap the per-trade risk fraction so the dollar risk (``equity × notional_hint``)
    never exceeds the remaining daily-loss budget.

    Gate 3 only checks ``budget.remaining > 0`` (binary), so near budget exhaustion a
    full-size trade could risk more than the allowance left. This clamps the risk
    fraction to ``budget_remaining / equity``. ``budget_remaining`` may be ``inf``
    (no budget configured) — a no-op in that case.
    """
    if equity <= 0 or budget_remaining == float("inf"):
        return notional_hint
    max_frac = max(budget_remaining, 0.0) / equity
    return min(notional_hint, max_frac)
