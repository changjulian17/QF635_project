"""
PyramidController — manages up to 3 legs of a position.

Leg 2 only if Leg 1 is profitable.
Leg 3 only if Legs 1 + 2 are combined profitable.
"""
import logging

logger = logging.getLogger(__name__)

_MAX_LEGS    = 3
_LEG_SCALARS = {1: 1.0, 2: 0.5, 3: 0.25}


class PyramidController:

    def __init__(self) -> None:
        self._legs: list[dict] = []  # each: {"qty": float, "entry": float, "direction": str}

    @property
    def leg_count(self) -> int:
        return len(self._legs)

    @property
    def max_legs(self) -> int:
        return _MAX_LEGS

    def can_add_leg(self, current_price: float) -> tuple[bool, str]:
        """Return (True, "") if a new leg is permitted; (False, reason) otherwise."""
        if self.leg_count >= _MAX_LEGS:
            return False, f"max legs ({_MAX_LEGS}) reached"

        next_leg = self.leg_count + 1

        if next_leg == 2:
            if self._unrealised_pnl_leg(0, current_price) <= 0:
                return False, "Leg 1 not yet profitable"

        if next_leg == 3:
            combined = sum(self._unrealised_pnl_leg(i, current_price) for i in range(len(self._legs)))
            if combined <= 0:
                return False, "open legs combined not profitable"

        return True, ""

    def leg_scalar(self) -> float:
        """Fraction of base Leg-1 size for the next leg."""
        return _LEG_SCALARS.get(self.leg_count + 1, 0.0)

    def open_leg(self, qty: float, entry_price: float, direction: str = "LONG") -> None:
        if len(self._legs) >= _MAX_LEGS:
            raise RuntimeError(f"Pyramid full ({_MAX_LEGS} legs) — call close_leg first")
        self._legs.append({"qty": qty, "entry": entry_price, "direction": direction})
        logger.info("[Pyramid] Leg %d opened qty=%.6f @ %.2f dir=%s",
                    self.leg_count, qty, entry_price, direction)

    def close_leg(self) -> None:
        """Close the oldest open leg (FIFO — legs settle in entry order)."""
        if self._legs:
            self._legs.pop(0)
            logger.info("[Pyramid] Leg closed. Remaining: %d", self.leg_count)

    def close_all(self) -> None:
        self._legs.clear()

    def _unrealised_pnl_leg(self, idx: int, current_price: float) -> float:
        leg = self._legs[idx]
        if leg["direction"] == "LONG":
            return leg["qty"] * (current_price - leg["entry"])
        return leg["qty"] * (leg["entry"] - current_price)
