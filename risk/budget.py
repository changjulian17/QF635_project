from dataclasses import dataclass


@dataclass
class DailyBudget:
    """
    Tracks the daily loss allowance against a fixed DOV (Day Opening Value).
    hard_limit = dov × hard_limit_pct (default 1%).

    Losses are stored as negative numbers in realised_pnl / unrealised_pnl,
    so remaining decreases as losses accumulate.
    """
    dov:            float
    hard_limit:     float
    realised_pnl:   float = 0.0
    unrealised_pnl: float = 0.0

    @classmethod
    def from_equity(cls, equity: float, hard_limit_pct: float = 0.01) -> "DailyBudget":
        return cls(dov=equity, hard_limit=equity * hard_limit_pct)

    @property
    def remaining(self) -> float:
        return self.hard_limit + self.realised_pnl + self.unrealised_pnl

    @property
    def loss_pct(self) -> float:
        return abs(min(self.realised_pnl, 0.0)) / self.dov if self.dov > 0 else 0.0

    def reset(self, new_equity: float, hard_limit_pct: float = 0.01) -> None:
        self.dov            = new_equity
        self.hard_limit     = new_equity * hard_limit_pct
        self.realised_pnl   = 0.0
        self.unrealised_pnl = 0.0
