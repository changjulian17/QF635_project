"""
AlertDispatcher — fire-and-forget webhook notifications for killswitch and tier events.

All errors are suppressed with a warning log so the dispatcher never crashes the engine.
Set ALERT_WEBHOOK_URL in .env to enable; leave empty to disable.
"""

import datetime
import logging

import aiohttp

logger = logging.getLogger(__name__)

_DEGRADATION_ORDER = ("FULL", "REDUCED", "MINIMAL", "PASSIVE", "HALTED")


class AlertDispatcher:

    def __init__(self, webhook_url: str) -> None:
        self._url = webhook_url.strip()

    async def notify(self, event_type: str, payload: dict) -> None:
        if not self._url:
            return
        body = {
            "event": event_type,
            "ts": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            **payload,
        }
        try:
            timeout = aiohttp.ClientTimeout(total=5)
            async with aiohttp.ClientSession() as session:
                await session.post(self._url, json=body, timeout=timeout)
        except Exception:
            logger.warning("[Alert] Webhook dispatch failed — suppressed")

    async def notify_killswitch(self, reason: str, equity: float) -> None:
        await self.notify("KILLSWITCH_FIRED", {"reason": reason, "equity": equity})

    async def notify_tier_change(self, old_tier: str, new_tier: str, loss_pct: float) -> None:
        try:
            old_idx = _DEGRADATION_ORDER.index(old_tier)
            new_idx = _DEGRADATION_ORDER.index(new_tier)
        except ValueError:
            return
        if new_idx > old_idx:
            await self.notify(
                "TIER_DEGRADED",
                {"from": old_tier, "to": new_tier, "loss_pct": loss_pct},
            )
