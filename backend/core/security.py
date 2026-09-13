"""In-process abuse controls: sliding-window rate limits and misuse pauses.

Misuse is classified by the interpretation pipeline (Prompt Guard and the model's
refusal reason), not by keyword patterns. Counters live in this process; a
multi-worker or multi-host deployment must move them to a shared store.
"""
from __future__ import annotations

import math
import threading
import time
from collections import deque

LIMITS: dict[str, tuple[int, float]] = {
    "login_client": (20, 60.0),
    "login_email": (5, 900.0),
    "signup_client": (5, 3600.0),
    "question_user": (20, 60.0),
    "plan_user": (120, 60.0),
    "model_global": (60, 60.0),
    "upload_user": (5, 600.0),
    "configure_user": (20, 600.0),
    "onboarding_user": (20, 600.0),
}
MISUSE_STRIKES = 5
MISUSE_WINDOW_SECONDS = 600.0
MISUSE_BLOCK_SECONDS = 900.0

class RateLimiter:
    def __init__(self, limits: dict[str, tuple[int, float]] | None = None):
        self.limits = dict(LIMITS if limits is None else limits)
        self._events: dict[tuple[str, str], deque[float]] = {}
        self._lock = threading.Lock()

    def hit(self, bucket: str, identity: str, now: float | None = None) -> int | None:
        """Record one attempt; return whole seconds to wait when the bucket is exhausted."""
        limit, window = self.limits[bucket]
        now = time.monotonic() if now is None else now
        with self._lock:
            events = self._events.setdefault((bucket, identity), deque())
            while events and events[0] <= now - window:
                events.popleft()
            if len(events) >= limit:
                return max(1, math.ceil(events[0] + window - now))
            events.append(now)
            if len(self._events) > 50_000:
                stale = [key for key, value in self._events.items() if not value or value[-1] <= now - self.limits[key[0]][1]]
                for key in stale:
                    del self._events[key]
            return None

    def clear(self, bucket: str, identity: str) -> None:
        with self._lock:
            self._events.pop((bucket, identity), None)


class MisuseMonitor:
    """Temporarily pause natural-language questions after repeated refused abuse attempts."""

    def __init__(self, strikes: int = MISUSE_STRIKES, window: float = MISUSE_WINDOW_SECONDS, block: float = MISUSE_BLOCK_SECONDS):
        self.strikes, self.window, self.block = strikes, window, block
        self._strikes: dict[str, deque[float]] = {}
        self._blocked_until: dict[str, float] = {}
        self._lock = threading.Lock()

    def blocked_for(self, identity: str, now: float | None = None) -> int | None:
        now = time.monotonic() if now is None else now
        with self._lock:
            until = self._blocked_until.get(identity)
            if until is None or until <= now:
                self._blocked_until.pop(identity, None)
                return None
            return max(1, math.ceil(until - now))

    def strike(self, identity: str, now: float | None = None) -> bool:
        """Record a refused misuse attempt; return True when it starts a block."""
        now = time.monotonic() if now is None else now
        with self._lock:
            events = self._strikes.setdefault(identity, deque())
            while events and events[0] <= now - self.window:
                events.popleft()
            events.append(now)
            if len(events) >= self.strikes:
                events.clear()
                self._blocked_until[identity] = now + self.block
                return True
            return False
