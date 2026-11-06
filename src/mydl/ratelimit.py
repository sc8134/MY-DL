"""Speed limiting (token bucket, thread-safe). Stdlib only."""

from __future__ import annotations

import threading
import time


def parse_speed(value: str | float | int | None) -> float | None:
    if value is None or value == "":
        return None
    if isinstance(value, (int, float)):
        v = float(value)
        return None if v <= 0 else v
    s = str(value).strip().upper()
    if not s or s == "0":
        return None
    units = {"B": 1, "K": 1024, "KB": 1024, "M": 1024**2, "MB": 1024**2, "G": 1024**3, "GB": 1024**3}
    for u in ("GB", "MB", "KB", "G", "M", "K", "B"):
        if s.endswith(u):
            return float(s[: -len(u)].strip() or 0) * units[u]
    return float(s)


class SpeedLimiter:
    def __init__(self, bytes_per_sec: float | None) -> None:
        self.rate = bytes_per_sec
        self._lock = threading.Lock()
        self._allowance = bytes_per_sec or 0.0
        self._last = time.monotonic()

    def acquire(self, n: int) -> None:
        rate = self.rate
        if not rate or n <= 0:
            return
        while True:
            with self._lock:
                now = time.monotonic()
                elapsed = now - self._last
                self._last = now
                self._allowance = min(rate, self._allowance + elapsed * rate)
                if self._allowance >= n:
                    self._allowance -= n
                    return
                deficit = n - self._allowance
            time.sleep(max(0.001, deficit / rate))
