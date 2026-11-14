import asyncio
from collections import deque
from math import isfinite
from random import uniform
from time import perf_counter


class RateLimiter:
    def __init__(
        self,
        requests_per_second: float = 1.0,
        per_domain: bool = True,
        *,
        min_delay: float = 0.0,
        jitter: float = 0.0,
    ) -> None:
        if not isfinite(requests_per_second) or requests_per_second <= 0:
            raise ValueError("requests_per_second должен быть положительным")
        if any(not isfinite(value) or value < 0 for value in (min_delay, jitter)):
            raise ValueError("min_delay и jitter должны быть неотрицательными")

        self._interval = max(1.0 / requests_per_second, min_delay)
        if not isfinite(self._interval):
            raise ValueError("Интервал между запросами должен быть конечным")

        self._per_domain = per_domain
        self._jitter = jitter
        self._lock = asyncio.Lock()
        self._next_start: dict[str | None, float] = {}
        self._domain_starts: dict[str | None, tuple[float, float, float]] = {}
        self._recent_starts: deque[float] = deque()
        self._total = 0
        self._first_start = 0.0
        self._last_start = 0.0

    async def acquire(
        self, domain: str | None = None, *, crawl_delay: float = 0.0
    ) -> None:
        """Дождаться времени старта запроса для домена или общего лимита."""
        if not isfinite(crawl_delay) or crawl_delay < 0:
            raise ValueError("crawl_delay должен быть неотрицательным")
        if domain is not None:
            domain = domain.lower().rstrip(".")
        key = domain if self._per_domain else None

        while True:
            async with self._lock:
                now = perf_counter()
                last_start, last_delay, last_jitter = self._domain_starts.get(
                    domain, (float("-inf"), 0.0, 0.0)
                )
                ready_at = max(
                    self._next_start.get(key, now),
                    last_start + max(last_delay, crawl_delay) + last_jitter,
                )
                if ready_at <= now:
                    jitter = uniform(0, self._jitter)
                    self._next_start[key] = now + self._interval + jitter
                    self._domain_starts[domain] = (now, crawl_delay, jitter)
                    if self._total == 0:
                        self._first_start = now
                    self._last_start = now
                    self._total += 1
                    self._recent_starts.append(now)
                    self._drop_old_starts(now)
                    return

            await asyncio.sleep(ready_at - now)

    def _drop_old_starts(self, now: float) -> None:
        while self._recent_starts and self._recent_starts[0] <= now - 1:
            self._recent_starts.popleft()

    def get_stats(self) -> dict[str, float | int]:
        self._drop_old_starts(perf_counter())
        intervals = self._total - 1
        return {
            "requests_per_second": len(self._recent_starts),
            "average_delay": (self._last_start - self._first_start) / intervals
            if intervals > 0
            else 0.0,
            "requests_total": self._total,
        }
