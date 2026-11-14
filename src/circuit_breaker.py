from collections import deque
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from math import isfinite
from time import monotonic
from urllib.parse import urlsplit

from src.errors import CircuitOpenError, NetworkError, PermanentError, TransientError


@dataclass
class _DomainState:
    results: deque[tuple[float, bool]] = field(default_factory=deque)
    opened_until: float = 0.0
    probing: bool = False


class CircuitBreaker:
    def __init__(
        self,
        min_requests: int = 5,
        failure_ratio: float = 0.5,
        window: float = 60.0,
        recovery_timeout: float = 30.0,
    ) -> None:
        if min_requests < 1 or not 0 < failure_ratio <= 1:
            raise ValueError("Неверный порог circuit breaker")
        if any(
            not isfinite(value) or value <= 0 for value in (window, recovery_timeout)
        ):
            raise ValueError("Интервалы circuit breaker должны быть положительными")
        self._min_requests = min_requests
        self._failure_ratio = failure_ratio
        self._window = window
        self._recovery_timeout = recovery_timeout
        self._domains: dict[str, _DomainState] = {}

    @asynccontextmanager
    async def request(self, url: str) -> AsyncIterator[None]:
        domain = (urlsplit(url).hostname or "").rstrip(".")
        state = self._domains.setdefault(domain, _DomainState())
        now = monotonic()
        if state.probing or state.opened_until > now:
            raise CircuitOpenError("Домен временно отключён circuit breaker", url=url)
        probe = state.opened_until != 0
        state.probing = probe
        failed = False
        completed = False
        try:
            yield
        except (TransientError, NetworkError):
            failed = True
            completed = True
            raise
        except PermanentError as error:
            completed = error.status is not None
            raise
        else:
            completed = True
        finally:
            if completed:
                now = monotonic()
                if probe:
                    state.results.clear()
                    state.opened_until = now + self._recovery_timeout if failed else 0.0
                state.results.append((now, failed))
                while state.results and state.results[0][0] <= now - self._window:
                    state.results.popleft()
                if len(state.results) >= self._min_requests:
                    ratio = sum(failure for _, failure in state.results) / len(
                        state.results
                    )
                    if ratio >= self._failure_ratio:
                        state.opened_until = now + self._recovery_timeout
            if probe:
                state.probing = False
