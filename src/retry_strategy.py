import asyncio
import logging
from collections import Counter
from collections.abc import Awaitable, Callable
from contextvars import ContextVar
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from math import isfinite
from time import perf_counter

from src.errors import NetworkError, PermanentError, TransientError

logger = logging.getLogger(__name__)
_attempt: ContextVar[int] = ContextVar("retry_attempt", default=1)


@dataclass(frozen=True)
class RetryPolicy:
    max_retries: int
    initial_delay: float

    def __post_init__(self) -> None:
        if (
            self.max_retries < 0
            or not isfinite(self.initial_delay)
            or self.initial_delay < 0
        ):
            raise ValueError("Лимит повторов и задержка должны быть неотрицательными")


@dataclass(frozen=True)
class ErrorRecord:
    timestamp: str
    url: str
    error_type: str
    message: str
    status: int | None
    attempt: int
    next_delay: float
    final: bool


class RetryStrategy:
    def __init__(
        self,
        max_retries: int = 3,
        backoff_factor: float = 2.0,
        retry_on: list[type[Exception]] | None = None,
        *,
        policies: dict[type[Exception], RetryPolicy] | None = None,
        max_delay: float = 60.0,
    ) -> None:
        if max_retries < 0:
            raise ValueError("max_retries должен быть неотрицательным")
        if not isfinite(backoff_factor) or backoff_factor < 1:
            raise ValueError("backoff_factor должен быть конечным и не меньше 1")
        if not isfinite(max_delay) or max_delay < 0:
            raise ValueError("max_delay должен быть неотрицательным")
        self.max_retries = max_retries
        self.backoff_factor = backoff_factor
        self.retry_on = tuple(
            [TransientError, NetworkError] if retry_on is None else retry_on
        )
        if any(
            not isinstance(cls, type) or not issubclass(cls, Exception)
            for cls in self.retry_on
        ):
            raise TypeError("retry_on должен содержать классы исключений")
        self.policies = {
            TransientError: RetryPolicy(max_retries, 1.0),
            NetworkError: RetryPolicy(min(max_retries, 2), 0.5),
        }
        self.policies.update(policies or {})
        self.max_delay = max_delay
        self.errors: list[ErrorRecord] = []
        self._counts: Counter[str] = Counter()
        self._permanent_urls: set[str] = set()
        self._successful_retries = 0
        self._retry_wait = 0.0
        self._wait_count = 0

    @property
    def current_attempt(self) -> int:
        """Номер попытки текущей задачи, независимо от других загрузок."""
        return _attempt.get()

    def record_error(
        self,
        error: Exception,
        *,
        attempt: int = 1,
        delay: float = 0.0,
        final: bool = True,
    ) -> None:
        url = getattr(error, "url", "")
        record = ErrorRecord(
            datetime.now(UTC).isoformat(),
            url,
            type(error).__name__,
            str(error),
            getattr(error, "status", None),
            attempt,
            delay,
            final,
        )
        self.errors.append(record)
        self._counts[record.error_type] += 1
        if isinstance(error, PermanentError) and url:
            self._permanent_urls.add(url)
        logger.warning(
            "Ошибка %s | URL %s | Попытка %d | Пауза %.3f с | Итог %s | %s",
            record.error_type,
            url,
            attempt,
            delay,
            "неудача" if final else "повтор",
            error,
            extra={"crawler_error": asdict(record)},
        )

    async def execute_with_retry[**P, T](
        self, coro: Callable[P, Awaitable[T]], *args: P.args, **kwargs: P.kwargs
    ) -> T:
        failures: Counter[type[Exception]] = Counter()
        url = args[0] if args and isinstance(args[0], str) else ""
        for attempt in range(1, self.max_retries + 2):
            token = _attempt.set(attempt)
            try:
                logger.info("URL %s | Начало попытки %d", url, attempt)
                result = await coro(*args, **kwargs)
            except Exception as error:
                policy_type = next(
                    (cls for cls in type(error).__mro__ if cls in self.policies),
                    type(error),
                )
                failures[policy_type] += 1
                policy = self.policies.get(
                    policy_type, RetryPolicy(self.max_retries, 1.0)
                )
                retry = (
                    isinstance(error, self.retry_on)
                    and not isinstance(error, PermanentError)
                    and attempt <= self.max_retries
                    and failures[policy_type] <= policy.max_retries
                )
                delay = 0.0
                if retry:
                    delay = policy.initial_delay
                    for _ in range(failures[policy_type] - 1):
                        delay = min(self.max_delay, delay * self.backoff_factor)
                    if getattr(error, "status", None) == 429:
                        delay *= 2
                    delay = max(
                        min(self.max_delay, delay), getattr(error, "retry_after", 0.0)
                    )
                self.record_error(error, attempt=attempt, delay=delay, final=not retry)
                if not retry:
                    raise
            else:
                if attempt > 1:
                    self._successful_retries += 1
                logger.info("URL %s | Итог попытки %d: успех", url, attempt)
                return result
            finally:
                _attempt.reset(token)

            started_at = perf_counter()
            await asyncio.sleep(delay)
            self._retry_wait += perf_counter() - started_at
            self._wait_count += 1

        raise RuntimeError("Исчерпаны попытки операции")

    def get_stats(self) -> dict[str, object]:
        return {
            "errors_by_type": dict(self._counts),
            "successful_retries": self._successful_retries,
            "average_retry_wait": self._retry_wait / self._wait_count
            if self._wait_count
            else 0.0,
            "permanent_error_urls": sorted(self._permanent_urls),
        }
