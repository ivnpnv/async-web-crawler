import asyncio
import logging
import re
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from math import isfinite
from time import perf_counter
from types import SimpleNamespace
from urllib.parse import urlsplit

import aiohttp
from yarl import URL

from src.circuit_breaker import CircuitBreaker
from src.crawler_queue import CrawlerQueue
from src.crawler_stats import CrawlerStats
from src.data_storage import CrawledPage, DataStorage, StorageError
from src.errors import (
    CrawlerError,
    NetworkError,
    ParseError,
    PermanentError,
    RobotsBlockedError,
    TransientError,
)
from src.html_parser import HTMLParser
from src.rate_limiter import RateLimiter
from src.retry_strategy import RetryStrategy
from src.robots_parser import RobotsParser
from src.semaphore_manager import SemaphoreManager

logger = logging.getLogger(__name__)

REDIRECT_STATUSES = {301, 302, 303, 307, 308}


@dataclass(frozen=True, slots=True)
class _RequestTiming:
    limiter: RateLimiter
    crawl_delay: float
    log_stats: Callable[[], None]


async def _limit_request_start(
    _session: aiohttp.ClientSession,
    context: SimpleNamespace,
    params: aiohttp.TraceRequestHeadersSentParams,
) -> None:
    timing = context.trace_request_ctx
    if isinstance(timing, _RequestTiming) and params.method == "GET":
        await timing.limiter.acquire(
            params.url.raw_host, crawl_delay=timing.crawl_delay
        )
        timing.log_stats()


_REQUEST_TRACE = aiohttp.TraceConfig()
_REQUEST_TRACE.on_request_headers_sent.append(_limit_request_start)
_REQUEST_TRACE.freeze()


def _retry_after(value: str | None) -> float:
    if not value:
        return 0.0
    try:
        if value.isascii() and value.isdecimal():
            seconds = float(value)
            return seconds if isfinite(seconds) else 0.0
        deadline = parsedate_to_datetime(value)
        if deadline.tzinfo is None:
            deadline = deadline.replace(tzinfo=UTC)
        return max(0.0, (deadline - datetime.now(UTC)).total_seconds())
    except (TypeError, ValueError, OverflowError):
        return 0.0


def _hostname(url: str) -> str | None:
    try:
        return (urlsplit(url).hostname or "").rstrip(".") or None
    except ValueError:
        return None


@dataclass
class AsyncCrawler:
    max_concurrent: int = 10
    session: aiohttp.ClientSession | None = None
    connect_timeout: float = 5.0
    read_timeout: float = 10.0
    max_depth: int = 2
    max_per_domain: int = 2
    requests_per_second: float = 1.0
    per_domain: bool = True
    min_delay: float = 0.0
    jitter: float = 0.0
    retry_strategy: RetryStrategy = field(default_factory=RetryStrategy)
    total_timeout: float = 30.0
    timeout_multiplier: float = 1.5
    max_timeout: float = 120.0
    circuit_breaker: CircuitBreaker = field(default_factory=CircuitBreaker)
    user_agent: str = "ItGrindCrawler/1.0"
    respect_robots: bool = True
    stats: CrawlerStats | None = None
    storage: DataStorage | None = None
    storage_errors: dict[str, str] = field(default_factory=dict, init=False, repr=False)
    saved_urls: set[str] = field(default_factory=set, init=False, repr=False)
    visited_urls: set[str] = field(
        default_factory=set, init=False, repr=False, compare=False
    )
    failed_urls: dict[str, str] = field(
        default_factory=dict, init=False, repr=False, compare=False
    )
    processed_urls: dict[str, CrawledPage] = field(
        default_factory=dict, init=False, repr=False, compare=False
    )
    url_depths: dict[str, int] = field(
        default_factory=dict, init=False, repr=False, compare=False
    )
    _crawl_lock: asyncio.Lock = field(
        default_factory=asyncio.Lock, init=False, repr=False, compare=False
    )
    _manager: SemaphoreManager = field(init=False, repr=False, compare=False)
    _rate_limiter: RateLimiter = field(init=False, repr=False, compare=False)
    _robots: RobotsParser = field(init=False, repr=False, compare=False)
    _robots_blocked: set[str] = field(
        default_factory=set, init=False, repr=False, compare=False
    )

    def __post_init__(self) -> None:
        timeouts = (
            self.connect_timeout,
            self.read_timeout,
            self.total_timeout,
            self.max_timeout,
        )
        if any(not isfinite(value) or value <= 0 for value in timeouts):
            raise ValueError("Таймауты должны быть конечными положительными числами")
        if not isfinite(self.timeout_multiplier) or self.timeout_multiplier < 1:
            raise ValueError("timeout_multiplier должен быть не меньше 1")
        if not self.user_agent.strip():
            raise ValueError("User-Agent не должен быть пустым")
        self._manager = SemaphoreManager(self.max_concurrent, self.max_per_domain)
        self._rate_limiter = RateLimiter(
            self.requests_per_second,
            self.per_domain,
            min_delay=self.min_delay,
            jitter=self.jitter,
        )
        self._robots = RobotsParser(self._download_robots)

    def create_session(self) -> None:
        """Создать HTTP-сессию краулера."""
        if self.session is None:
            self.session = aiohttp.ClientSession(
                connector=aiohttp.TCPConnector(limit=self.max_concurrent),
                timeout=aiohttp.ClientTimeout(
                    connect=self.connect_timeout,
                    sock_read=self.read_timeout,
                    total=self.total_timeout,
                ),
                trace_configs=[_REQUEST_TRACE],
            )

    async def fetch_url(self, url: str) -> str:
        """Загрузить страницу по URL и вернуть HTML."""
        html, _ = await self._fetch_page(url)
        return html

    def get_request_stats(self) -> dict[str, float | int]:
        """Вернуть частоту за последнюю секунду и накопленные счётчики."""
        stats = self._rate_limiter.get_stats()
        stats["robots_blocked"] = len(self._robots_blocked)
        return stats

    def get_error_stats(self) -> dict[str, object]:
        return self.retry_strategy.get_stats()

    def get_storage_stats(self) -> dict[str, object]:
        return {
            "saved_urls": len(self.saved_urls),
            "failed_urls": dict(self.storage_errors),
            "storage": self.storage.get_stats() if self.storage is not None else None,
        }

    def _log_request_stats(self) -> None:
        stats = self.get_request_stats()
        logger.info(
            "HTTP: %d req/sec | Средний интервал: %.3f с | Блокировки robots.txt: %d",
            stats["requests_per_second"],
            stats["average_delay"],
            stats["robots_blocked"],
        )

    def _record_request_start(self) -> None:
        if self.stats is not None:
            self.stats.http_started += 1
        self._log_request_stats()

    async def _robots_delay(self, url: str) -> float:
        await self._robots.fetch_robots(url)
        if not self._robots.can_fetch(url, self.user_agent):
            self._robots_blocked.add(url)
            logger.warning("URL заблокирован robots.txt: %s", url)
            self._log_request_stats()
            raise RobotsBlockedError(f"URL заблокирован robots.txt: {url}", url=url)
        return self._robots.get_crawl_delay(self.user_agent, base_url=url)

    async def _request(
        self, url: URL, *, check_robots: bool
    ) -> tuple[aiohttp.ClientResponse, bytes]:
        return await self.retry_strategy.execute_with_retry(
            self._request_once, str(url), check_robots=check_robots
        )

    async def _request_once(
        self, target_url: str, *, check_robots: bool
    ) -> tuple[aiohttp.ClientResponse, bytes]:
        session = self.session
        if session is None:
            raise RuntimeError("Вызовите create_session() перед загрузкой страницы")
        if _REQUEST_TRACE not in session.trace_configs:
            session.trace_configs.append(_REQUEST_TRACE)

        url = URL(target_url)
        attempt = self.retry_strategy.current_attempt
        scale = min(self.timeout_multiplier, 10.0) ** min(attempt - 1, 10)
        timeout = aiohttp.ClientTimeout(
            connect=min(self.max_timeout, self.connect_timeout * scale),
            sock_read=min(self.max_timeout, self.read_timeout * scale),
            total=min(self.max_timeout, self.total_timeout * scale),
        )
        crawl_delay = await self._robots_delay(target_url) if check_robots else 0.0
        async with (
            self._manager.limit(target_url),
            self.circuit_breaker.request(target_url),
        ):
            try:
                async with (
                    session.get(
                        url,
                        headers={"User-Agent": self.user_agent},
                        allow_redirects=False,
                        raise_for_status=False,
                        timeout=timeout,
                        trace_request_ctx=_RequestTiming(
                            self._rate_limiter, crawl_delay, self._record_request_start
                        ),
                    ) as response,
                ):
                    if self.stats is not None:
                        self.stats.http_statuses[response.status] += 1
                    redirect = response.status in REDIRECT_STATUSES and (
                        response.headers.get("Location") or response.headers.get("URI")
                    )
                    body = (
                        await response.read() if response.ok and not redirect else b""
                    )
                    if self.stats is not None:
                        if response.status >= 400:
                            self.stats.http_failed += 1
                        else:
                            self.stats.http_successful += 1
                    if response.status in (408, 429) or 500 <= response.status < 600:
                        raise TransientError(
                            f"HTTP {response.status}",
                            url=target_url,
                            status=response.status,
                            retry_after=_retry_after(
                                response.headers.get("Retry-After")
                            ),
                        )
                    if response.status >= 400:
                        if (
                            not check_robots
                            and url.raw_path == "/robots.txt"
                            and response.status in (404, 410)
                        ):
                            return response, body
                        raise PermanentError(
                            f"HTTP {response.status}",
                            url=target_url,
                            status=response.status,
                        )
                    return response, body
            except TimeoutError as error:
                if self.stats is not None:
                    self.stats.http_transport_errors += 1
                    self.stats.http_failed += 1
                raise TransientError(
                    f"Таймаут: {type(error).__name__}", url=target_url
                ) from error
            except (aiohttp.ClientSSLError, aiohttp.ServerFingerprintMismatch) as error:
                if self.stats is not None:
                    self.stats.http_transport_errors += 1
                    self.stats.http_failed += 1
                raise PermanentError(str(error), url=target_url) from error
            except (aiohttp.ClientConnectionError, aiohttp.ClientPayloadError) as error:
                if self.stats is not None:
                    self.stats.http_transport_errors += 1
                    self.stats.http_failed += 1
                raise NetworkError(str(error), url=target_url) from error
            except aiohttp.ClientError as error:
                if isinstance(error, CrawlerError):
                    raise
                raise PermanentError(str(error), url=target_url) from error

    async def _fetch_response(
        self,
        url: str,
        *,
        check_robots: bool = True,
        allow_url: Callable[[str], bool] | None = None,
    ) -> tuple[aiohttp.ClientResponse, bytes]:
        target = URL(url).with_fragment(None)
        history: list[aiohttp.ClientResponse] = []
        while True:
            if target.scheme not in ("http", "https") or not target.raw_host:
                raise aiohttp.InvalidURL(target)

            response, body = await self._request(
                target,
                check_robots=self.respect_robots
                and check_robots
                and target.raw_path != "/robots.txt",
            )
            location = response.headers.get("Location") or response.headers.get("URI")
            if response.status not in REDIRECT_STATUSES or not location:
                return response, body
            if len(history) >= 10:
                raise aiohttp.TooManyRedirects(response.request_info, tuple(history))
            history.append(response)

            destination = response.url.join(URL(location)).with_fragment(None)
            if allow_url is not None and not allow_url(str(destination)):
                raise aiohttp.InvalidURL(
                    destination, "Редирект не прошёл фильтры обхода"
                )
            session = self.session
            if (
                session is not None
                and destination.scheme in ("http", "https")
                and destination.raw_host
                and target.origin() != destination.origin()
                and (
                    session.auth
                    or "Authorization" in session.headers
                    or "Cookie" in session.headers
                )
            ):
                raise aiohttp.InvalidURL(
                    destination, "Межсайтовый редирект с данными авторизации"
                )
            target = destination

    async def _download_robots(self, url: str) -> tuple[int, str]:
        response, _ = await self._fetch_response(url, check_robots=False)
        body = (
            await response.text(encoding="utf-8", errors="replace")
            if 200 <= response.status < 300
            else ""
        )
        return response.status, body

    async def _fetch_page(
        self, url: str, allow_url: Callable[[str], bool] | None = None
    ) -> tuple[str, aiohttp.ClientResponse]:
        logger.info("Начало загрузки %s", url)
        try:
            try:
                response, _ = await self._fetch_response(url, allow_url=allow_url)
            except (ValueError, aiohttp.InvalidURL, aiohttp.TooManyRedirects) as error:
                classified = PermanentError(str(error), url=url)
                self.retry_strategy.record_error(classified)
                raise classified from error
            response.raise_for_status()
            try:
                html = await response.text()
            except UnicodeError as error:
                classified = ParseError("Не удалось декодировать HTML", url=url)
                self.retry_strategy.record_error(classified)
                raise classified from error
            logger.info("Загружено %s (HTTP %s)", url, response.status)
            return html, response
        except (TimeoutError, aiohttp.ClientError) as error:
            logger.warning("Ошибка загрузки %s (%s)", url, type(error).__name__)
            raise

    async def fetch_urls(self, urls: list[str]) -> dict[str, str]:
        """Загрузить страницы конкурентно и вернуть HTML по URL."""
        tasks = []
        for url in urls:
            tasks.append(self.fetch_url(url))
        responses = await asyncio.gather(*tasks, return_exceptions=True)
        return {
            url: result
            for url, result in zip(urls, responses)
            if isinstance(result, str)
        }

    async def fetch_and_parse(
        self, url: str, *, allow_url: Callable[[str], bool] | None = None
    ) -> CrawledPage:
        """Загрузить страницу и распарсить HTML."""
        html, response = await self._fetch_page(url, allow_url)
        final_url = str(response.url)
        parser = HTMLParser(on_error=self.retry_strategy.record_error)
        page = await parser.parse_html(html, final_url)
        result: CrawledPage = {
            **page,
            "url": url,
            "final_url": final_url,
            "crawled_at": datetime.now(UTC),
            "status_code": response.status,
            "content_type": response.headers.get("Content-Type", ""),
        }
        if self.storage is not None:
            try:
                await self.storage.save(dict(result))
            except (StorageError, OSError) as error:
                self.storage_errors[url] = str(error)
                logger.error(
                    "Ошибка сохранения %s (%s): %s", url, type(error).__name__, error
                )
            else:
                self.saved_urls.add(url)
                self.storage_errors.pop(url, None)
        return result

    async def crawl(
        self,
        start_urls: list[str],
        max_pages: int = 100,
        *,
        same_domain_only: bool = False,
        exclude_patterns: list[str] | None = None,
        include_patterns: list[str] | None = None,
    ) -> dict[str, CrawledPage]:
        """Обойти URL по глубине. Паттерны проверяются как регулярные выражения."""
        if max_pages < 0 or self.max_depth < 0:
            raise ValueError("max_pages и max_depth должны быть неотрицательными")

        include = [re.compile(pattern) for pattern in include_patterns or []]
        exclude = [re.compile(pattern) for pattern in exclude_patterns or []]
        allowed_domains = (
            {_hostname(url) for url in start_urls} - {None}
            if same_domain_only
            else None
        )

        def allow_url(url: str) -> bool:
            if allowed_domains is not None and _hostname(url) not in allowed_domains:
                return False
            if any(pattern.search(url) for pattern in exclude):
                return False
            return not include or any(pattern.search(url) for pattern in include)

        async with self._crawl_lock:
            self.visited_urls.clear()
            self.failed_urls.clear()
            self.processed_urls.clear()
            self.url_depths.clear()
            self.storage_errors.clear()
            self.saved_urls.clear()
            if not start_urls or max_pages == 0:
                return {}

            owns_session = self.session is None
            self.create_session()
            try:
                await self._crawl_levels(start_urls, max_pages, allow_url)
            finally:
                if owns_session:
                    await self._close_session()

            return {
                url: self.processed_urls[url]
                for url in self.url_depths
                if url in self.processed_urls
            }

    async def _crawl_levels(
        self,
        start_urls: list[str],
        max_pages: int,
        allow_url: Callable[[str], bool],
    ) -> None:
        queue = CrawlerQueue()
        started_at = perf_counter()

        def enqueue(url: str, depth: int) -> None:
            url = url.partition("#")[0]
            if url not in self.url_depths and allow_url(url):
                queue.add_url(url)
                self.url_depths[url] = depth

        for url in start_urls:
            enqueue(url, depth=0)

        async def load_page(url: str) -> CrawledPage | None:
            try:
                self.visited_urls.add(url)
                page = await self.fetch_and_parse(url, allow_url=allow_url)
            except (TimeoutError, aiohttp.ClientError, ValueError) as error:
                reason = str(error) or type(error).__name__
                self.failed_urls[url] = reason
                queue.mark_failed(url, reason)
                page = None
                if self.stats is not None:
                    self.stats.record_page(
                        url, getattr(error, "status", None), success=False
                    )
            else:
                self.processed_urls[url] = page
                queue.mark_processed(url)
                if self.stats is not None:
                    self.stats.record_page(url, page["status_code"], success=True)

            processed = len(self.processed_urls)
            failed = len(self.failed_urls)
            pending = max(
                0,
                len(self.url_depths) - processed - failed - self._manager.active_tasks,
            )
            elapsed = max(perf_counter() - started_at, 1e-9)
            logger.info(
                "Обработано: %d | В очереди: %d | Ошибки: %d | Скорость: %.2f стр/с",
                processed,
                pending,
                failed,
                processed / elapsed,
            )
            return page

        async def worker(
            work: Iterator[tuple[int, str]], results: list[CrawledPage | None]
        ) -> None:
            for index, url in work:
                results[index] = await load_page(url)

        pages_left = max_pages
        for depth in range(self.max_depth + 1):
            current_level: list[str] = []
            while len(current_level) < pages_left:
                url = await queue.get_next()
                if url is None:
                    break
                current_level.append(url)
            if not current_level:
                break

            pages_left -= len(current_level)
            # Число задач ограничено числом работников, независимо от размера уровня.
            work = iter(enumerate(current_level))
            results: list[CrawledPage | None] = [None] * len(current_level)

            workers = await asyncio.gather(
                *(
                    worker(work, results)
                    for _ in range(min(self.max_concurrent, len(current_level)))
                ),
                return_exceptions=True,
            )
            for result in workers:
                if isinstance(result, BaseException):
                    raise result
            for result in results:
                if result is not None and depth < self.max_depth and pages_left:
                    for link in result["links"]:
                        enqueue(link, depth + 1)

            if not pages_left:
                break

    async def close(self) -> None:
        """Закрыть HTTP-сессию и подключённое хранилище."""
        try:
            await self._close_session()
        finally:
            if self.storage is not None:
                await self.storage.close()

    async def _close_session(self) -> None:
        if self.session:
            await self.session.close()
            self.session = None
