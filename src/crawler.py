import asyncio
import logging
import re
from collections.abc import Callable
from dataclasses import dataclass, field
from time import perf_counter
from urllib.parse import urlsplit

import aiohttp

from src.crawler_queue import CrawlerQueue
from src.html_parser import HTMLParser, ParsedPage
from src.semaphore_manager import SemaphoreManager

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def _hostname(url: str) -> str | None:
    try:
        return (urlsplit(url).hostname or "").rstrip(".") or None
    except ValueError:
        return None


@dataclass
class AsyncCrawler:
    max_concurrent: int = 10
    session: aiohttp.ClientSession = None
    connect_timeout: int = 5
    read_timeout: int = 10
    max_depth: int = 2
    max_per_domain: int = 2
    visited_urls: set[str] = field(
        default_factory=set, init=False, repr=False, compare=False
    )
    failed_urls: dict[str, str] = field(
        default_factory=dict, init=False, repr=False, compare=False
    )
    processed_urls: dict[str, ParsedPage] = field(
        default_factory=dict, init=False, repr=False, compare=False
    )
    url_depths: dict[str, int] = field(
        default_factory=dict, init=False, repr=False, compare=False
    )
    _crawl_lock: asyncio.Lock = field(
        default_factory=asyncio.Lock, init=False, repr=False, compare=False
    )

    def create_session(self) -> None:
        """Создать HTTP-сессию краулера."""
        if self.session is None:
            self.session = aiohttp.ClientSession(
                connector=aiohttp.TCPConnector(limit=self.max_concurrent),
                timeout=aiohttp.ClientTimeout(
                    connect=self.connect_timeout,
                    sock_read=self.read_timeout,
                ),
            )

    async def fetch_url(self, url: str) -> str:
        """Загрузить страницу по URL и вернуть HTML."""
        logger.info("Начало загрузки %s", url)
        try:
            async with self.session.get(url) as response:
                response.raise_for_status()
                html = await response.text()
                logger.info("Загружено %s (HTTP %s)", url, response.status)
                return html
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

    async def fetch_and_parse(self, url: str) -> ParsedPage:
        """Загрузить страницу и распарсить HTML."""
        html = await self.fetch_url(url)
        parser = HTMLParser()
        return await parser.parse_html(html, url)

    async def crawl(
        self,
        start_urls: list[str],
        max_pages: int = 100,
        *,
        same_domain_only: bool = False,
        exclude_patterns: list[str] | None = None,
        include_patterns: list[str] | None = None,
    ) -> dict[str, ParsedPage]:
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
            if not start_urls or max_pages == 0:
                return {}

            manager = SemaphoreManager(self.max_concurrent, self.max_per_domain)
            owns_session = self.session is None
            self.create_session()
            try:
                await self._crawl_levels(start_urls, max_pages, manager, allow_url)
            finally:
                if owns_session:
                    await self.close()

            return {
                url: self.processed_urls[url]
                for url in self.url_depths
                if url in self.processed_urls
            }

    async def _crawl_levels(
        self,
        start_urls: list[str],
        max_pages: int,
        manager: SemaphoreManager,
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

        async def load_page(url: str) -> ParsedPage | None:
            try:
                async with manager.limit(url):
                    self.visited_urls.add(url)
                    page = await self.fetch_and_parse(url)
            except (TimeoutError, aiohttp.ClientError, ValueError) as error:
                self.visited_urls.add(url)
                reason = str(error) or type(error).__name__
                self.failed_urls[url] = reason
                queue.mark_failed(url, reason)
                page = None
            else:
                self.processed_urls[url] = page
                queue.mark_processed(url)

            processed = len(self.processed_urls)
            elapsed = max(perf_counter() - started_at, 1e-9)
            logger.info(
                "Обработано: %d | В очереди: %d | Ошибки: %d | Скорость: %.2f стр/с",
                processed,
                len(self.url_depths) - len(self.visited_urls),
                len(self.failed_urls),
                processed / elapsed,
            )
            return page

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
            results = await asyncio.gather(
                *(load_page(url) for url in current_level), return_exceptions=True
            )
            for result in results:
                if isinstance(result, BaseException):
                    raise result
                if result is not None and depth < self.max_depth and pages_left:
                    for link in result["links"]:
                        enqueue(link, depth + 1)

            if not pages_left:
                break

    async def close(self) -> None:
        """Закрыть HTTP-сессию краулера."""
        if self.session:
            await self.session.close()
            self.session = None
