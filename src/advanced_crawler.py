import asyncio
import json
import logging
from pathlib import Path
from urllib.parse import urlsplit

from src.config import CrawlerConfig
from src.crawler import AsyncCrawler
from src.crawler_stats import CrawlerStats, atomic_text
from src.data_storage import CrawledPage
from src.file_storage import CSVStorage, JSONStorage
from src.logging_setup import LoggingSession
from src.retry_strategy import RetryStrategy
from src.sitemap_parser import SitemapParser
from src.sqlite_storage import SQLiteStorage

logger = logging.getLogger(__name__)


class AdvancedCrawler:
    def __init__(self, config: CrawlerConfig | None = None) -> None:
        self.config = config or CrawlerConfig()
        config = self.config
        self.stats = CrawlerStats()
        suffix = Path(config.output).suffix.lower()
        if suffix in (".db", ".sqlite", ".sqlite3"):
            storage = SQLiteStorage(config.output)
        elif suffix == ".csv":
            storage = CSVStorage(config.output, encoding=config.encoding)
        else:
            storage = JSONStorage(config.output, json_lines=suffix == ".jsonl")
        self.crawler = AsyncCrawler(
            max_concurrent=config.max_concurrent,
            max_per_domain=config.max_per_domain,
            max_depth=config.max_depth,
            requests_per_second=config.rate_limit,
            min_delay=config.min_delay,
            jitter=config.jitter,
            user_agent=config.user_agent,
            respect_robots=config.respect_robots,
            stats=self.stats,
            storage=storage,
            connect_timeout=config.connect_timeout,
            read_timeout=config.read_timeout,
            total_timeout=config.total_timeout,
            retry_strategy=RetryStrategy(config.max_retries, config.backoff_factor),
        )
        self.sitemap_parser = SitemapParser(self._download_sitemap)
        self._logs = LoggingSession(
            config.log_file,
            config.log_level,
            config.log_max_bytes,
            config.log_backup_count,
        )
        self._lock = asyncio.Lock()
        self._closed = False
        self._budget = config.max_pages
        self._sitemap_errors: dict[str, str] = {}

    @classmethod
    def from_config(cls, filename: str | Path) -> "AdvancedCrawler":
        return cls(CrawlerConfig.from_file(filename))

    async def _download_sitemap(self, url: str) -> bytes:
        _, body = await self.crawler._fetch_response(url)
        return body

    async def crawl(
        self, start_urls: list[str] | None = None
    ) -> dict[str, CrawledPage]:
        async with self._lock:
            if self._closed:
                raise RuntimeError("Краулер уже закрыт")
            self.stats.start()
            self._sitemap_errors.clear()
            stop = asyncio.Event()
            monitor = asyncio.create_task(self._monitor(stop))
            try:
                self.crawler.create_session()
                urls = list(self.config.urls if start_urls is None else start_urls)
                allowed = {
                    urlsplit(url).hostname for url in urls or self.config.sitemap_urls
                }
                for sitemap_url in self.config.sitemap_urls:
                    found = await self.sitemap_parser.fetch_sitemap(sitemap_url)
                    self._sitemap_errors.update(self.sitemap_parser.errors)
                    urls.extend(
                        url
                        for url in found
                        if not self.config.same_domain_only
                        or urlsplit(url).hostname in allowed
                    )
                return await self.crawler.crawl(
                    list(dict.fromkeys(urls)),
                    max_pages=self.config.max_pages,
                    same_domain_only=self.config.same_domain_only,
                    include_patterns=self.config.include_patterns,
                    exclude_patterns=self.config.exclude_patterns,
                )
            except BaseException as error:
                self.stats.finish(
                    "cancelled"
                    if isinstance(error, asyncio.CancelledError)
                    else "failed"
                )
                raise
            finally:
                if self.stats.state == "running":
                    self.stats.finish()
                stop.set()
                await monitor
                await self.crawler._close_session()
                self._log_progress()

    def get_stats(self) -> dict[str, object]:
        stats = self.stats.get_stats()
        remaining = max(
            0,
            min(
                self._budget - stats["total_pages"],
                len(self.crawler.url_depths) - stats["total_pages"],
            ),
        )
        rate = stats["pages_per_second"]
        stats.update(
            {
                "active_tasks": self.crawler._manager.active_tasks,
                "progress_percent_of_limit": min(
                    100.0, stats["total_pages"] / self._budget * 100
                )
                if self._budget
                else 100.0,
                "eta_known_seconds": remaining / rate if rate > 0 else None,
                "sitemap_errors": dict(self._sitemap_errors),
                "request_stats": self.crawler.get_request_stats(),
                "error_stats": self.crawler.get_error_stats(),
                "storage_stats": self.crawler.get_storage_stats(),
            }
        )
        return stats

    def _log_progress(self) -> None:
        stats = self.get_stats()
        logger.info(
            "Прогресс %.1f%% от лимита | Страниц %d | %.2f стр/с | ETA известных URL %s с | Активных %d | %s",
            stats["progress_percent_of_limit"],
            stats["total_pages"],
            stats["pages_per_second"],
            stats["eta_known_seconds"],
            stats["active_tasks"],
            stats["state"],
        )

    async def _monitor(self, stop: asyncio.Event) -> None:
        while not stop.is_set():
            self._log_progress()
            try:
                await asyncio.wait_for(stop.wait(), self.config.progress_interval)
            except TimeoutError:
                continue

    def export_to_json(self, filename: str | Path) -> None:
        atomic_text(
            filename, json.dumps(self.get_stats(), ensure_ascii=False, indent=2)
        )

    def export_to_html_report(self, filename: str | Path) -> None:
        self.stats.export_to_html_report(filename, extra=self.get_stats())

    async def close(self) -> None:
        async with self._lock:
            if not self._closed:
                try:
                    await self.crawler.close()
                finally:
                    await asyncio.to_thread(self._logs.close)
                    self._closed = True
