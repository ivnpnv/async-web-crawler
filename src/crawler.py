import asyncio
import logging
from dataclasses import dataclass

import aiohttp

from src.html_parser import HTMLParser, ParsedPage

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@dataclass
class AsyncCrawler:
    max_concurrent: int = 10
    session: aiohttp.ClientSession = None
    connect_timeout: int = 5
    read_timeout: int = 10

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

    async def close(self) -> None:
        """Закрыть HTTP-сессию краулера."""
        if self.session:
            await self.session.close()
            self.session = None
