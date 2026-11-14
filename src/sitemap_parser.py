import asyncio
import gzip
import io
import logging
from collections.abc import Awaitable, Callable

from lxml import etree
from yarl import URL

from src.errors import CrawlerError

logger = logging.getLogger(__name__)


class SitemapParser:
    def __init__(
        self,
        download: Callable[[str], Awaitable[bytes]],
        *,
        max_sitemaps: int = 100,
        max_urls: int = 100_000,
        max_depth: int = 10,
    ) -> None:
        self._download = download
        self.max_sitemaps = max_sitemaps
        self.max_urls = max_urls
        self.max_depth = max_depth
        self.errors: dict[str, str] = {}

    @staticmethod
    def _parse(data: bytes) -> tuple[str, list[str]]:
        if data.startswith(b"\x1f\x8b"):
            with gzip.GzipFile(fileobj=io.BytesIO(data)) as source:
                data = source.read(50 * 1024 * 1024 + 1)
        if len(data) > 50 * 1024 * 1024:
            raise ValueError("Sitemap превышает 50 МиБ")
        parser = etree.XMLParser(
            resolve_entities=False, load_dtd=False, no_network=True
        )
        root = etree.fromstring(data, parser=parser)
        if root.getroottree().docinfo.doctype:
            raise ValueError("DTD в sitemap не поддерживается")
        kind = etree.QName(root).localname
        if kind not in ("urlset", "sitemapindex"):
            raise ValueError("Ожидался urlset или sitemapindex")
        child = "url" if kind == "urlset" else "sitemap"
        locations = root.findall(f"{{*}}{child}/{{*}}loc")
        urls: list[str] = []
        for location in locations:
            target = URL((location.text or "").strip()).with_fragment(None)
            if target.scheme not in ("http", "https") or not target.host:
                raise ValueError("Sitemap содержит не абсолютный HTTP URL")
            urls.append(str(target))
        return kind, urls

    async def fetch_sitemap(self, sitemap_url: str) -> list[str]:
        seen: set[str] = set()
        pages: dict[str, None] = {}
        pending = [(sitemap_url, 0)]
        self.errors.clear()
        while pending:
            url, depth = pending.pop()
            if url in seen:
                continue
            if len(seen) >= self.max_sitemaps or depth > self.max_depth:
                self.errors[url] = "Превышен лимит sitemap или глубины индекса"
                continue
            seen.add(url)
            try:
                data = await self._download(url)
                kind, urls = await asyncio.to_thread(self._parse, data)
            except (
                CrawlerError,
                ValueError,
                OSError,
                EOFError,
                etree.XMLSyntaxError,
            ) as error:
                self.errors[url] = str(error)
                logger.warning("Ошибка sitemap %s: %s", url, error)
                continue
            if kind == "sitemapindex":
                pending.extend((child, depth + 1) for child in reversed(urls))
            else:
                for page in urls:
                    if page not in pages and len(pages) >= self.max_urls:
                        self.errors[url] = "Превышен лимит URL в sitemap"
                        break
                    pages[page] = None
        if not pages and self.errors:
            logger.warning("Не извлечено страниц из sitemap %s", sitemap_url)
        return list(pages)
