import asyncio
import gzip
import json
import sys
import tempfile
import unittest
from collections import Counter
from contextlib import redirect_stderr
from io import StringIO
from pathlib import Path
from unittest.mock import AsyncMock, patch

import aiosqlite
from aiohttp import web
from aiohttp.test_utils import TestServer

from crawler import parse_config
from src.advanced_crawler import AdvancedCrawler
from src.config import CrawlerConfig
from src.crawler import AsyncCrawler
from src.crawler_stats import CrawlerStats
from src.rate_limiter import RateLimiter
from src.sitemap_parser import SitemapParser


class SitemapTests(unittest.IsolatedAsyncioTestCase):
    async def test_high_resolution_rate_clock(self) -> None:
        limiter = RateLimiter(1_000_000)
        with (
            patch("src.rate_limiter.perf_counter", side_effect=[100.0, 100.0001]),
            patch("src.rate_limiter.asyncio.sleep", new_callable=AsyncMock) as sleep,
        ):
            await limiter.acquire("a.test")
            await limiter.acquire("a.test")
        sleep.assert_not_awaited()

    async def test_nested_index_gzip_cycles_and_duplicates(self) -> None:
        documents = {
            "https://a.test/index.xml": b"<sitemapindex><sitemap><loc>https://a.test/child.xml</loc></sitemap><sitemap><loc>https://a.test/index.xml</loc></sitemap></sitemapindex>",
            "https://a.test/child.xml": gzip.compress(
                b'<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9"><url><loc>https://a.test/1</loc></url><url><loc>https://a.test/1#top</loc></url><url><loc>https://a.test/2</loc></url></urlset>'
            ),
        }
        calls = Counter()

        async def download(url: str) -> bytes:
            calls[url] += 1
            return documents[url]

        parser = SitemapParser(download)
        urls = await parser.fetch_sitemap("https://a.test/index.xml")
        self.assertEqual(urls, ["https://a.test/1", "https://a.test/2"])
        self.assertEqual(calls, {url: 1 for url in documents})
        self.assertEqual(parser.errors, {})

    async def test_invalid_xml_entities_and_limits(self) -> None:
        async def download(_url: str) -> bytes:
            return b'<!DOCTYPE urlset [<!ENTITY secret SYSTEM "file:///secret">]><urlset><url><loc>&secret;</loc></url></urlset>'

        parser = SitemapParser(download)
        self.assertEqual(await parser.fetch_sitemap("https://a.test/index.xml"), [])
        self.assertTrue(parser.errors)

        async def many(_url: str) -> bytes:
            return b"<urlset><url><loc>https://a.test/1</loc></url><url><loc>https://a.test/2</loc></url></urlset>"

        parser = SitemapParser(many, max_urls=1)
        self.assertEqual(
            await parser.fetch_sitemap("https://a.test/index.xml"), ["https://a.test/1"]
        )
        self.assertTrue(parser.errors)


class ConfigTests(unittest.TestCase):
    def test_validation_and_cli_overrides(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "config.json"
            path.write_text(
                json.dumps(
                    {
                        "urls": ["https://example.com"],
                        "max_pages": 12,
                        "respect_robots": False,
                    }
                ),
                encoding="utf-8",
            )
            config = parse_config(
                ["--config", str(path), "--max-pages", "2", "--respect-robots"]
            )
            self.assertEqual(config.max_pages, 2)
            self.assertTrue(config.respect_robots)
            self.assertTrue(Path(config.output).is_absolute())
            for values in (
                {"max_pages": True},
                {"rate_limit": -1},
                {"respect_robots": "false"},
                {"unknown": 1},
            ):
                path.write_text(json.dumps(values), encoding="utf-8")
                with self.assertRaises((ValueError, TypeError)):
                    CrawlerConfig.from_file(path)
        with redirect_stderr(StringIO()), self.assertRaises(SystemExit):
            parse_config(["--urls", "https://example.com", "--max-pages", "-1"])

    def test_html_report_escapes_external_values(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            stats = CrawlerStats()
            stats.domains["<script>alert(1)</script>"] = 1
            path = Path(directory) / "report.html"
            stats.export_to_html_report(path)
            report = path.read_text(encoding="utf-8")
            self.assertNotIn("<script>", report)
            self.assertIn("&lt;script&gt;", report)


class IntegrationTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.directory = Path(self.temporary.name)
        self.hits: Counter[str] = Counter()

        async def handler(request: web.Request) -> web.Response:
            path = request.path
            self.hits[path] += 1
            base = f"http://{request.host}"
            if path == "/robots.txt":
                return web.Response(text="User-agent: *\nDisallow: /blocked\n")
            if path == "/sitemap.xml":
                return web.Response(
                    text=f"<sitemapindex><sitemap><loc>{base}/child.xml</loc></sitemap></sitemapindex>"
                )
            if path == "/child.xml":
                return web.Response(
                    text=f"<urlset><url><loc>{base}/</loc></url><url><loc>{base}/missing</loc></url><url><loc>{base}/blocked</loc></url></urlset>"
                )
            if path == "/missing":
                raise web.HTTPNotFound()
            return web.Response(
                text='<title>Page</title><a href="/next">Next</a>',
                content_type="text/html",
            )

        app = web.Application()
        app.router.add_get("/{path:.*}", handler)
        self.server = TestServer(app)
        await self.server.start_server()

    async def asyncTearDown(self) -> None:
        await self.server.close()
        self.temporary.cleanup()

    def config(self, **kwargs: object) -> CrawlerConfig:
        return CrawlerConfig(
            output=str(self.directory / "pages.sqlite"),
            log_file=str(self.directory / "crawler.log"),
            stats_json=str(self.directory / "stats.json"),
            report_html=str(self.directory / "report.html"),
            rate_limit=1000,
            progress_interval=0.01,
            **kwargs,
        )

    async def test_sitemap_crawl_reports_storage_and_rotation(self) -> None:
        config = self.config(
            sitemap_urls=[str(self.server.make_url("/sitemap.xml"))],
            max_depth=1,
            log_max_bytes=500,
            log_backup_count=2,
        )
        crawler = AdvancedCrawler(config)
        try:
            pages = await crawler.crawl()
            stats = crawler.get_stats()
            self.assertEqual(len(pages), 2)
            self.assertEqual(
                (stats["total_pages"], stats["successful"], stats["failed"]), (4, 2, 2)
            )
            self.assertEqual(stats["status_codes"], {200: 2, 404: 1})
            self.assertEqual(stats["http_status_codes"], {200: 5, 404: 1})
            self.assertEqual(stats["request_stats"]["robots_blocked"], 1)
            self.assertEqual(self.hits["/blocked"], 0)
            crawler.export_to_json(config.stats_json)
            crawler.export_to_html_report(config.report_html)
            self.assertEqual(
                json.loads(Path(config.stats_json).read_text(encoding="utf-8"))[
                    "total_pages"
                ],
                4,
            )
            self.assertIn(
                "<meter", Path(config.report_html).read_text(encoding="utf-8")
            )
        finally:
            await crawler.close()
        async with (
            aiosqlite.connect(config.output) as connection,
            connection.execute("SELECT count(*) FROM pages") as cursor,
        ):
            self.assertEqual((await cursor.fetchone())[0], 2)
        logs = list(self.directory.glob("crawler.log*"))
        self.assertGreater(len(logs), 1)
        for log in logs:
            for line in log.read_text(encoding="utf-8").splitlines():
                self.assertIn("timestamp", json.loads(line))

    async def test_cli_real_process_and_no_robots_override(self) -> None:
        values = vars(
            self.config(urls=[str(self.server.make_url("/blocked"))], max_depth=0)
        )
        config_file = self.directory / "config.json"
        config_file.write_text(json.dumps(values), encoding="utf-8")
        process = await asyncio.create_subprocess_exec(
            sys.executable,
            "-B",
            str(Path(__file__).resolve().parents[1] / "crawler.py"),
            "--config",
            str(config_file),
            "--no-respect-robots",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await asyncio.wait_for(process.communicate(), 30)
        self.assertEqual(process.returncode, 0, stderr.decode(errors="replace"))
        self.assertEqual(json.loads(stdout)["successful"], 1)
        self.assertEqual(self.hits["/robots.txt"], 0)
        self.assertEqual(self.hits["/blocked"], 1)

    async def test_worker_pool_does_not_create_one_task_per_url(self) -> None:
        crawler = AsyncCrawler(
            max_concurrent=3,
            max_per_domain=3,
            max_depth=0,
            requests_per_second=10000,
            respect_robots=False,
        )
        peak = 0
        original = crawler.fetch_and_parse

        async def observe(url: str, **kwargs: object) -> object:
            nonlocal peak
            peak = max(
                peak,
                sum(
                    task.get_coro().__qualname__.endswith(".worker")
                    for task in asyncio.all_tasks()
                ),
            )
            return await original(url, **kwargs)

        from unittest.mock import patch

        with patch.object(crawler, "fetch_and_parse", side_effect=observe):
            pages = await crawler.crawl(
                [str(self.server.make_url(f"/{index}")) for index in range(20)]
            )
        self.assertEqual(len(pages), 20)
        self.assertLessEqual(peak, 3)


if __name__ == "__main__":
    unittest.main()
