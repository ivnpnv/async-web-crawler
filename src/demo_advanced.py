import argparse
import asyncio
import json
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

from aiohttp import web

from src.advanced_crawler import AdvancedCrawler
from src.config import CrawlerConfig


async def main(output_root: Path) -> None:
    directory = (
        output_root / f"advanced-{datetime.now(UTC):%Y%m%d-%H%M%S}-{uuid4().hex[:6]}"
    )
    attempts = 0

    async def page(request: web.Request) -> web.Response:
        nonlocal attempts
        base = f"http://{request.host}"
        if request.path == "/robots.txt":
            return web.Response(text="User-agent: *\nDisallow: /private/\n")
        if request.path == "/sitemap.xml":
            return web.Response(
                text=f'<sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9"><sitemap><loc>{base}/pages.xml</loc></sitemap></sitemapindex>',
                content_type="application/xml",
            )
        if request.path == "/pages.xml":
            urls = ["/", "/flaky", "/missing", "/private/data"]
            xml = '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">'
            xml += (
                "".join(f"<url><loc>{base}{path}</loc></url>" for path in urls)
                + "</urlset>"
            )
            return web.Response(text=xml, content_type="application/xml")
        if request.path == "/missing":
            raise web.HTTPNotFound()
        if request.path == "/flaky":
            attempts += 1
            if attempts == 1:
                return web.Response(status=503)
        return web.Response(
            text='<title>Демо</title><h1>Краулер</h1><a href="/books">Книги</a>',
            content_type="text/html",
        )

    app = web.Application()
    app.router.add_get("/{path:.*}", page)
    runner = web.AppRunner(app)
    await runner.setup()
    crawler: AdvancedCrawler | None = None
    try:
        await web.TCPSite(runner, "127.0.0.1", 0).start()
        port = runner.addresses[0][1]
        config = CrawlerConfig(
            sitemap_urls=[f"http://127.0.0.1:{port}/sitemap.xml"],
            max_depth=1,
            rate_limit=30,
            progress_interval=0.25,
            output=str(directory / "pages.sqlite"),
            log_file=str(directory / "crawler.log"),
            stats_json=str(directory / "stats.json"),
            report_html=str(directory / "report.html"),
        )
        crawler = AdvancedCrawler(config)
        await crawler.crawl()
        await asyncio.to_thread(crawler.export_to_json, config.stats_json)
        await asyncio.to_thread(crawler.export_to_html_report, config.report_html)
        print(json.dumps(crawler.get_stats(), ensure_ascii=False, indent=2))
        print("Отчёт", Path(config.report_html).resolve())
    finally:
        try:
            if crawler is not None:
                await crawler.close()
        finally:
            await runner.cleanup()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Полная локальная демонстрация краулера"
    )
    parser.add_argument("--output", type=Path, default=Path("results"))
    asyncio.run(main(parser.parse_args().output))
