import argparse
import asyncio
import json
import sys
from dataclasses import replace

from src.advanced_crawler import AdvancedCrawler
from src.config import CrawlerConfig

__all__ = ["AdvancedCrawler"]


def parse_config(argv: list[str] | None = None) -> CrawlerConfig:
    parser = argparse.ArgumentParser(description="Асинхронный веб-краулер")
    parser.add_argument("--config", help="JSON-конфигурация")
    parser.add_argument("--urls", nargs="+", help="Стартовые URL")
    parser.add_argument(
        "--sitemaps", nargs="+", dest="sitemap_urls", help="URL sitemap"
    )
    for flag, value_type in (
        ("max-pages", int),
        ("max-depth", int),
        ("max-concurrent", int),
        ("rate-limit", float),
    ):
        parser.add_argument(f"--{flag}", type=value_type)
    parser.add_argument("--output", help="Файл json/jsonl/csv/sqlite")
    parser.add_argument(
        "--respect-robots", action=argparse.BooleanOptionalAction, default=None
    )
    parser.add_argument(
        "--same-domain-only", action=argparse.BooleanOptionalAction, default=None
    )
    parser.add_argument("--include-patterns", nargs="+")
    parser.add_argument("--exclude-patterns", nargs="+")
    parser.add_argument("--log-level", choices=("DEBUG", "INFO", "WARNING", "ERROR"))
    args = vars(parser.parse_args(argv))
    filename = args.pop("config")
    try:
        config = CrawlerConfig.from_file(filename) if filename else CrawlerConfig()
        config = replace(
            config, **{key: value for key, value in args.items() if value is not None}
        )
        if not config.urls and not config.sitemap_urls:
            parser.error("Укажите --urls или --sitemaps либо задайте их в конфигурации")
        return config
    except (OSError, ValueError, TypeError) as error:
        parser.error(str(error))


async def run(config: CrawlerConfig) -> int:
    crawler = AdvancedCrawler(config)
    try:
        await crawler.crawl()
        await asyncio.to_thread(crawler.export_to_json, config.stats_json)
        await asyncio.to_thread(crawler.export_to_html_report, config.report_html)
        stats = crawler.get_stats()
        print(json.dumps(stats, ensure_ascii=False, indent=2))
        return int(
            bool(
                stats["failed"]
                or stats["sitemap_errors"]
                or crawler.crawler.storage_errors
            )
        )
    finally:
        await crawler.close()


if __name__ == "__main__":
    try:
        raise SystemExit(asyncio.run(run(parse_config())))
    except KeyboardInterrupt:
        raise SystemExit(130) from None
    except (OSError, ValueError, RuntimeError) as error:
        print(str(error), file=sys.stderr)
        raise SystemExit(1) from None
