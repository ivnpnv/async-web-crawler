import argparse
import asyncio
import gc
import http.client
import json
import tracemalloc
from pathlib import Path
from time import perf_counter

from aiohttp import web

from src.crawler import AsyncCrawler
from src.crawler_stats import atomic_text
from src.html_parser import HTMLParser, ParsedPage


def synchronous_crawl(port: int, size: int) -> list[ParsedPage]:
    async def collect() -> list[ParsedPage]:
        connection = http.client.HTTPConnection("127.0.0.1", port, timeout=10)
        parser = HTMLParser()
        pages = []
        try:
            for index in range(size):
                connection.request("GET", f"/{index}")
                response = connection.getresponse()
                if response.status != 200:
                    raise RuntimeError(f"HTTP {response.status}")
                html = response.read().decode("utf-8")
                pages.append(
                    await parser.parse_html(html, f"http://127.0.0.1:{port}/{index}")
                )
        finally:
            connection.close()
        return pages

    return asyncio.run(collect())


async def benchmark(
    sizes: list[int], delay: float, concurrency: int
) -> dict[str, object]:
    async def page(_request: web.Request) -> web.Response:
        await asyncio.sleep(delay)
        return web.Response(
            text="<title>Benchmark</title><h1>Page</h1><p>" + "data " * 100 + "</p>",
            content_type="text/html",
        )

    app = web.Application()
    app.router.add_get("/{index}", page)
    runner = web.AppRunner(app, access_log=None)
    await runner.setup()
    rows = []
    try:
        await web.TCPSite(runner, "127.0.0.1", 0).start()
        port = runner.addresses[0][1]
        # Прогрев импортов и парсера выполняется вне измерений.
        warmup = AsyncCrawler(
            max_depth=0, requests_per_second=1_000_000, respect_robots=False
        )
        await warmup.crawl([f"http://127.0.0.1:{port}/warmup"])
        for size in sizes:
            row: dict[str, object] = {"pages": size}
            for mode in ("sync", "async"):
                gc.collect()
                tracemalloc.start()
                started = perf_counter()
                try:
                    if mode == "sync":
                        pages = await asyncio.to_thread(synchronous_crawl, port, size)
                    else:
                        crawler = AsyncCrawler(
                            max_concurrent=concurrency,
                            max_per_domain=concurrency,
                            requests_per_second=1_000_000,
                            respect_robots=False,
                            max_depth=0,
                        )
                        pages = await crawler.crawl(
                            [
                                f"http://127.0.0.1:{port}/{index}"
                                for index in range(size)
                            ],
                            max_pages=size,
                        )
                    duration = perf_counter() - started
                    _, peak = tracemalloc.get_traced_memory()
                    if len(pages) != size:
                        raise RuntimeError("Потеряны страницы при измерении")
                finally:
                    tracemalloc.stop()
                row[f"{mode}_seconds"] = round(duration, 4)
                row[f"{mode}_python_peak_mib"] = round(peak / 1024**2, 3)
                del pages
                if mode == "async":
                    del crawler
            row["speedup"] = round(row["sync_seconds"] / row["async_seconds"], 2)
            rows.append(row)
            print(json.dumps(row), flush=True)
    finally:
        await runner.cleanup()
    return {
        "server_delay_seconds": delay,
        "max_concurrent": concurrency,
        "memory_measurement": "tracemalloc Python allocations, including local server; not process RSS",
        "method": "Single run per size, keep-alive in both clients, shared HTML parser, no storage or rate bottleneck",
        "results": rows,
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Локальное сравнение sync и async загрузок"
    )
    parser.add_argument("--sizes", nargs="+", type=int, default=[100, 500, 1000])
    parser.add_argument("--delay", type=float, default=0.01)
    parser.add_argument("--concurrency", type=int, default=20)
    parser.add_argument("--output", type=Path, default=Path("results/benchmark.json"))
    args = parser.parse_args()
    if any(size < 1 for size in args.sizes) or args.delay < 0 or args.concurrency < 1:
        parser.error(
            "Размеры и конкурентность должны быть положительными, задержка неотрицательной"
        )
    result = asyncio.run(benchmark(args.sizes, args.delay, args.concurrency))
    atomic_text(args.output, json.dumps(result, ensure_ascii=False, indent=2))
