import argparse
import asyncio
import csv
import json
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

import aiofiles
import aiosqlite
from aiohttp import web

from src.crawler import AsyncCrawler
from src.file_storage import CSVStorage, JSONStorage
from src.sqlite_storage import SQLiteStorage


async def main(output_root: Path) -> None:
    output = output_root / f"day6-{datetime.now(UTC):%Y%m%d-%H%M%S}-{uuid4().hex[:6]}"
    await asyncio.to_thread(output.mkdir, parents=True)

    async def page(request: web.Request) -> web.Response:
        if request.path == "/robots.txt":
            return web.Response(text="User-agent: *\nDisallow: /private/\n")
        return web.Response(
            text=f"<title>Страница {request.path}</title><h1>Книги, данные и Python</h1>"
            '<p>Текст с "кавычками" и запятой, который сохраняется целиком.</p>'
            '<a href="/books">Книги</a><a href="/authors">Авторы</a>'
            '<a href="/private/data">Закрытый раздел</a>',
            content_type="text/html",
        )

    app = web.Application()
    app.router.add_get("/{path:.*}", page)
    runner = web.AppRunner(app)
    await runner.setup()
    crawler: AsyncCrawler | None = None
    try:
        await web.TCPSite(runner, "127.0.0.1", 0).start()
        port = runner.addresses[0][1]
        json_storage = JSONStorage(output / "results.json")
        crawler = AsyncCrawler(
            storage=json_storage, max_depth=1, requests_per_second=20
        )
        pages = await crawler.crawl([f"http://127.0.0.1:{port}/"])
        print("Краулер", crawler.get_storage_stats())
    finally:
        try:
            if crawler is not None:
                await crawler.close()
        finally:
            await runner.cleanup()

    records = [dict(page) for page in pages.values()]
    stores = [
        CSVStorage(output / "results.csv", encoding="utf-8-sig"),
        SQLiteStorage(output / "crawler.db"),
        JSONStorage(output / "results.jsonl", json_lines=True),
    ]
    for storage in stores:
        try:
            await storage.save_many(records)
            print(type(storage).__name__, storage.get_stats())
        finally:
            await storage.close()

    async with aiofiles.open(output / "results.json", encoding="utf-8") as source:
        json_rows = json.loads(await source.read())

    def read_csv() -> list[dict[str, str]]:
        with (output / "results.csv").open(encoding="utf-8-sig", newline="") as source:
            return list(csv.DictReader(source))

    csv_rows = await asyncio.to_thread(read_csv)
    async with (
        aiosqlite.connect(output / "crawler.db") as connection,
        connection.execute("SELECT data FROM pages ORDER BY url") as cursor,
    ):
        sqlite_rows = [json.loads(row[0]) async for row in cursor]
    async with aiofiles.open(output / "results.jsonl", encoding="utf-8") as source:
        jsonl_rows = [json.loads(line) async for line in source]

    expected = {row["url"] for row in json_rows}
    for rows in (csv_rows, sqlite_rows, jsonl_rows):
        if {row["url"] for row in rows} != expected:
            raise RuntimeError("Сохранённые результаты различаются")
    print(f"Прочитано {len(json_rows)} записей из каждого формата")
    print("Пример", json.dumps(json_rows[0], ensure_ascii=False, indent=2))
    print("Результаты", output.resolve())


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Локальная демонстрация сохранения данных"
    )
    parser.add_argument("--output", type=Path, default=Path("results"))
    asyncio.run(main(parser.parse_args().output))
