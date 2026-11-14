import asyncio
import csv
import errno
import json
import tempfile
import unittest
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import patch

import aiosqlite
from aiohttp import web
from aiohttp.test_utils import TestServer

from src.crawler import AsyncCrawler
from src.data_storage import StorageError
from src.file_storage import CSVStorage, JSONStorage
from src.sqlite_storage import SQLiteStorage


def record(number: int = 1) -> dict[str, object]:
    return {
        "url": f"https://example.test/{number}",
        "title": 'Книга, "Python"',
        "text": "Первая строка\nВторая строка",
        "links": ["https://example.test/books?author=1&title=test"],
        "metadata": {"description": "текст"},
        "crawled_at": datetime(2026, 11, 14, 12, tzinfo=UTC),
        "status_code": 200,
        "content_type": "text/html; charset=utf-8",
    }


class StorageTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.directory = Path(self.temporary.name)

    async def asyncTearDown(self) -> None:
        self.temporary.cleanup()

    async def test_json_array_jsonl_concurrency_and_reopen(self) -> None:
        for json_lines in (False, True):
            path = self.directory / ("pages.jsonl" if json_lines else "pages.json")
            storage = JSONStorage(path, json_lines=json_lines)
            await asyncio.gather(*(storage.save(record(i)) for i in range(4)))
            await storage.close()
            storage = JSONStorage(path, json_lines=json_lines)
            await storage.save_many([record(4), record(5)])
            await storage.close()
            text = path.read_text(encoding="utf-8")
            rows = (
                [json.loads(line) for line in text.splitlines()]
                if json_lines
                else json.loads(text)
            )
            self.assertEqual(len(rows), 6)
            self.assertEqual(
                {row["url"] for row in rows}, {record(i)["url"] for i in range(6)}
            )
            self.assertEqual(rows[0]["metadata"], record()["metadata"])
            self.assertEqual(rows[0]["crawled_at"], "2026-11-14T12:00:00+00:00")

    async def test_csv_round_trip_encodings_and_schema(self) -> None:
        for encoding in ("utf-8-sig", "utf-16", "cp1251"):
            path = self.directory / f"pages-{encoding}.csv"
            storage = CSVStorage(path, encoding=encoding)
            await storage.save(record())
            await storage.save(record(2))
            before = path.read_bytes()
            with self.assertRaises(StorageError):
                await storage.save({**record(3), "new_column": "value"})
            await storage.close()
            self.assertEqual(path.read_bytes(), before)
            with path.open(encoding=encoding, newline="") as source:
                rows = list(csv.DictReader(source))
            self.assertEqual(len(rows), 2)
            self.assertEqual(rows[0]["text"], record()["text"])
            self.assertEqual(rows[0]["title"], record()["title"])
            self.assertEqual(json.loads(rows[0]["links"]), record()["links"])

    async def test_atomic_failure_and_retry_without_duplicate(self) -> None:
        path = self.directory / "pages.json"
        storage = JSONStorage(path)
        await storage.save(record())
        original = path.read_bytes()
        with (
            patch(
                "src.file_storage.os.replace",
                side_effect=OSError(errno.ENOSPC, "disk full"),
            ),
            self.assertRaises(StorageError),
        ):
            await storage.save(record(2))
        self.assertEqual(path.read_bytes(), original)
        import os

        replace = os.replace
        count = 0

        def busy_once(source: Path, target: Path) -> None:
            nonlocal count
            count += 1
            if count == 1:
                raise OSError(errno.EBUSY, "busy")
            replace(source, target)

        with patch("src.file_storage.os.replace", side_effect=busy_once):
            await storage.save(record(2))
        await storage.close()
        self.assertEqual(len(json.loads(path.read_text(encoding="utf-8"))), 2)
        self.assertEqual(count, 2)
        self.assertEqual(storage.get_stats()["saved_records"], 2)
        self.assertEqual(storage.get_stats()["failed_records"], 1)
        self.assertFalse(list(self.directory.glob("*.tmp")))

    async def test_sqlite_batch_upsert_indexes_and_rollback(self) -> None:
        path = self.directory / "pages.db"
        storage = SQLiteStorage(path)
        await storage.init_db()
        await storage.save_many([record(1), record(2)])
        await storage.save({**record(1), "title": "Updated"})
        async with aiosqlite.connect(path) as connection:
            await connection.execute("""
                CREATE TRIGGER reject_page BEFORE INSERT ON pages
                WHEN NEW.url = 'https://example.test/4'
                BEGIN SELECT RAISE(ABORT, 'test failure'); END
            """)
            await connection.commit()
        with self.assertRaises(StorageError):
            await storage.save_many([record(3), record(4)])
        await storage.close()
        async with aiosqlite.connect(path) as connection:
            async with connection.execute(
                "SELECT data FROM pages ORDER BY url"
            ) as cursor:
                rows = [json.loads(row[0]) async for row in cursor]
            async with connection.execute("PRAGMA index_list(pages)") as cursor:
                indexes = [row[1] async for row in cursor]
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]["title"], "Updated")
        self.assertEqual(rows[1]["text"], record()["text"])
        self.assertIn("idx_pages_crawled_at", indexes)
        self.assertIn("idx_pages_status", indexes)
        with self.assertRaises(StorageError):
            await storage.save(record())

    async def test_cancel_waits_for_atomic_write_before_unlocking(self) -> None:
        storage = JSONStorage(self.directory / "cancel.json")
        started, proceed = asyncio.Event(), asyncio.Event()
        original = storage._write

        async def slow_write(records: list[dict[str, object]]) -> None:
            started.set()
            await proceed.wait()
            await original(records)

        with patch.object(storage, "_write", side_effect=slow_write):
            task = asyncio.create_task(storage.save(record()))
            await started.wait()
            task.cancel()
            await asyncio.sleep(0)
            self.assertFalse(task.done())
            proceed.set()
            with self.assertRaises(asyncio.CancelledError):
                await task
        await storage.save(record(2))
        await storage.close()
        self.assertEqual(storage.get_stats()["saved_records"], 2)

    async def test_invalid_record_does_not_modify_file(self) -> None:
        path = self.directory / "validated.json"
        storage = JSONStorage(path)
        await storage.save(record())
        original = path.read_bytes()
        with self.assertRaises(StorageError):
            await storage.save(
                {
                    **record(2),
                    "crawled_at": datetime(2026, 11, 14, tzinfo=UTC).replace(
                        tzinfo=None
                    ),
                }
            )
        await storage.close()
        self.assertEqual(path.read_bytes(), original)
        self.assertEqual(storage.get_stats()["failed_records"], 1)

    async def test_cancelled_close_still_marks_storage_closed(self) -> None:
        storage = JSONStorage(self.directory / "closed.json")
        started, proceed = asyncio.Event(), asyncio.Event()

        async def slow_close() -> None:
            started.set()
            await proceed.wait()

        with patch.object(storage, "_close", side_effect=slow_close):
            task = asyncio.create_task(storage.close())
            await started.wait()
            task.cancel()
            proceed.set()
            with self.assertRaises(asyncio.CancelledError):
                await task
        with self.assertRaises(StorageError):
            await storage.save(record())

    async def test_crawler_persists_metadata_and_continues_on_storage_error(
        self,
    ) -> None:
        async def handler(request: web.Request) -> web.Response:
            if request.path == "/robots.txt":
                return web.Response(status=404)
            return web.Response(
                text='<title>Test</title><a href="/next">Next</a>',
                content_type="text/html",
            )

        app = web.Application()
        app.router.add_get("/{path:.*}", handler)
        storage = JSONStorage(self.directory / "crawl.json")
        crawler = AsyncCrawler(storage=storage, max_depth=1, requests_per_second=1000)
        try:
            async with TestServer(app) as server:
                pages = await crawler.crawl([str(server.make_url("/"))])
                self.assertEqual(len(pages), 2)
                self.assertEqual(len(crawler.saved_urls), 2)
                self.assertTrue(
                    all(page["status_code"] == 200 for page in pages.values())
                )
                self.assertTrue(
                    all("text/html" in page["content_type"] for page in pages.values())
                )
                with patch.object(
                    storage, "_write", side_effect=OSError(errno.ENOSPC, "disk full")
                ):
                    pages = await crawler.crawl([str(server.make_url("/"))])
                self.assertEqual(len(pages), 2)
                self.assertEqual(len(crawler.storage_errors), 2)
                self.assertEqual(crawler.failed_urls, {})
        finally:
            await crawler.close()
        self.assertEqual(
            len(
                json.loads((self.directory / "crawl.json").read_text(encoding="utf-8"))
            ),
            2,
        )


if __name__ == "__main__":
    unittest.main()
