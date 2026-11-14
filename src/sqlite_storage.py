import asyncio
import json
from pathlib import Path

import aiosqlite

from src.data_storage import DataStorage, StorageError, finish_io


class SQLiteStorage(DataStorage):
    def __init__(self, path: str | Path) -> None:
        super().__init__()
        self.path = Path(path)
        self._connection: aiosqlite.Connection | None = None

    async def init_db(self) -> None:
        async with self._lock:
            if self._closed:
                raise StorageError("Хранилище уже закрыто")
            await finish_io(self._connect())

    async def _connect(self) -> None:
        if self._connection is not None:
            return
        await asyncio.to_thread(self.path.parent.mkdir, parents=True, exist_ok=True)
        connection = await aiosqlite.connect(str(self.path), timeout=1.0)
        try:
            await connection.executescript("""
                CREATE TABLE IF NOT EXISTS pages (
                    url TEXT PRIMARY KEY,
                    title TEXT NOT NULL,
                    crawled_at TEXT NOT NULL,
                    status_code INTEGER NOT NULL,
                    content_type TEXT NOT NULL,
                    data TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_pages_crawled_at ON pages(crawled_at);
                CREATE INDEX IF NOT EXISTS idx_pages_status ON pages(status_code);
            """)
            await connection.commit()
        except BaseException:
            await connection.close()
            raise
        self._connection = connection

    async def _write(self, records: list[dict[str, object]]) -> None:
        await self._connect()
        connection = self._connection
        assert connection is not None
        rows = await asyncio.to_thread(
            lambda: [
                (
                    record["url"],
                    record["title"],
                    record["crawled_at"],
                    record["status_code"],
                    record["content_type"],
                    json.dumps(record, ensure_ascii=False, allow_nan=False),
                )
                for record in records
            ]
        )
        try:
            async with connection.executemany(
                """
                INSERT INTO pages(url, title, crawled_at, status_code, content_type, data)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(url) DO UPDATE SET
                    title=excluded.title, crawled_at=excluded.crawled_at,
                    status_code=excluded.status_code, content_type=excluded.content_type,
                    data=excluded.data
            """,
                rows,
            ):
                pass
            await connection.commit()
        except BaseException:
            await connection.rollback()
            raise

    async def _close(self) -> None:
        if self._connection is not None:
            await self._connection.close()
            self._connection = None
