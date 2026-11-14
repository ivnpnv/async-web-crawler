import asyncio
import errno
import json
import sqlite3
from abc import ABC, abstractmethod
from collections.abc import Awaitable
from datetime import UTC, datetime

from src.html_parser import ParsedPage
from src.retry_strategy import RetryPolicy, RetryStrategy


class CrawledPage(ParsedPage):
    crawled_at: datetime
    status_code: int
    content_type: str
    final_url: str


class StorageError(Exception):
    def __init__(self, message: str, *, url: str = "") -> None:
        super().__init__(message)
        self.url = url


class TransientStorageError(StorageError):
    pass


def serialize_datetime(value: object) -> str:
    if isinstance(value, datetime) and value.utcoffset() is not None:
        return value.astimezone(UTC).isoformat()
    raise TypeError("Поддерживаются только JSON-данные и datetime с часовым поясом")


def normalize_record(data: dict[str, object]) -> dict[str, object]:
    for key in ("url", "title", "text", "content_type"):
        if not isinstance(data.get(key), str):
            raise TypeError(f"Поле {key} должно быть строкой")
    if not data["url"]:
        raise ValueError("URL не должен быть пустым")
    if not isinstance(data.get("links"), list) or any(
        not isinstance(link, str) for link in data["links"]
    ):
        raise ValueError("links должен содержать список строк")
    if not isinstance(data.get("metadata"), dict):
        raise TypeError("metadata должен быть словарём")
    status = data.get("status_code")
    if type(status) is not int or not 100 <= status <= 599:
        raise ValueError("Неверный status_code")
    timestamp = data.get("crawled_at")
    if isinstance(timestamp, str):
        timestamp = datetime.fromisoformat(timestamp)
    if not isinstance(timestamp, datetime) or timestamp.utcoffset() is None:
        raise ValueError("crawled_at должен содержать дату с часовым поясом")
    return json.loads(
        json.dumps(
            {**data, "crawled_at": timestamp.astimezone(UTC)},
            ensure_ascii=False,
            allow_nan=False,
            default=serialize_datetime,
        )
    )


async def finish_io[T](operation: Awaitable[T]) -> T:
    """Завершить начатую запись до освобождения блокировки при отмене задачи."""
    task = asyncio.ensure_future(operation)
    cancelled = False
    while True:
        try:
            result = await asyncio.shield(task)
            break
        except asyncio.CancelledError:
            if task.cancelled():
                raise
            cancelled = True
    if cancelled:
        raise asyncio.CancelledError
    return result


class DataStorage(ABC):
    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._closed = False
        self._saved = 0
        self._failed = 0
        self._retry = RetryStrategy(
            max_retries=2,
            retry_on=[TransientStorageError],
            policies={TransientStorageError: RetryPolicy(2, 0.1)},
        )

    async def save(self, data: dict[str, object]) -> None:
        await self.save_many([data])

    async def save_many(self, records: list[dict[str, object]]) -> None:
        async with self._lock:
            if self._closed:
                raise StorageError("Хранилище уже закрыто")
            try:
                normalized = await asyncio.to_thread(
                    lambda: [normalize_record(record) for record in records]
                )
            except (TypeError, ValueError) as error:
                self._failed += len(records)
                classified = StorageError(str(error))
                self._retry.record_error(classified)
                raise classified from error
            if normalized:
                await finish_io(self._save_batch(normalized))

    async def _save_batch(self, records: list[dict[str, object]]) -> None:
        try:
            await self._retry.execute_with_retry(self._write_attempt, records)
        except StorageError:
            self._failed += len(records)
            raise
        self._saved += len(records)

    async def _write_attempt(self, records: list[dict[str, object]]) -> None:
        try:
            await self._write(records)
        except (OSError, sqlite3.Error, UnicodeError) as error:
            temporary = (
                isinstance(error, OSError)
                and (
                    error.errno
                    in (errno.EAGAIN, errno.EBUSY, errno.EINTR, errno.ETIMEDOUT)
                    or getattr(error, "winerror", None) in (32, 33)
                )
            ) or (
                isinstance(error, sqlite3.OperationalError)
                and getattr(error, "sqlite_errorcode", 0) & 0xFF
                in (sqlite3.SQLITE_BUSY, sqlite3.SQLITE_LOCKED)
            )
            cls = TransientStorageError if temporary else StorageError
            raise cls(str(error), url=str(records[0]["url"])) from error

    async def close(self) -> None:
        async with self._lock:
            if not self._closed:
                await finish_io(self._finish_close())

    async def _finish_close(self) -> None:
        await self._close()
        self._closed = True

    def get_stats(self) -> dict[str, object]:
        return {
            "saved_records": self._saved,
            "failed_records": self._failed,
            "retry_stats": self._retry.get_stats(),
        }

    @abstractmethod
    async def _write(self, records: list[dict[str, object]]) -> None:
        raise NotImplementedError

    async def _close(self) -> None:
        pass
