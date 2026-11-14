import asyncio
import codecs
import csv
import io
import json
import logging
import os
from abc import abstractmethod
from pathlib import Path
from uuid import uuid4

import aiofiles
from aiofiles.threadpool.binary import AsyncBufferedIOBase

from src.data_storage import DataStorage, StorageError

logger = logging.getLogger(__name__)
CHUNK_SIZE = 64 * 1024


class _FileStorage(DataStorage):
    def __init__(self, path: str | Path) -> None:
        super().__init__()
        self.path = Path(path)

    async def _write(self, records: list[dict[str, object]]) -> None:
        await asyncio.to_thread(self.path.parent.mkdir, parents=True, exist_ok=True)
        temporary = self.path.with_name(f".{self.path.name}.{uuid4().hex}.tmp")
        try:
            async with aiofiles.open(temporary, "xb", buffering=CHUNK_SIZE) as output:
                await self._compose(records, output)
                await output.flush()
            await asyncio.to_thread(os.replace, temporary, self.path)
        finally:
            try:
                await asyncio.to_thread(temporary.unlink, missing_ok=True)
            except OSError:
                logger.warning("Не удалось удалить временный файл %s", temporary)

    async def _copy(
        self, output: AsyncBufferedIOBase, limit: int | None = None
    ) -> None:
        async with aiofiles.open(self.path, "rb") as source:
            while limit is None or limit > 0:
                chunk = await source.read(
                    CHUNK_SIZE if limit is None else min(limit, CHUNK_SIZE)
                )
                if not chunk:
                    break
                await output.write(chunk)
                if limit is not None:
                    limit -= len(chunk)

    @abstractmethod
    async def _compose(
        self, records: list[dict[str, object]], output: AsyncBufferedIOBase
    ) -> None:
        raise NotImplementedError


class JSONStorage(_FileStorage):
    def __init__(
        self, path: str | Path, *, indent: int | None = 2, json_lines: bool = False
    ) -> None:
        super().__init__(path)
        if indent is not None and indent < 0:
            raise ValueError("indent должен быть неотрицательным")
        self.indent = indent
        self.json_lines = json_lines

    async def _compose(
        self, records: list[dict[str, object]], output: AsyncBufferedIOBase
    ) -> None:
        size = (
            (await asyncio.to_thread(self.path.stat)).st_size
            if await asyncio.to_thread(self.path.exists)
            else 0
        )
        nonempty = False
        if size:
            if self.json_lines:
                await self._copy(output)
                async with aiofiles.open(self.path, "rb") as source:
                    await source.seek(-1, os.SEEK_END)
                    if await source.read(1) != b"\n":
                        await output.write(b"\n")
            else:
                async with aiofiles.open(self.path, "rb") as source:
                    if await source.read(1) != b"[":
                        raise StorageError("JSON-файл должен содержать массив")
                    await source.seek(max(0, size - CHUNK_SIZE))
                    raw_tail = await source.read()
                    tail = raw_tail.rstrip()
                    if not tail.endswith(b"]"):
                        raise StorageError("JSON-файл не завершён, запись остановлена")
                    prefix_length = size - len(raw_tail) + len(raw_tail.rstrip()) - 1
                    nonempty = tail[:-1].rstrip() != b"["
                await self._copy(output, prefix_length)
        elif not self.json_lines:
            await output.write(b"[\n")

        for record in records:
            if nonempty:
                await output.write(b",\n")
            text = await asyncio.to_thread(
                json.dumps,
                record,
                ensure_ascii=False,
                allow_nan=False,
                indent=None if self.json_lines else self.indent,
            )
            await output.write(
                text.encode("utf-8") + (b"\n" if self.json_lines else b"")
            )
            nonempty = not self.json_lines
        if not self.json_lines:
            await output.write(b"\n]\n")


class CSVStorage(_FileStorage):
    def __init__(self, path: str | Path, *, encoding: str = "utf-8") -> None:
        super().__init__(path)
        self.encoding = encoding

    def _encode_rows(self, records: list[dict[str, object]], existing: bool) -> bytes:
        fields = list(records[0])
        if existing:
            with self.path.open(encoding=self.encoding, newline="") as source:
                fields = next(csv.reader(source))
        if any(set(record) != set(fields) for record in records):
            raise StorageError("Набор полей CSV отличается от заголовка")
        buffer = io.StringIO(newline="")
        writer = csv.DictWriter(buffer, fieldnames=fields)
        if not existing:
            writer.writeheader()
        for record in records:
            writer.writerow(
                {
                    key: json.dumps(value, ensure_ascii=False)
                    if isinstance(value, (list, dict))
                    else value
                    for key, value in record.items()
                }
            )
        encoder = codecs.getincrementalencoder(self.encoding)()
        if existing:
            encoder.encode("")  # BOM уже находится в начале существующего файла.
        return encoder.encode(buffer.getvalue(), final=True)

    async def _compose(
        self, records: list[dict[str, object]], output: AsyncBufferedIOBase
    ) -> None:
        existing = (
            await asyncio.to_thread(self.path.exists)
            and (await asyncio.to_thread(self.path.stat)).st_size > 0
        )
        payload = await asyncio.to_thread(self._encode_rows, records, existing)
        if existing:
            await self._copy(output)
        await output.write(payload)
