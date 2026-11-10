from heapq import heappop, heappush


class CrawlerQueue:
    def __init__(self) -> None:
        self._pending: list[tuple[int, int, str]] = []
        self._seen: set[str] = set()
        self._in_progress: set[str] = set()
        self._processed: set[str] = set()
        self._failed: dict[str, str] = {}
        self._next_order: int = 0

    def add_url(self, url: str, priority: int = 0) -> None:
        """Поставить URL в очередь, если его ещё не добавляли."""
        if url in self._seen:
            return
        heappush(self._pending, (-priority, self._next_order, url))
        self._seen.add(url)
        self._next_order += 1

    async def get_next(self) -> str | None:
        """Забрать следующий URL или вернуть None, если очередь пуста."""
        if not self._pending:
            return None
        _, _, url = heappop(self._pending)
        self._in_progress.add(url)
        return url

    def mark_processed(self, url: str) -> None:
        """Отметить взятый из очереди URL как обработанный."""
        if url in self._in_progress:
            self._in_progress.remove(url)
            self._processed.add(url)

    def mark_failed(self, url: str, error: str) -> None:
        """Отметить взятый из очереди URL как неудачный и сохранить ошибку."""
        if url in self._in_progress:
            self._in_progress.remove(url)
            self._failed[url] = error

    def get_stats(self) -> dict[str, int]:
        """Вернуть счётчики ожидающих, обработанных и неудачных URL."""
        return {
            "pending": len(self._pending),
            "processed": len(self._processed),
            "failed": len(self._failed),
        }
