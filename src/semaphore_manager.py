import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from urllib.parse import urlsplit


class SemaphoreManager:
    def __init__(self, max_concurrent: int = 10, max_per_domain: int = 2) -> None:
        if max_concurrent < 1 or max_per_domain < 1:
            raise ValueError("Лимиты конкурентности должны быть положительными")

        self._global_semaphore = asyncio.Semaphore(max_concurrent)
        self._max_per_domain = max_per_domain
        self._domain_semaphores: dict[str, asyncio.Semaphore] = {}
        self._active_tasks = 0

    @property
    def active_tasks(self) -> int:
        return self._active_tasks

    @asynccontextmanager
    async def limit(self, url: str) -> AsyncIterator[None]:
        """Выделить место для запроса по глобальному и доменному лимитам."""
        domain = (urlsplit(url).hostname or "").rstrip(".")
        if not domain:
            raise ValueError("URL должен содержать имя хоста")

        domain_semaphore = self._domain_semaphores.get(domain)
        if domain_semaphore is None:
            domain_semaphore = asyncio.Semaphore(self._max_per_domain)
            self._domain_semaphores[domain] = domain_semaphore

        async with domain_semaphore, self._global_semaphore:
            self._active_tasks += 1
            try:
                yield
            finally:
                self._active_tasks -= 1
