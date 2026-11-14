import asyncio
import logging
import re
from collections import defaultdict
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from fnmatch import translate
from math import isfinite
from time import monotonic

import aiohttp
from yarl import URL

from src.errors import PermanentError

logger = logging.getLogger(__name__)


@dataclass
class RobotsGroup:
    rules: list[tuple[re.Pattern[str], int, bool]] = field(default_factory=list)
    crawl_delay: float = 0.0


class RobotsParser:
    def __init__(self, download: Callable[[str], Awaitable[tuple[int, str]]]) -> None:
        self._download = download
        self._cache: dict[str, tuple[float, dict[str, list[RobotsGroup]]]] = {}
        self._locks: defaultdict[str, asyncio.Lock] = defaultdict(asyncio.Lock)
        self._last_origin: str | None = None

    async def fetch_robots(self, base_url: str) -> dict[str, list[RobotsGroup]]:
        origin = str(URL(base_url).origin())
        async with self._locks[origin]:
            cached = self._cache.get(origin)
            if cached is None or cached[0] <= monotonic():
                rules, ttl = await self._load_rules(origin)
                self._cache[origin] = (monotonic() + ttl, rules)
            self._last_origin = origin
            return self._cache[origin][1]

    async def _load_rules(
        self, origin: str
    ) -> tuple[dict[str, list[RobotsGroup]], int]:
        url = f"{origin}/robots.txt"
        try:
            status, body = await self._download(url)
        except PermanentError as error:
            if error.status in (404, 410):
                return {}, 86_400
            logger.warning("Не удалось загрузить %s (%s)", url, error)
        except (TimeoutError, aiohttp.ClientError, ValueError) as error:
            logger.warning("Не удалось загрузить %s (%s)", url, type(error).__name__)
        else:
            if 200 <= status < 300:
                return self._parse(body), 86_400
            if status in (404, 410):
                return {}, 86_400
            logger.warning("Не удалось загрузить %s (HTTP %d)", url, status)

        return self._parse("User-agent: *\nDisallow: /"), 60

    @staticmethod
    def _parse(text: str) -> dict[str, list[RobotsGroup]]:
        groups: dict[str, list[RobotsGroup]] = {}
        group: RobotsGroup | None = None
        has_rules = False
        for line in text.lstrip("\ufeff").splitlines():
            key, _, value = line.partition("#")[0].partition(":")
            key, value = key.strip().lower(), value.strip()
            if key == "user-agent" and value:
                if group is None or has_rules:
                    group = RobotsGroup()
                    has_rules = False
                groups.setdefault(value.lower(), []).append(group)
            elif group is not None and key in ("allow", "disallow", "crawl-delay"):
                has_rules = True
                if key == "crawl-delay":
                    try:
                        delay = float(value)
                    except ValueError:
                        continue
                    if isfinite(delay) and delay >= 0:
                        group.crawl_delay = max(group.crawl_delay, delay)
                elif value.startswith("/"):
                    # URL normalizes escapes the same way for rules and requests.
                    path = URL(
                        f"http://robots.invalid{value.removesuffix('$')}"
                    ).raw_path_qs
                    glob = path.replace("[", "[[]").replace("?", "[?]")
                    if not value.endswith("$"):
                        glob += "*"
                    group.rules.append(
                        (re.compile(translate(glob)), len(path), key == "allow")
                    )
        return groups

    def _groups(self, origin: str, user_agent: str) -> list[RobotsGroup]:
        rules = self._cache[origin][1]
        token = user_agent.strip().split(maxsplit=1)
        product = token[0].partition("/")[0].lower() if token else "*"
        return rules.get(product, rules.get("*", []))

    def can_fetch(self, url: str, user_agent: str = "*") -> bool:
        target = URL(url)
        if target.raw_path == "/robots.txt":
            return True
        origin = str(target.origin())
        if origin not in self._cache:
            return False

        matches = (
            (length, allowed)
            for group in self._groups(origin, user_agent)
            for pattern, length, allowed in group.rules
            if pattern.match(target.raw_path_qs)
        )
        return max(matches, default=(0, True))[1]

    def get_crawl_delay(
        self, user_agent: str = "*", *, base_url: str | None = None
    ) -> float:
        origin = (
            str(URL(base_url).origin()) if base_url is not None else self._last_origin
        )
        if origin is None or origin not in self._cache:
            return 0.0
        return max(
            (group.crawl_delay for group in self._groups(origin, user_agent)),
            default=0.0,
        )
