import json
import re
from dataclasses import dataclass, fields
from math import isfinite
from pathlib import Path

from yarl import URL


@dataclass
class CrawlerConfig:
    urls: list[str] | None = None
    sitemap_urls: list[str] | None = None
    max_pages: int = 100
    max_depth: int = 2
    max_concurrent: int = 10
    max_per_domain: int = 2
    rate_limit: float = 1.0
    min_delay: float = 0.0
    jitter: float = 0.0
    respect_robots: bool = True
    same_domain_only: bool = True
    include_patterns: list[str] | None = None
    exclude_patterns: list[str] | None = None
    user_agent: str = "ItGrindCrawler/1.0"
    connect_timeout: float = 5.0
    read_timeout: float = 10.0
    total_timeout: float = 30.0
    max_retries: int = 3
    backoff_factor: float = 2.0
    output: str = "results/pages.json"
    encoding: str = "utf-8"
    log_file: str = "results/crawler.log"
    log_level: str = "INFO"
    log_max_bytes: int = 1_000_000
    log_backup_count: int = 3
    stats_json: str = "results/stats.json"
    report_html: str = "results/report.html"
    progress_interval: float = 1.0

    def __post_init__(self) -> None:
        for name in ("urls", "sitemap_urls", "include_patterns", "exclude_patterns"):
            value = getattr(self, name)
            if value is None:
                setattr(self, name, [])
            elif not isinstance(value, list) or any(
                not isinstance(item, str) for item in value
            ):
                raise ValueError(f"{name} должен быть списком строк")
        for url in self.urls + self.sitemap_urls:
            target = URL(url)
            if target.scheme not in ("http", "https") or not target.host:
                raise ValueError(f"Неверный URL: {url}")
        for name in ("max_pages", "max_depth", "max_retries", "log_backup_count"):
            if type(getattr(self, name)) is not int or getattr(self, name) < 0:
                raise ValueError(f"{name} должен быть целым неотрицательным числом")
        for name in ("max_concurrent", "max_per_domain", "log_max_bytes"):
            if type(getattr(self, name)) is not int or getattr(self, name) < 1:
                raise ValueError(f"{name} должен быть целым положительным числом")
        for name in (
            "rate_limit",
            "connect_timeout",
            "read_timeout",
            "total_timeout",
            "progress_interval",
            "min_delay",
            "jitter",
            "backoff_factor",
        ):
            value = getattr(self, name)
            if type(value) not in (int, float) or not isfinite(value) or value < 0:
                raise ValueError(f"Неверное значение {name}")
            if name not in ("min_delay", "jitter") and value == 0:
                raise ValueError(f"{name} должен быть положительным")
        if self.backoff_factor < 1:
            raise ValueError("backoff_factor должен быть не меньше 1")
        for name in ("respect_robots", "same_domain_only"):
            if type(getattr(self, name)) is not bool:
                raise ValueError(f"{name} должен быть boolean")
        for name in (
            "output",
            "encoding",
            "log_file",
            "log_level",
            "stats_json",
            "report_html",
            "user_agent",
        ):
            if (
                not isinstance(getattr(self, name), str)
                or not getattr(self, name).strip()
            ):
                raise ValueError(f"{name} должен быть непустой строкой")
        if self.log_level not in ("DEBUG", "INFO", "WARNING", "ERROR"):
            raise ValueError("Неизвестный уровень логирования")
        if Path(self.output).suffix.lower() not in (
            ".json",
            ".jsonl",
            ".csv",
            ".db",
            ".sqlite",
            ".sqlite3",
        ):
            raise ValueError(
                "Формат output определяется расширением json/jsonl/csv/db/sqlite"
            )
        paths = [
            Path(getattr(self, name)).resolve()
            for name in ("output", "log_file", "stats_json", "report_html")
        ]
        if len(set(paths)) != len(paths):
            raise ValueError("Файлы результатов, логов и отчётов должны различаться")
        for pattern in self.include_patterns + self.exclude_patterns:
            re.compile(pattern)

    @classmethod
    def from_file(cls, filename: str | Path) -> "CrawlerConfig":
        path = Path(filename).resolve()
        values = json.loads(path.read_text(encoding="utf-8-sig"))
        if not isinstance(values, dict):
            raise TypeError("Конфигурация должна быть JSON-объектом")
        unknown = set(values) - {item.name for item in fields(cls)}
        if unknown:
            raise ValueError(f"Неизвестные параметры: {sorted(unknown)}")
        config = cls(**values)
        for name in ("output", "log_file", "stats_json", "report_html"):
            value = Path(getattr(config, name))
            if not value.is_absolute():
                setattr(config, name, str(path.parent / value))
        return config
