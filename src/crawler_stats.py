import json
import os
from collections import Counter
from datetime import UTC, datetime
from html import escape
from pathlib import Path
from time import perf_counter
from urllib.parse import urlsplit
from uuid import uuid4


def atomic_text(filename: str | Path, text: str) -> None:
    path = Path(filename)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid4().hex}.tmp")
    try:
        temporary.write_text(text, encoding="utf-8")
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


class CrawlerStats:
    def __init__(self) -> None:
        self.start()

    def start(self) -> None:
        self.started_at = datetime.now(UTC).isoformat()
        self._started = perf_counter()
        self._finished: float | None = None
        self.successful = 0
        self.failed = 0
        self.http_started = 0
        self.http_successful = 0
        self.http_failed = 0
        self.http_transport_errors = 0
        self.http_statuses: Counter[int] = Counter()
        self.page_statuses: Counter[int] = Counter()
        self.domains: Counter[str] = Counter()
        self.state = "running"

    def finish(self, state: str = "finished") -> None:
        self._finished = perf_counter()
        self.state = state

    def record_page(self, url: str, status: int | None, *, success: bool) -> None:
        if success:
            self.successful += 1
            self.domains[urlsplit(url).hostname or "unknown"] += 1
        else:
            self.failed += 1
        if status is not None:
            self.page_statuses[status] += 1

    def get_stats(self) -> dict[str, object]:
        elapsed = (
            self._finished if self._finished is not None else perf_counter()
        ) - self._started
        total = self.successful + self.failed
        return {
            "state": self.state,
            "started_at": self.started_at,
            "total_pages": total,
            "successful": self.successful,
            "failed": self.failed,
            "elapsed_seconds": elapsed,
            "pages_per_second": total / elapsed if elapsed > 0 else 0.0,
            "http_started": self.http_started,
            "http_successful": self.http_successful,
            "http_failed": self.http_failed,
            "http_transport_errors": self.http_transport_errors,
            "status_codes": dict(self.page_statuses),
            "http_status_codes": dict(self.http_statuses),
            "top_domains": dict(self.domains.most_common(10)),
        }

    def export_to_json(self, filename: str | Path) -> None:
        atomic_text(
            filename, json.dumps(self.get_stats(), ensure_ascii=False, indent=2)
        )

    def export_to_html_report(
        self, filename: str | Path, *, extra: dict[str, object] | None = None
    ) -> None:
        stats = self.get_stats()
        stats.update(extra or {})
        rows = "".join(
            f"<tr><th>{escape(str(key))}</th><td>{escape(str(value))}</td></tr>"
            for key, value in stats.items()
            if not isinstance(value, dict)
        )
        charts = []
        for title, counts in (
            ("HTTP-ответы", self.http_statuses),
            ("Домены", self.domains),
        ):
            maximum = max(counts.values(), default=1)
            bars = "".join(
                f"<tr><th>{escape(str(label))}</th><td>"
                f'<meter min="0" max="{maximum}" value="{count}">{count}</meter>'
                f" {count}</td></tr>"
                for label, count in counts.most_common(10)
            )
            charts.append(f"<h2>{title}</h2><table>{bars}</table>")
        atomic_text(
            filename,
            '<!doctype html><html lang="ru"><meta charset="utf-8">'
            '<meta name="viewport" content="width=device-width,initial-scale=1">'
            "<title>Отчёт краулера</title><style>"
            "body{font:16px system-ui;max-width:960px;margin:2rem auto;padding:1rem}"
            "table{border-collapse:collapse;width:100%}th,td{text-align:left;"
            "padding:.5rem;border-bottom:1px solid #aaa;overflow-wrap:anywhere}"
            "meter{width:60%}</style><h1>Отчёт краулера</h1><table>"
            + rows
            + "</table>"
            + "".join(charts)
            + "<details><summary>Все показатели</summary><pre>"
            + escape(json.dumps(stats, ensure_ascii=False, indent=2))
            + "</pre></details></html>",
        )
