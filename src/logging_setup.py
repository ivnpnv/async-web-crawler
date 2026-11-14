import json
import logging
from datetime import UTC, datetime
from logging.handlers import QueueHandler, QueueListener, RotatingFileHandler
from pathlib import Path
from queue import SimpleQueue


class JSONFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        result = {
            "timestamp": datetime.fromtimestamp(record.created, UTC).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        if hasattr(record, "crawler_error"):
            result["error"] = record.crawler_error
        if record.exc_info:
            result["exception"] = self.formatException(record.exc_info)
        return json.dumps(result, ensure_ascii=False, default=str)


class LoggingSession:
    def __init__(self, filename: str, level: str, max_bytes: int, backups: int) -> None:
        path = Path(filename)
        path.parent.mkdir(parents=True, exist_ok=True)
        self._file = RotatingFileHandler(
            path, maxBytes=max_bytes, backupCount=backups, encoding="utf-8"
        )
        self._file.setFormatter(JSONFormatter())
        console = logging.StreamHandler()
        console.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
        queue: SimpleQueue[logging.LogRecord] = SimpleQueue()
        self._handler = QueueHandler(queue)
        self._logger = logging.getLogger("src")
        self._previous = (
            self._logger.handlers[:],
            self._logger.level,
            self._logger.propagate,
        )
        self._logger.handlers = [self._handler]
        self._logger.setLevel(level)
        self._logger.propagate = False
        self._listener = QueueListener(
            queue, self._file, console, respect_handler_level=True
        )
        self._listener.start()

    def close(self) -> None:
        self._listener.stop()
        self._logger.handlers, level, self._logger.propagate = self._previous
        self._logger.setLevel(level)
        self._file.close()
        self._handler.close()
