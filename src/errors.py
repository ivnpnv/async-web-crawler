import aiohttp


class CrawlerError(aiohttp.ClientError):
    def __init__(
        self,
        message: str,
        *,
        url: str = "",
        status: int | None = None,
        retry_after: float = 0.0,
    ) -> None:
        super().__init__(message)
        self.url = url
        self.status = status
        self.retry_after = retry_after


class TransientError(CrawlerError):
    """Временный сбой сервера или таймаут."""


class PermanentError(CrawlerError):
    """Запрос требует изменения адреса, прав или конфигурации."""


class NetworkError(CrawlerError):
    """Не удалось установить соединение или получить полный ответ."""


class ParseError(CrawlerError):
    """Не удалось извлечь данные из полученного документа."""


class RobotsBlockedError(PermanentError):
    pass


class CircuitOpenError(CrawlerError):
    """Домен временно отключён после серии сбоев."""
