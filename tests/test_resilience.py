import asyncio
import unittest
from collections import Counter
from unittest.mock import AsyncMock, patch

import aiohttp
from aiohttp import web
from aiohttp.test_utils import TestServer

from src.circuit_breaker import CircuitBreaker
from src.crawler import AsyncCrawler
from src.errors import (
    CircuitOpenError,
    NetworkError,
    ParseError,
    PermanentError,
    TransientError,
)
from src.html_parser import HTMLParser
from src.retry_strategy import RetryPolicy, RetryStrategy


class RetryTests(unittest.IsolatedAsyncioTestCase):
    async def test_backoff_retry_after_and_success(self) -> None:
        strategy = RetryStrategy()
        operation = AsyncMock(
            side_effect=[
                TransientError("busy", url="https://a.test", status=429, retry_after=3),
                TransientError("busy", url="https://a.test", status=503),
                "OK",
            ]
        )
        with patch("src.retry_strategy.asyncio.sleep", new_callable=AsyncMock) as sleep:
            self.assertEqual(await strategy.execute_with_retry(operation), "OK")
        self.assertEqual([call.args[0] for call in sleep.call_args_list], [3, 2])
        self.assertEqual(strategy.get_stats()["successful_retries"], 1)
        self.assertEqual(strategy.get_stats()["errors_by_type"], {"TransientError": 2})

    async def test_per_type_budget_and_permanent_override(self) -> None:
        strategy = RetryStrategy(max_retries=5)
        operation = AsyncMock(
            side_effect=NetworkError("DNS failed", url="https://a.test")
        )
        with (
            patch("src.retry_strategy.asyncio.sleep", new_callable=AsyncMock),
            self.assertRaises(NetworkError),
        ):
            await strategy.execute_with_retry(operation)
        self.assertEqual(operation.await_count, 3)

        strategy = RetryStrategy(retry_on=[Exception])
        operation = AsyncMock(
            side_effect=PermanentError("HTTP 404", url="https://a.test", status=404)
        )
        with self.assertRaises(PermanentError):
            await strategy.execute_with_retry(operation)
        self.assertEqual(operation.await_count, 1)
        self.assertEqual(
            strategy.get_stats()["permanent_error_urls"], ["https://a.test"]
        )

    async def test_task_local_attempts_and_cancellation(self) -> None:
        strategy = RetryStrategy(policies={TransientError: RetryPolicy(3, 0)})
        attempts: dict[str, list[int]] = {"first": [], "second": []}

        async def operation(name: str) -> str:
            attempts[name].append(strategy.current_attempt)
            await asyncio.sleep(0)
            if name == "first" and strategy.current_attempt == 1:
                raise TransientError("retry")
            return name

        await asyncio.gather(
            *(strategy.execute_with_retry(operation, name) for name in attempts)
        )
        self.assertEqual(attempts, {"first": [1, 2], "second": [1]})
        self.assertEqual(strategy.current_attempt, 1)

        operation_mock = AsyncMock(side_effect=asyncio.CancelledError)
        before = len(strategy.errors)
        with self.assertRaises(asyncio.CancelledError):
            await strategy.execute_with_retry(operation_mock)
        self.assertEqual(operation_mock.await_count, 1)
        self.assertEqual(len(strategy.errors), before)


class CircuitTests(unittest.IsolatedAsyncioTestCase):
    async def test_open_half_open_and_recovery(self) -> None:
        breaker = CircuitBreaker(min_requests=2, recovery_timeout=5)
        url = "https://a.test/page"
        with patch("src.circuit_breaker.monotonic", return_value=100):
            for _ in range(2):
                with self.assertRaises(TransientError):
                    async with breaker.request(url):
                        raise TransientError("HTTP 503")
            with self.assertRaises(CircuitOpenError):
                async with breaker.request(url):
                    self.fail("Open circuit must reject the request")
            async with breaker.request("https://other.test"):
                pass

        with patch("src.circuit_breaker.monotonic", return_value=106):
            async with breaker.request(url):
                with self.assertRaises(CircuitOpenError):
                    async with breaker.request(url):
                        self.fail("Only one probe is allowed")
            async with breaker.request(url):
                pass

    async def test_cancelled_probe_can_be_retried(self) -> None:
        breaker = CircuitBreaker(min_requests=1, recovery_timeout=5)
        url = "https://a.test"
        with (
            patch("src.circuit_breaker.monotonic", return_value=100),
            self.assertRaises(NetworkError),
        ):
            async with breaker.request(url):
                raise NetworkError("offline")
        with patch("src.circuit_breaker.monotonic", return_value=106):
            with self.assertRaises(asyncio.CancelledError):
                async with breaker.request(url):
                    raise asyncio.CancelledError
            async with breaker.request(url):
                pass


class CrawlerTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.hits: Counter[str] = Counter()

        async def handler(request: web.Request) -> web.Response:
            path = request.path
            self.hits[path] += 1
            if path == "/robots.txt":
                return web.Response(status=404)
            if path == "/slow":
                await asyncio.sleep(0.3)
            if path == "/redirect":
                raise web.HTTPFound("/403")
            if path[1:].isdigit():
                status = int(path[1:])
                if status in (429, 503) and self.hits[path] >= 2:
                    status = 200
                return web.Response(status=status, text="OK")
            return web.Response(
                text="<title>OK</title><table><tr><td>1</td></tr></table>"
            )

        app = web.Application()
        app.router.add_get("/{path:.*}", handler)
        self.server = TestServer(app)
        await self.server.start_server()

    async def asyncTearDown(self) -> None:
        await self.server.close()

    def url(self, path: str) -> str:
        return str(self.server.make_url(path))

    def crawler(self, **kwargs: object) -> AsyncCrawler:
        return AsyncCrawler(
            requests_per_second=10_000,
            retry_strategy=RetryStrategy(
                policies={
                    TransientError: RetryPolicy(3, 0.001),
                    NetworkError: RetryPolicy(2, 0.001),
                }
            ),
            circuit_breaker=CircuitBreaker(min_requests=100),
            **kwargs,
        )

    async def test_http_classification_limits_and_crawl_state(self) -> None:
        crawler = self.crawler(max_depth=0)
        paths = ["/429", "/503", "/500", "/401", "/403", "/404", "/redirect"]
        result = await crawler.crawl([self.url(path) for path in paths])
        self.assertEqual(set(result), {self.url("/429"), self.url("/503")})
        self.assertEqual(self.hits["/500"], 4)
        self.assertEqual(self.hits["/429"], 2)
        self.assertEqual(self.hits["/503"], 2)
        self.assertEqual(self.hits["/401"], 1)
        self.assertEqual(self.hits["/403"], 2)
        self.assertEqual(self.hits["/404"], 1)
        self.assertEqual(crawler.get_error_stats()["successful_retries"], 2)
        self.assertEqual(len(crawler.failed_urls), 5)
        self.assertIsNone(crawler.session)

    async def test_read_timeout_grows_for_next_attempt(self) -> None:
        crawler = self.crawler(read_timeout=0.1, timeout_multiplier=10, total_timeout=2)
        crawler.create_session()
        try:
            self.assertIn("OK", await crawler.fetch_url(self.url("/slow")))
        finally:
            await crawler.close()
        self.assertEqual(self.hits["/slow"], 2)
        self.assertEqual(
            crawler.get_error_stats()["errors_by_type"]["TransientError"], 1
        )

    async def test_total_timeout_and_partial_parse_error(self) -> None:
        crawler = self.crawler(total_timeout=0.1, read_timeout=2, timeout_multiplier=10)
        crawler.create_session()
        try:
            self.assertIn("OK", await crawler.fetch_url(self.url("/slow")))
            crawler.total_timeout = 2
            with patch.object(
                HTMLParser, "extract_tables", side_effect=ValueError("bad table")
            ):
                page = await crawler.fetch_and_parse(self.url("/page"))
        finally:
            await crawler.close()
        self.assertEqual(self.hits["/slow"], 2)
        self.assertEqual(self.hits["/page"], 1)
        self.assertEqual(page["title"], "OK")
        self.assertEqual(page["tables"], [])
        self.assertTrue(page["parse_errors"])
        self.assertIsInstance(crawler.retry_strategy.errors[-1].message, str)
        self.assertEqual(crawler.get_error_stats()["errors_by_type"]["ParseError"], 1)

    async def test_parse_errors_are_not_retried_by_default(self) -> None:
        strategy = RetryStrategy()
        operation = AsyncMock(side_effect=ParseError("invalid HTML"))
        with self.assertRaises(ParseError):
            await strategy.execute_with_retry(operation)
        self.assertEqual(operation.await_count, 1)

    async def test_network_error_is_classified_and_retry_is_bounded(self) -> None:
        crawler = self.crawler()
        crawler.create_session()
        try:
            await crawler._robots.fetch_robots(self.url("/page"))
            with patch.object(
                crawler.session,
                "get",
                side_effect=aiohttp.ServerDisconnectedError("lost connection"),
            ) as get:
                with self.assertRaises(NetworkError):
                    await crawler.fetch_url(self.url("/page"))
                self.assertEqual(get.call_count, 3)
        finally:
            await crawler.close()
        self.assertEqual(
            crawler.get_error_stats()["errors_by_type"], {"NetworkError": 3}
        )

    async def test_circuit_breaker_rejects_requests_without_http(self) -> None:
        crawler = self.crawler(max_depth=0)
        crawler.circuit_breaker = CircuitBreaker(min_requests=1)
        result = await crawler.crawl([self.url("/500")])
        self.assertEqual(result, {})
        self.assertEqual(self.hits["/500"], 1)
        self.assertEqual(
            crawler.get_error_stats()["errors_by_type"]["CircuitOpenError"], 1
        )


if __name__ == "__main__":
    unittest.main()
