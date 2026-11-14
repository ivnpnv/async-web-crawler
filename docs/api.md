# API

## AdvancedCrawler

`AdvancedCrawler(config: CrawlerConfig | None = None)` создаёт компоненты и
запускает поток записи логов. Закрывайте объект через `await close()` в `finally`.

`AdvancedCrawler.from_config(filename)` читает JSON и создаёт экземпляр.

`await crawl(start_urls=None)` обходит URL из конфигурации и явно заданных sitemap.
Если передать start_urls, они заменяют стартовый список конфигурации. Возвращается
словарь URL → CrawledPage. Страницы sitemap получают глубину 0.

`get_stats()` возвращает снимок. total_pages = successful + failed. failed включает
запрещённые robots.txt URL. Ошибка сохранения отражается отдельно в storage_stats.
Страница с частично извлечёнными полями остаётся успешной и содержит parse_errors.

`export_to_json(filename)` и `export_to_html_report(filename)` синхронно экспортируют
показатели. В асинхронном коде вызывайте их через `asyncio.to_thread`.

`await close()` освобождает сессию, хранилище и поток логов. Повторное закрытие допустимо.

## Показатели

| Поле | Значение |
| --- | --- |
| total_pages | Завершённые обработки URL, включая неудачные |
| successful / failed | Успешные и неуспешные страницы последнего запуска |
| pages_per_second | Завершённые страницы / время последнего запуска |
| elapsed_seconds | Время с начала запуска, включая sitemap |
| status_codes | Последний известный HTTP-статус каждой страницы |
| http_started | Отправки HTTP-заголовков, включая внутренние повторы клиента |
| http_successful / http_failed | Наблюдаемые успешные и неуспешные HTTP-попытки |
| http_status_codes | Все полученные статусы, включая промежуточные |
| http_transport_errors | Ошибки передачи, замеченные кодом краулера |
| top_domains | Топ-10 хостов успешных страниц |
| active_tasks | Занятые места менеджера запросов, включая ожидание rate limiter |
| progress_percent_of_limit | Завершённые URL / max_pages |
| eta_known_seconds | Оценка времени для известных оставшихся URL |
| sitemap_errors | Неудачные sitemap и сообщения об ограничениях |

Оценка ETA не учитывает ещё не найденные ссылки. Если сайт закончился раньше
max_pages, запуск имеет state=finished при проценте меньше 100.

Скрытые внутренние повторы aiohttp видны по http_started, но библиотека может
не передать наружу промежуточный сбой соединения. Поэтому счётчики отправок
и наблюдаемых результатов не обязаны совпадать. request_stats и error_stats
сохраняют накопленную статистику за время жизни объекта, как в предыдущих днях.

## AsyncCrawler

`create_session()` вызывается внутри работающего event loop. `fetch_url()`
возвращает HTML. `fetch_urls()` возвращает успешные загрузки, исключая ошибки.
`fetch_and_parse()` возвращает CrawledPage и сохраняет его при подключённом storage.

`await crawl(start_urls, max_pages=100, same_domain_only=False,
include_patterns=None, exclude_patterns=None)` обходит ссылки по уровням.
max_pages ограничивает число попыток обработки URL. Редиректы и повторы не
расходуют отдельное место в этом лимите. Исключающие регулярные выражения
имеют приоритет над включающими.

## Хранилища

`JSONStorage(path, indent=2, json_lines=False)` и `CSVStorage(path, encoding='utf-8')`
добавляют записи. `SQLiteStorage(path)` выполняет UPSERT по URL.

Все хранилища поддерживают `await save(data)`, `await save_many(records)`,
`await close()` и `get_stats()`. SQLite дополнительно предоставляет init_db().

При сериализации crawled_at приводится к UTC ISO 8601. Дополнительные поля
парсера сохраняются. В CSV списки и словари находятся в JSON-ячейках.

## SitemapParser

Конструктор получает асинхронную функцию URL → bytes.
`await fetch_sitemap(url)` возвращает уникальные URL, а errors — ошибки обхода.
Поддерживаются namespaces, вложенные индексы, gzip и циклы.
Лимиты по умолчанию — 100 sitemap, глубина 10, 100000 результирующих URL,
50 МиБ распакованного XML одного документа.
