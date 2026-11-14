# Асинхронный веб-краулер

Краулер загружает и разбирает HTML,
обходит ссылки и sitemap, соблюдает robots.txt, ограничивает скорость,
повторяет временные ошибки и сохраняет результаты в файлы или SQLite.

## Установка зависимостей

Выполни команды в PowerShell из папки проекта.

```powershell
py -3.12 -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
```

## Запуск

```powershell
.\.venv\Scripts\python.exe crawler.py --urls https://example.com --max-pages 100 --max-depth 2 --rate-limit 1 --output results/pages.sqlite
.\.venv\Scripts\python.exe crawler.py --config config.example.json
.\.venv\Scripts\python.exe crawler.py --help
```

Результаты записываются после обработки каждой страницы. Статистика, HTML-отчёт
и JSON-логи по умолчанию находятся в `results/`. Расширение `--output` определяет
хранилище. robots.txt учитывается по умолчанию. CLI-параметры переопределяют
конфигурацию. Пути в конфигурации отсчитываются от её каталога, а пути CLI —
от текущего каталога.

Код завершения `0` означает успешный обход, `1` — ошибки загрузки, sitemap или
сохранения, `2` — неверные параметры CLI, `130` — остановку пользователем.

## Полная локальная демонстрация

```powershell
.\.venv\Scripts\python.exe -m src.demo_advanced
```

Создаётся учебный сайт с индексом sitemap, временной ошибкой 503, отсутствующей
страницей 404 и запрещённым разделом. Результаты сохраняются в SQLite,
статистика — в JSON, отчёт с таблицами и диаграммами — в HTML. Демонстрация
намеренно содержит неуспешные страницы.

## AdvancedCrawler

```python
import asyncio
from crawler import AdvancedCrawler

async def main() -> None:
    crawler = AdvancedCrawler.from_config("config.example.json")
    try:
        await crawler.crawl()
        print(crawler.get_stats())
        await asyncio.to_thread(crawler.export_to_json, "results/stats.json")
        await asyncio.to_thread(crawler.export_to_html_report, "results/report.html")
    finally:
        await crawler.close()

asyncio.run(main())
```

## Компоненты

- `AsyncCrawler` управляет HTTP-запросами и обходом по уровням глубины.
- `CrawlerQueue` хранит приоритеты URL и защищает от повторной постановки.
- `SemaphoreManager` ограничивает конкурентность глобально и по хостам.
- `RateLimiter` регулирует интервалы между отправками запросов.
- `RobotsParser` кэширует разрешения и Crawl-delay по origin.
- `RetryStrategy` классифицирует повторы, хранит историю ошибок и backoff.
- `CircuitBreaker` временно отключает хост после высокой доли сбоев.
- `SitemapParser` обходит XML-индексы с защитой от циклов и ограничениями размера.
- `DataStorage` объединяет JSON, JSONL, CSV и SQLite.
- `CrawlerStats` формирует показатели и отчёты.
- `AdvancedCrawler` объединяет конфигурацию, мониторинг и жизненный цикл.

Подробности находятся в [описании API](docs/api.md),
[руководстве конфигурации](docs/configuration.md) и
[описании измерений](docs/performance.md).

## Демонстрация сохранения данных

```powershell
.\.venv\Scripts\python.exe -m src.demo_storage
```

Скрипт запускает локальный учебный сайт, обходит его и сохраняет результаты
в JSON, JSONL, CSV и SQLite. Затем читает записи и сравнивает наборы URL.
Каждый запуск создаёт отдельную папку внутри `results/`.

## Использование хранилища

```python
from src.crawler import AsyncCrawler
from src.file_storage import JSONStorage

async def main() -> None:
    crawler = AsyncCrawler(storage=JSONStorage("results/pages.json"))
    try:
        await crawler.crawl(["https://example.com"], same_domain_only=True)
        print(crawler.get_storage_stats())
    finally:
        await crawler.close()
```

`JSONStorage(..., json_lines=True)` записывает по одному объекту на строку.
`CSVStorage` принимает кодировку через `encoding`.
`SQLiteStorage` сохраняет последнюю версию страницы по URL.
Все хранилища поддерживают `save_many(records)` для пакетной записи.

Файловые хранилища добавляют записи, копируя прежний файл небольшими блоками
во временный файл и заменяя его после успешной записи. Они рассчитаны на
один экземпляр хранилища на файл. Пакетная запись сокращает число копирований.
Для больших постоянно пополняемых наборов предпочтительна SQLite.

## Тесты

```powershell
.\.venv\Scripts\python.exe -m unittest discover -s tests -q
```

## Производительность

```powershell
.\.venv\Scripts\python.exe -m src.benchmark --sizes 100 500 1000 --output results/benchmark.json
```

Измеряется последовательная и конкурентная загрузка с одинаковым HTML-парсером
и повторным использованием соединений. Сеть моделируется локальным сервером.
`tracemalloc` измеряет выделения Python, включая локальный сервер, и не отражает
полный RSS процесса или всю память нативных библиотек.

## Ограничения

Очередь, результаты, кэш и лимиты находятся в памяти одного процесса. Работников
не больше `max_concurrent`, но множество найденных URL и результаты продолжают
расти с обходом. Уровни глубины выполняются последовательно.

Sitemap задаются явно. Поддерживаются XML urlset, sitemapindex и gzip.
XML-парсер отключает внешние сущности и DTD. Размер проверяется после загрузки,
поэтому это ограничение разбора, а не потоковый предел HTTP-ответа.

Одновременно следует использовать один AdvancedCrawler с настройкой логирования
на процесс. Очередь логирования переносит дисковую запись в отдельный поток.
Экспорт отчётов имеет синхронный интерфейс, CLI вызывает его через `to_thread`.

Логи могут содержать URL и сообщения сервера. Статистика HTTP учитывает служебные
запросы и повторы, а показатели страниц — конечные результаты обхода.
