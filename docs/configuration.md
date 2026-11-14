# Конфигурация

Используется JSON. Пример находится в config.example.json. Неизвестные поля и
неверные типы вызывают ошибку до создания выходных файлов. Значения CLI имеют
приоритет над JSON. Пути JSON разрешаются относительно файла конфигурации.

| Параметр | По умолчанию | Назначение |
| --- | --- | --- |
| urls | [] | Стартовые URL |
| sitemap_urls | [] | Адреса XML sitemap или индексов |
| max_pages | 100 | Предел числа обрабатываемых URL |
| max_depth | 2 | Максимальная глубина, начальный уровень 0 |
| max_concurrent | 10 | Число работников и общий лимит запросов |
| max_per_domain | 2 | Конкурентность на один хост |
| rate_limit | 1.0 | Частота стартов на хост в секунду |
| min_delay / jitter | 0 / 0 | Минимальный интервал и дополнительная случайная пауза |
| respect_robots | true | Применять robots.txt и Crawl-delay |
| same_domain_only | true | Ограничить хостами стартовых URL |
| include_patterns / exclude_patterns | [] / [] | Регулярные выражения для URL |
| user_agent | ItGrindCrawler/1.0 | Имя клиента и выбор группы robots |
| connect_timeout / read_timeout / total_timeout | 5 / 10 / 30 | Таймауты одной попытки в секундах |
| max_retries / backoff_factor | 3 / 2.0 | Бюджет повторов и рост паузы |
| output | results/pages.json | Файл результатов |
| encoding | utf-8 | Кодировка CSV |
| stats_json | results/stats.json | JSON статистики |
| report_html | results/report.html | HTML с таблицами и диаграммами |
| log_file / log_level | results/crawler.log / INFO | Файл и уровень логов |
| log_max_bytes / log_backup_count | 1000000 / 3 | Размер ротации и число архивов |
| progress_interval | 1.0 | Период вывода прогресса в секундах |

Расширение output выбирает JSON, JSONL, CSV или SQLite (.db/.sqlite/.sqlite3).
Файлы результатов, статистики, HTML и логов должны иметь разные пути.

При same_domain_only=true URL из sitemap фильтруются по хостам стартовых URL.
Если стартовый список пуст, используются хосты явно заданных sitemap.
Поддомены рассматриваются как отдельные хосты. Фильтры применяются и при редиректах.

CLI поддерживает --urls, --sitemaps, --max-pages, --max-depth, --max-concurrent,
--output, --config, --rate-limit, --include-patterns, --exclude-patterns,
--log-level, --respect-robots/--no-respect-robots и
--same-domain-only/--no-same-domain-only.

Для тонкой настройки отдельных retry-политик, глобального rate limiter,
таймаутов роста и circuit breaker используйте Python API AsyncCrawler.
