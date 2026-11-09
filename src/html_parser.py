import logging
from collections.abc import Callable
from typing import TypedDict
from urllib.parse import urljoin, urlsplit

from bs4 import BeautifulSoup, ParserRejectedMarkup, Tag

logger = logging.getLogger(__name__)


class PageList(TypedDict):
    tag: str
    items: list[str]


class ParsedPage(TypedDict):
    url: str
    title: str
    links: list[str]
    text: str
    metadata: dict[str, str]
    images: list[dict[str, str]]
    headings: list[dict[str, str]]
    tables: list[list[list[str]]]
    lists: list[PageList]


class HTMLParser:
    async def parse_html(self, html: str, url: str) -> ParsedPage:
        page: ParsedPage = {
            "url": url,
            "title": "",
            "links": [],
            "text": "",
            "metadata": {"title": "", "description": "", "keywords": ""},
            "images": [],
            "headings": [],
            "tables": [],
            "lists": [],
        }

        for parser in ("lxml", "html.parser"):
            try:
                soup = BeautifulSoup(html, parser)
                break
            except ParserRejectedMarkup:
                logger.warning("Парсер %s отклонил HTML страницы %s", parser, url)
        else:
            return page

        def extract_or_default[T](
            field: str, extract: Callable[[], T], default: T
        ) -> T:
            try:
                return extract()
            except Exception:
                logger.warning(
                    "Ошибка извлечения поля %s на странице %s",
                    field,
                    url,
                    exc_info=True,
                )
                return default

        page["metadata"] = extract_or_default(
            "metadata", lambda: self.extract_metadata(soup), page["metadata"]
        )
        page["title"] = page["metadata"]["title"]
        page["links"] = extract_or_default(
            "links", lambda: self.extract_links(soup, url), page["links"]
        )
        page["text"] = extract_or_default(
            "text", lambda: self.extract_text(soup), page["text"]
        )
        page["images"] = extract_or_default(
            "images", lambda: self.extract_images(soup, url), page["images"]
        )
        page["headings"] = extract_or_default(
            "headings", lambda: self.extract_headings(soup), page["headings"]
        )
        page["tables"] = extract_or_default(
            "tables", lambda: self.extract_tables(soup), page["tables"]
        )
        page["lists"] = extract_or_default(
            "lists", lambda: self.extract_lists(soup), page["lists"]
        )
        return page

    def extract_links(self, soup: BeautifulSoup, base_url: str) -> list[str]:
        links: list[str] = []
        for anchor in soup.find_all("a", href=True):
            href = anchor.get("href")
            if not isinstance(href, str) or not href.strip():
                continue
            try:
                link = urljoin(base_url, href.strip())
                parsed = urlsplit(link)
            except ValueError as error:
                logger.warning("Некорректный href на странице %s: %s", base_url, error)
                continue
            if parsed.scheme in ("http", "https") and parsed.hostname:
                links.append(link)
        return links

    def extract_text(self, soup: BeautifulSoup, selector: str | None = None) -> str:
        if selector:
            element = soup.select_one(selector)
            return element.get_text(" ", strip=True) if element is not None else ""
        return soup.get_text(" ", strip=True)

    def extract_metadata(self, soup: BeautifulSoup) -> dict[str, str]:
        title = soup.find("title")
        metadata = {
            "title": title.get_text(" ", strip=True) if title is not None else "",
        }
        for name in ("description", "keywords"):
            tag = soup.find("meta", attrs={"name": name})
            content = tag.get("content") if isinstance(tag, Tag) else None
            metadata[name] = content.strip() if isinstance(content, str) else ""
        return metadata

    def extract_images(
        self, soup: BeautifulSoup, base_url: str
    ) -> list[dict[str, str]]:
        images: list[dict[str, str]] = []
        for img in soup.find_all("img", src=True):
            src = img.get("src")
            if not isinstance(src, str) or not src.strip():
                continue
            try:
                image_url = urljoin(base_url, src.strip())
                parsed = urlsplit(image_url)
            except ValueError as error:
                logger.warning("Некорректный src на странице %s: %s", base_url, error)
                continue
            if parsed.scheme in ("http", "https") and parsed.hostname:
                alt = img.get("alt")
                images.append(
                    {"src": image_url, "alt": alt if isinstance(alt, str) else ""}
                )
        return images

    def extract_headings(self, soup: BeautifulSoup) -> list[dict[str, str]]:
        return [
            {"tag": heading.name, "text": heading.get_text(" ", strip=True)}
            for heading in soup.select("h1, h2, h3")
        ]

    def extract_tables(self, soup: BeautifulSoup) -> list[list[list[str]]]:
        tables: list[list[list[str]]] = []
        for table in soup.find_all("table"):
            rows: list[list[str]] = []
            for row in table.find_all("tr"):
                if row.find_parent("table") is table:
                    cells = row.find_all(["th", "td"], recursive=False)
                    rows.append([cell.get_text(" ", strip=True) for cell in cells])
            tables.append(rows)
        return tables

    def extract_lists(self, soup: BeautifulSoup) -> list[PageList]:
        lists: list[PageList] = []
        for page_list in soup.find_all(["ul", "ol"]):
            items: list[str] = []
            for item in page_list.find_all("li", recursive=False):
                text = " ".join(
                    part.strip()
                    for part in item.strings
                    if part.strip() and part.find_parent(["ul", "ol"]) is page_list
                )
                items.append(text)
            lists.append({"tag": page_list.name, "items": items})
        return lists


if __name__ == "__main__":
    html = '<a href="/books">Books</a>'
    soup = BeautifulSoup(html, "lxml")
    link = soup.find("a")

    if link is not None:
        print(link.get_text())
        print(link.get("href"))
