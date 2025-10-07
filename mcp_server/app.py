"""FastAPI application exposing the scrape crawler via MCP."""
from __future__ import annotations

import json
from functools import lru_cache
from typing import Any, AsyncIterator, Dict, Iterable, List, Optional
from urllib.parse import urlparse

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import BaseModel, Field, ValidationError

from scrape.cache import ResponseCache
from scrape.crawler import PlaywrightCrawler

STREAM_PROTOCOL = "streamable-http"
STREAM_VERSION = "0.1"


class ToolArgumentError(HTTPException):
    """Raised when a request references an unknown tool or bad arguments."""

    def __init__(self, detail: str, status_code: int = 400) -> None:
        super().__init__(status_code=status_code, detail=detail)


class ToolRequest(BaseModel):
    """Schema for POST /call_tool requests."""

    name: str = Field(..., description="Name of the tool to execute")
    arguments: Dict[str, Any] = Field(default_factory=dict, description="Tool arguments")


class ToolDescriptor(BaseModel):
    """Description of a tool surfaced via /list_tools."""

    name: str
    description: str
    input_schema: Dict[str, Any]


class MCPListResponse(BaseModel):
    """Payload returned from /list_tools."""

    tools: List[ToolDescriptor]
    stream_protocols: List[Dict[str, str]]


def create_app() -> FastAPI:
    """Return an application instance suitable for Uvicorn."""

    app = FastAPI(
        title="Andronoma MCP Server",
        version="0.1.0",
        default_response_class=JSONResponse,
        docs_url=None,
        redoc_url=None,
        openapi_url=None,
    )

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_methods=["*"],
        allow_headers=["*"],
        allow_credentials=True,
    )

    cache = ResponseCache()

    @app.get("/healthz")
    async def healthcheck() -> Dict[str, Any]:
        return {"status": "ok", "cache_available": cache.available}

    @app.get("/mcp/list_tools")
    async def list_tools() -> MCPListResponse:
        tools = [
            ToolDescriptor(
                name="fetch_html",
                description="Fetch a single URL and return response metadata and HTML using the Playwright crawler.",
                input_schema={
                    "type": "object",
                    "properties": {
                        "url": {
                            "type": "string",
                            "format": "uri",
                            "description": "Fully-qualified URL to fetch",
                        }
                    },
                    "required": ["url"],
                },
            ),
            ToolDescriptor(
                name="crawl_once",
                description="Fetch a URL and derive structured metadata (title, headings, meta tags, price heuristics).",
                input_schema={
                    "type": "object",
                    "properties": {
                        "url": {
                            "type": "string",
                            "format": "uri",
                            "description": "Fully-qualified URL to crawl",
                        }
                    },
                    "required": ["url"],
                },
            ),
        ]
        return MCPListResponse(
            tools=tools,
            stream_protocols=[{"type": STREAM_PROTOCOL, "version": STREAM_VERSION}],
        )

    @app.post("/mcp/call_tool")
    async def call_tool(request: Request) -> StreamingResponse:
        try:
            payload = await request.json()
            parsed = ToolRequest.model_validate(payload)
        except (ValueError, ValidationError) as exc:  # ValueError for JSON decode errors
            raise ToolArgumentError("Invalid request payload") from exc

        if parsed.name == "fetch_html":
            url = parsed.arguments.get("url")
            if not isinstance(url, str):
                raise ToolArgumentError("fetch_html requires a 'url' string argument")
            result = await _run_fetch_html(url, cache)
        elif parsed.name == "crawl_once":
            url = parsed.arguments.get("url")
            if not isinstance(url, str):
                raise ToolArgumentError("crawl_once requires a 'url' string argument")
            result = await _run_crawl_once(url, cache)
        else:
            raise ToolArgumentError(f"Unknown tool '{parsed.name}'", status_code=404)

        async def iterator() -> AsyncIterator[bytes]:
            envelope = {
                "type": "result",
                "stream": {
                    "protocol": STREAM_PROTOCOL,
                    "version": STREAM_VERSION,
                    "messages": [
                        {
                            "type": "json",
                            "content": result,
                        }
                    ],
                    "final": True,
                },
            }
            yield json.dumps(envelope, ensure_ascii=False).encode("utf-8")

        headers = {
            "X-Accel-Buffering": "no",
            "Cache-Control": "no-store",
        }
        return StreamingResponse(iterator(), media_type="application/json", headers=headers)

    return app


def _domain_allowlist(url: str) -> Iterable[str]:
    parsed = urlparse(url)
    if not parsed.scheme or not parsed.netloc:
        raise ToolArgumentError("URL must include scheme and hostname")

    hostname = parsed.hostname
    if hostname is None:
        raise ToolArgumentError("Unable to resolve hostname from URL")

    parts = hostname.split(".")
    allowlist = {hostname}
    if len(parts) >= 2:
        allowlist.add(".".join(parts[-2:]))
    return allowlist


async def _run_fetch_html(url: str, cache: ResponseCache) -> Dict[str, Any]:
    allowed = _domain_allowlist(url)
    try:
        async with _crawler(allowed, cache) as crawler:
            responses = await crawler.crawl([url])
    except PermissionError as exc:
        raise ToolArgumentError(str(exc), status_code=403) from exc
    except ValueError as exc:
        raise ToolArgumentError(str(exc)) from exc
    if not responses:
        raise ToolArgumentError("No response captured from crawler", status_code=502)
    response = next(iter(responses.values()))
    return {
        "url": response.url,
        "status": response.status,
        "headers": response.headers,
        "html": response.body,
        "fetched_at": response.fetched_at.isoformat(),
        "latency": response.latency,
        "from_cache": response.from_cache,
    }


async def _run_crawl_once(url: str, cache: ResponseCache) -> Dict[str, Any]:
    allowed = _domain_allowlist(url)
    try:
        async with _crawler(allowed, cache) as crawler:
            responses = await crawler.crawl([url])
    except PermissionError as exc:
        raise ToolArgumentError(str(exc), status_code=403) from exc
    except ValueError as exc:
        raise ToolArgumentError(str(exc)) from exc
    if not responses:
        raise ToolArgumentError("No response captured from crawler", status_code=502)
    response = next(iter(responses.values()))

    metadata = _extract_metadata(response.body)
    metadata.update(
        {
            "url": response.url,
            "status": response.status,
            "headers": response.headers,
            "fetched_at": response.fetched_at.isoformat(),
            "latency": response.latency,
            "from_cache": response.from_cache,
        }
    )
    return metadata


@lru_cache(maxsize=1)
def _crawler_factory() -> Dict[str, Any]:
    return {
        "max_concurrent_per_domain": 2,
        "max_requests_per_second": 5,
        "crawl_delay": 0.5,
        "request_timeout": 30.0,
        "user_agent": "andronoma-mcp/0.1",
    }


class _CrawlerContext:
    """Async context manager to reuse the Playwright crawler configuration."""

    def __init__(self, allowed_domains: Iterable[str], cache: ResponseCache):
        self._allowed_domains = allowed_domains
        self._cache = cache
        self._manager: Optional[PlaywrightCrawler] = None

    async def __aenter__(self) -> PlaywrightCrawler:
        kwargs = dict(_crawler_factory())
        self._manager = PlaywrightCrawler(allowed_domains=self._allowed_domains, cache=self._cache, **kwargs)
        return await self._manager.__aenter__()

    async def __aexit__(self, exc_type, exc, tb) -> None:  # type: ignore[override]
        assert self._manager is not None
        await self._manager.__aexit__(exc_type, exc, tb)


def _crawler(allowed_domains: Iterable[str], cache: ResponseCache) -> _CrawlerContext:
    return _CrawlerContext(allowed_domains, cache)


def _extract_metadata(html: str) -> Dict[str, Any]:
    from bs4 import BeautifulSoup

    soup = BeautifulSoup(html, "html.parser")
    title = soup.title.string.strip() if soup.title and soup.title.string else None
    h1 = [element.get_text(strip=True) for element in soup.find_all("h1") if element.get_text(strip=True)]
    description = None
    meta_tags: Dict[str, Any] = {}
    for meta in soup.find_all("meta"):
        name = meta.get("name") or meta.get("property")
        content = meta.get("content")
        if not name or content is None:
            continue
        meta_tags[name.lower()] = content.strip()
        if name.lower() == "description":
            description = content.strip()

    price = _detect_price(soup)
    link_candidates = [a.get("href") for a in soup.find_all("a") if a.get("href")]
    image_candidates = [img.get("src") for img in soup.find_all("img") if img.get("src")]
    links = list(dict.fromkeys(link_candidates))
    images = list(dict.fromkeys(image_candidates))

    return {
        "title": title,
        "h1": h1,
        "description": description,
        "meta": meta_tags,
        "price": price,
        "links": links,
        "images": images,
    }


def _detect_price(soup: Any) -> Optional[Dict[str, Any]]:
    price_candidates: List[str] = []
    for selector in [
        {"attrs": {"itemprop": "price"}},
        {"attrs": {"data-price": True}},
        {"attrs": {"data-price-amount": True}},
    ]:
        for element in soup.find_all(True, **selector):
            candidate = element.get("content") or element.get_text(strip=True)
            if candidate:
                price_candidates.append(candidate)
    for element in soup.select(".price, .Price, [class*='price']"):
        candidate = element.get_text(strip=True)
        if candidate:
            price_candidates.append(candidate)

    for raw in price_candidates:
        parsed = _parse_price(raw)
        if parsed:
            return parsed
    return None


def _parse_price(raw: str) -> Optional[Dict[str, Any]]:
    import re

    pattern = re.compile(r"(?P<currency>[\$£€¥])?\s*(?P<amount>\d+[\d,]*(?:\.\d{2})?)")
    match = pattern.search(raw)
    if not match:
        return None
    amount = match.group("amount").replace(",", "")
    try:
        value = float(amount)
    except ValueError:
        return None
    currency = match.group("currency")
    return {"raw": raw, "value": value, "currency": currency}


app = create_app()
