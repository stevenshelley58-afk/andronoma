"""Playwright-powered crawler utilities for the scrape stage."""
from __future__ import annotations

import asyncio
import datetime as dt
import ipaddress
import re
import time
from collections import defaultdict, deque
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional
from urllib.parse import urlparse
from urllib.robotparser import RobotFileParser

import httpx

try:  # pragma: no cover - optional dependency in CI environments
    from playwright.async_api import Browser, Playwright, async_playwright
except ImportError:  # pragma: no cover - documented fallback
    Browser = Playwright = None  # type: ignore[assignment]
    async_playwright = None  # type: ignore[assignment]

from .cache import ResponseCache


@dataclass
class CrawlerResponse:
    """Representation of a crawled document."""

    url: str
    status: int
    body: str
    headers: Dict[str, str]
    fetched_at: dt.datetime
    latency: float
    from_cache: bool

    def to_cache_payload(self) -> Dict[str, object]:
        return {
            "url": self.url,
            "status": self.status,
            "body": self.body,
            "headers": self.headers,
            "fetched_at": self.fetched_at.isoformat(),
            "latency": self.latency,
        }

    @classmethod
    def from_cache(cls, payload: Dict[str, object]) -> "CrawlerResponse":
        fetched_at = payload.get("fetched_at")
        timestamp = (
            dt.datetime.fromisoformat(str(fetched_at))
            if isinstance(fetched_at, str)
            else dt.datetime.now(dt.UTC)
        )
        if timestamp.tzinfo is None:
            timestamp = timestamp.replace(tzinfo=dt.UTC)
        return cls(
            url=str(payload.get("url")),
            status=int(payload.get("status", 0)),
            body=str(payload.get("body", "")),
            headers={k: str(v) for k, v in dict(payload.get("headers", {})).items()},
            fetched_at=timestamp,
            latency=float(payload.get("latency", 0.0)),
            from_cache=True,
        )


class PlaywrightCrawler:
    """Headless crawler that enforces crawl constraints from the spec."""

    def __init__(
        self,
        *,
        allowed_domains: Iterable[str],
        cache: Optional[ResponseCache] = None,
        max_concurrent_per_domain: int = 2,
        max_requests_per_second: int = 10,
        crawl_delay: float = 2.0,
        request_timeout: float = 30.0,
        user_agent: str = "andronoma-crawler/1.0",
    ) -> None:
        self.allowed_domains = {domain.lower(): None for domain in allowed_domains}
        self.cache = cache or ResponseCache()
        self.max_concurrent_per_domain = max_concurrent_per_domain
        self.max_requests_per_second = max_requests_per_second
        self.crawl_delay = crawl_delay
        self.request_timeout = request_timeout
        self.user_agent = user_agent

        self._playwright: Optional[Playwright] = None
        self._browser: Optional[Browser] = None
        self._http_client: Optional[httpx.AsyncClient] = None

        self._domain_semaphores: Dict[str, asyncio.Semaphore] = {}
        self._domain_windows: Dict[str, deque] = defaultdict(deque)
        self._domain_last_fetch: Dict[str, float] = defaultdict(float)
        self._robots: Dict[str, RobotFileParser] = {}

        self._metrics = {
            "request_count": 0,
            "cache_hits": 0,
            "latency_total": 0.0,
            "requests_per_domain": defaultdict(int),
        }

    async def __aenter__(self) -> "PlaywrightCrawler":
        if async_playwright is None:
            # Fallback to HTTPX client so the stage remains testable in CI.
            self._http_client = httpx.AsyncClient(
                headers={"User-Agent": self.user_agent},
                timeout=self.request_timeout,
                follow_redirects=True,
            )
            return self

        self._playwright = await async_playwright().start()
        self._browser = await self._playwright.chromium.launch(
            headless=True,
            args=[
                "--disable-dev-shm-usage",
                "--disable-blink-features=AutomationControlled",
            ],
        )
        self._http_client = httpx.AsyncClient(
            headers={"User-Agent": self.user_agent},
            timeout=self.request_timeout,
            follow_redirects=True,
        )
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:  # type: ignore[override]
        if self._browser is not None:
            await self._browser.close()
        if self._playwright is not None:
            await self._playwright.stop()
        if self._http_client is not None:
            await self._http_client.aclose()

    @property
    def metrics(self) -> Dict[str, object]:
        """Return aggregate crawl metrics for telemetry."""

        base = {k: v for k, v in self._metrics.items() if k != "requests_per_domain"}
        base["requests_per_domain"] = dict(self._metrics["requests_per_domain"])
        base.update(self.cache.stats)
        return base

    async def crawl(self, urls: Iterable[str]) -> Dict[str, CrawlerResponse]:
        tasks = [asyncio.create_task(self._fetch(url)) for url in dict.fromkeys(urls)]
        results = await asyncio.gather(*tasks)
        return {response.url: response for response in results if response is not None}

    async def _fetch(self, url: str) -> Optional[CrawlerResponse]:
        parsed = urlparse(url)
        if not self._is_allowed_host(parsed.hostname):
            raise ValueError(f"Blocked by SSRF guard: {url}")

        cached = self.cache.get(url)
        if cached:
            self._metrics["cache_hits"] += 1
            return CrawlerResponse.from_cache(cached)

        domain = parsed.hostname.lower() if parsed.hostname else ""
        semaphore = self._domain_semaphores.setdefault(
            domain, asyncio.Semaphore(self.max_concurrent_per_domain)
        )

        async with semaphore:
            await self._respect_robots(url)
            await self._enforce_rate_limit(domain)
            await self._enforce_crawl_delay(domain)

            start = time.perf_counter()
            response: Optional[CrawlerResponse]
            try:
                if self._browser is not None:
                    response = await self._fetch_with_playwright(url)
                else:
                    response = await self._fetch_with_httpx(url)
            finally:
                latency = time.perf_counter() - start
                self._metrics["latency_total"] += latency

            if response is None:
                return None

            response.latency = latency
            self._metrics["request_count"] += 1
            self._metrics["requests_per_domain"][domain] += 1

            cache_payload = response.to_cache_payload()
            cache_payload["fetched_at"] = response.fetched_at.isoformat()
            self.cache.set(url, cache_payload)
            return response

    async def _fetch_with_playwright(self, url: str) -> Optional[CrawlerResponse]:
        assert self._browser is not None
        context = await self._browser.new_context(
            user_agent=self.user_agent,
            java_script_enabled=True,
        )
        page = await context.new_page()
        try:
            primary = await page.goto(url, wait_until="networkidle", timeout=self.request_timeout * 1000)
            body = await page.content()
            status = primary.status if primary else 0
            headers = dict(primary.headers()) if primary else {}
            final_url = primary.url if primary else url
        finally:
            await context.close()

        parsed = urlparse(final_url)
        if not self._is_allowed_host(parsed.hostname):
            raise ValueError(f"Redirect outside allowlist: {final_url}")

        return CrawlerResponse(
            url=final_url,
            status=status,
            body=body,
            headers=headers,
            fetched_at=dt.datetime.now(dt.UTC),
            latency=0.0,
            from_cache=False,
        )

    async def _fetch_with_httpx(self, url: str) -> Optional[CrawlerResponse]:
        assert self._http_client is not None
        resp = await self._http_client.get(url)
        parsed = urlparse(str(resp.url))
        if not self._is_allowed_host(parsed.hostname):
            raise ValueError(f"Redirect outside allowlist: {resp.url}")

        return CrawlerResponse(
            url=str(resp.url),
            status=resp.status_code,
            body=resp.text,
            headers=dict(resp.headers),
            fetched_at=dt.datetime.now(dt.UTC),
            latency=0.0,
            from_cache=False,
        )

    async def _respect_robots(self, url: str) -> None:
        parsed = urlparse(url)
        domain = parsed.hostname
        if domain is None:
            return

        key = domain.lower()
        parser = self._robots.get(key)
        if parser is None:
            parser = RobotFileParser()
            robots_url = f"{parsed.scheme}://{domain}/robots.txt"
            try:
                assert self._http_client is not None
                resp = await self._http_client.get(robots_url)
            except httpx.HTTPError:
                parser.parse([])
            else:
                parser.parse(resp.text.splitlines())
            self._robots[key] = parser

        if not parser.can_fetch(self.user_agent, url):
            raise PermissionError(f"robots.txt forbids fetching {url}")

    async def _enforce_rate_limit(self, domain: str) -> None:
        window = self._domain_windows[domain]
        now = time.perf_counter()
        while window and now - window[0] > 1.0:
            window.popleft()
        while len(window) >= self.max_requests_per_second:
            await asyncio.sleep(0.05)
            now = time.perf_counter()
            while window and now - window[0] > 1.0:
                window.popleft()
        window.append(now)

    async def _enforce_crawl_delay(self, domain: str) -> None:
        now = time.perf_counter()
        last = self._domain_last_fetch[domain]
        wait_for = self.crawl_delay - (now - last)
        if wait_for > 0:
            await asyncio.sleep(wait_for)
        self._domain_last_fetch[domain] = time.perf_counter()

    def _is_allowed_host(self, hostname: Optional[str]) -> bool:
        if hostname is None:
            return False

        host = hostname.lower()
        if host in {"localhost", "127.0.0.1"}:
            return False

        try:
            ip = ipaddress.ip_address(host)
        except ValueError:
            pass
        else:
            if ip.is_private or ip.is_loopback or ip.is_reserved:
                return False

        for domain in self.allowed_domains.keys():
            if host == domain or host.endswith(f".{domain}"):
                return True
        return False


def extract_text_segments(html: str) -> List[str]:
    """Return a list of lower-cased text segments for heuristic parsing."""

    # Remove scripts and styles before tokenisation.
    cleaned = re.sub(r"<script[\s\S]*?</script>", " ", html, flags=re.IGNORECASE)
    cleaned = re.sub(r"<style[\s\S]*?</style>", " ", cleaned, flags=re.IGNORECASE)
    text = re.sub(r"<[^>]+>", " ", cleaned)
    tokens = [token.strip().lower() for token in text.split() if token.strip()]
    return tokens

