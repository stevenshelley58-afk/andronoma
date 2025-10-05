"""Scraping stage implementation."""
from __future__ import annotations

import asyncio
import datetime as dt
import hashlib
import html
import io
import json
import re
import uuid
from collections import Counter
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple
from urllib.parse import urljoin, urlparse

from shared.models import AssetRecord
from shared.stages.base import BaseStage
from shared.storage import put_object

from .cache import ResponseCache
from .crawler import CrawlerResponse, PlaywrightCrawler, extract_text_segments


CrawlerFactory = Callable[..., PlaywrightCrawler]


class ScrapeStage(BaseStage):
    """Scrape brand surfaces, normalize payloads, and persist telemetry."""

    name = "scrape"

    def __init__(
        self,
        context,
        *,
        crawler_factory: Optional[CrawlerFactory] = None,
        cache: Optional[ResponseCache] = None,
        storage_put: Callable[[str, io.BytesIO, int, str], str] = put_object,
    ) -> None:
        super().__init__(context)
        self._crawler_factory = crawler_factory or (lambda **kwargs: PlaywrightCrawler(**kwargs))
        self._cache = cache or ResponseCache()
        self._storage_put = storage_put

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def execute(self) -> Dict[str, Any]:
        self.ensure_budget(10.0)

        run = self.context.run
        payload = run.input_payload or {}

        base_url = self._resolve_base_url(payload)
        parsed = urlparse(base_url)
        if not parsed.hostname:
            raise ValueError("Unable to derive hostname for crawl allowlist")

        hostname = parsed.hostname
        domain_parts = hostname.split(".") if hostname else []
        if len(domain_parts) >= 2:
            registrable = ".".join(domain_parts[-2:])
        else:
            registrable = hostname

        allowed_domains = {hostname}
        if registrable:
            allowed_domains.add(registrable)
        seed_urls = self._build_seed_urls(base_url)

        responses, crawler_metrics = self._crawl(allowed_domains, seed_urls)

        normalized, coverage, notes = self._normalize_payloads(base_url, responses)

        manifest_entries = self._persist_payloads(run.id, normalized)
        manifest_uri = self._upload_manifest(run.id, manifest_entries, coverage, crawler_metrics)

        self._record_asset(run.id, manifest_uri, coverage, crawler_metrics)
        self._store_notes(notes)

        telemetry = {
            "manifest_uri": manifest_uri,
            "coverage": coverage,
            "request_count": crawler_metrics.get("request_count", 0),
            "cache_hits": crawler_metrics.get("cache_hits", 0),
            "latency_seconds": round(crawler_metrics.get("latency_total", 0.0), 3),
            "spend_cents": round(self._estimate_spend(crawler_metrics), 2),
            "cache_available": self._cache.available,
            "notes": notes,
        }
        return telemetry

    # ------------------------------------------------------------------
    # Crawl helpers
    # ------------------------------------------------------------------
    def _resolve_base_url(self, payload: Dict[str, Any]) -> str:
        raw = payload.get("base_url") or payload.get("brand_url")
        if not raw:
            raise ValueError("Scrape stage requires a 'base_url' in the run payload")
        raw = raw.strip()
        parsed = urlparse(raw)
        if not parsed.scheme:
            raw = f"https://{raw}"
        return raw.rstrip("/")

    def _build_seed_urls(self, base_url: str) -> List[str]:
        candidates = [
            base_url,
            urljoin(base_url + "/", "sitemap.xml"),
            urljoin(base_url + "/", "collections"),
            urljoin(base_url + "/", "products"),
            urljoin(base_url + "/", "about"),
            urljoin(base_url + "/", "faq"),
            urljoin(base_url + "/", "blog"),
            urljoin(base_url + "/", "reviews"),
        ]
        seen = set()
        ordered: List[str] = []
        for url in candidates:
            if url not in seen:
                seen.add(url)
                ordered.append(url)
        return ordered

    def _crawl(
        self,
        allowed_domains: Iterable[str],
        seed_urls: Iterable[str],
    ) -> Tuple[Dict[str, CrawlerResponse], Dict[str, Any]]:
        async def runner() -> Tuple[Dict[str, CrawlerResponse], Dict[str, Any]]:
            async with self._crawler_factory(allowed_domains=allowed_domains, cache=self._cache) as crawler:
                results = await crawler.crawl(seed_urls)
                return results, crawler.metrics

        responses, metrics = asyncio.run(runner())
        return responses, metrics

    # ------------------------------------------------------------------
    # Normalization helpers
    # ------------------------------------------------------------------
    def _normalize_payloads(
        self,
        base_url: str,
        responses: Dict[str, CrawlerResponse],
    ) -> Tuple[Dict[str, Any], Dict[str, float], List[str]]:
        products, product_stats = self._collect_products(responses)
        reviews, review_stats = self._collect_reviews(responses)
        seo, seo_stats = self._collect_seo(base_url, responses)
        competitors, competitor_stats = self._collect_competitors(base_url, responses)
        tone, tone_stats = self._collect_tone(responses, seo)

        coverage = {
            "products_pct": product_stats["coverage"],
            "dimensions_pct": product_stats["dimensions_coverage"],
            "reviews_pct": review_stats["coverage"],
            "seo_pct": seo_stats["coverage"],
            "competitors_pct": competitor_stats["coverage"],
            "tone_pct": tone_stats["coverage"],
        }

        notes: List[str] = []
        notes.extend(product_stats["gaps"])
        notes.extend(review_stats["gaps"])
        notes.extend(seo_stats["gaps"])
        notes.extend(competitor_stats["gaps"])
        notes.extend(tone_stats["gaps"])
        if not notes:
            notes.append("Coverage targets met across tracked datasets.")

        normalized = {
            "products": products,
            "reviews": reviews,
            "seo": seo,
            "competitors": competitors,
            "tone": tone,
        }
        return normalized, coverage, notes

    def _collect_products(self, responses: Dict[str, CrawlerResponse]) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
        products: List[Dict[str, Any]] = []
        candidate_urls: set[str] = set()
        products_with_dimensions = 0

        for url, response in responses.items():
            lower = url.lower()
            if any(token in lower for token in ("product", "collection", "item", "shop")):
                candidate_urls.add(url)
                product = self._build_product_payload(url, response)
                if product:
                    if product["dimensions"]["normalized"]:
                        products_with_dimensions += 1
                    products.append(product)

        if not products and responses:
            # Fallback to ensure downstream consumers receive at least one item.
            first_url, first_response = next(iter(responses.items()))
            candidate_urls.add(first_url)
            product = self._build_product_payload(first_url, first_response)
            if product:
                if product["dimensions"]["normalized"]:
                    products_with_dimensions += 1
                products.append(product)

        candidate_count = max(len(candidate_urls), len(products)) or 1
        coverage = round(min(1.0, len(products) / candidate_count) * 100, 2)
        dimensions_coverage = round(
            (products_with_dimensions / len(products)) * 100, 2
        ) if products else 0.0

        gaps: List[str] = []
        if coverage < 95.0:
            gaps.append(f"Product coverage below target (actual={coverage:.1f}% < 95%).")
        if dimensions_coverage < 90.0:
            gaps.append(
                f"Dimensional data captured for {dimensions_coverage:.1f}% of products (<90% target)."
            )

        stats = {
            "coverage": coverage,
            "dimensions_coverage": dimensions_coverage,
            "gaps": gaps,
        }
        return products, stats

    def _build_product_payload(self, url: str, response: CrawlerResponse) -> Optional[Dict[str, Any]]:
        body = response.body
        name = self._extract_title(body) or self._extract_h1(body) or "Untitled Product"
        price = self._extract_price(body)
        dimensions = self._extract_dimensions(body)
        description = self._summarize_text(body)

        return {
            "id": hashlib.sha256(url.encode("utf-8")).hexdigest()[:16],
            "name": name,
            "price": price,
            "dimensions": dimensions,
            "description": description,
            "source": self._source_metadata(response),
        }

    def _collect_reviews(self, responses: Dict[str, CrawlerResponse]) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
        reviews: List[Dict[str, Any]] = []
        candidate_count = 0

        review_block_pattern = re.compile(r"<[^>]*review[^>]*>.*?</[^>]+>", re.IGNORECASE | re.DOTALL)
        rating_pattern = re.compile(r'(data-rating|itemprop="ratingValue")\D*(\d+(?:\.\d+)?)', re.IGNORECASE)
        author_pattern = re.compile(r'(data-author|itemprop="author")\D*([A-Za-z0-9 _-]+)', re.IGNORECASE)
        date_pattern = re.compile(r'(datetime|data-date)[^>]*="([^"]+)"', re.IGNORECASE)

        for url, response in responses.items():
            matches = list(review_block_pattern.finditer(response.body))
            candidate_count += len(matches)
            for match in matches:
                block = match.group(0)
                text = self._sanitize_text(self._strip_tags(block)).strip()
                if not text:
                    continue
                rating_match = rating_pattern.search(block)
                rating = float(rating_match.group(2)) if rating_match else None
                author_match = author_pattern.search(block)
                author = author_match.group(2).strip() if author_match else None
                date_match = date_pattern.search(block)
                timestamp = date_match.group(2) if date_match else None

                review = {
                    "id": hashlib.sha256((url + text).encode("utf-8")).hexdigest()[:16],
                    "content": text[:1000],
                    "rating": rating,
                    "author": author,
                    "timestamp": timestamp,
                    "source": self._source_metadata(response),
                }
                reviews.append(review)

        candidate_count = max(candidate_count, len(reviews), 1)
        coverage = round(min(1.0, len(reviews) / candidate_count) * 100, 2)

        gaps: List[str] = []
        if coverage < 80.0:
            gaps.append(f"Review coverage below expectation ({coverage:.1f}% < 80%).")

        stats = {"coverage": coverage, "gaps": gaps}
        return reviews, stats

    def _collect_seo(
        self,
        base_url: str,
        responses: Dict[str, CrawlerResponse],
    ) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        primary = responses.get(base_url)
        if not primary and responses:
            primary = next(iter(responses.values()))

        meta_title = self._extract_title(primary.body) if primary else None
        meta_description = self._extract_meta(primary.body, "description") if primary else None
        meta_keywords = self._extract_meta(primary.body, "keywords") if primary else None

        headings = {"h1": [], "h2": []}
        alt_text: List[str] = []
        keyword_counter: Counter[str] = Counter()

        heading_pattern = re.compile(r"<(h[12])[^>]*>(.*?)</\1>", re.IGNORECASE | re.DOTALL)
        alt_pattern = re.compile(r"<img[^>]*alt=\"([^\"]+)\"", re.IGNORECASE)

        for response in responses.values():
            for match in heading_pattern.finditer(response.body):
                tag = match.group(1).lower()
                headings[tag].append(self._sanitize_text(self._strip_tags(match.group(2))))
            for alt in alt_pattern.findall(response.body):
                cleaned = self._sanitize_text(alt.strip())
                if cleaned:
                    alt_text.append(cleaned)
            keyword_counter.update(token for token in extract_text_segments(response.body) if len(token) > 4)

        seo_payload = {
            "meta": {
                "title": meta_title,
                "description": meta_description,
                "keywords": meta_keywords,
            },
            "headings": headings,
            "alt_text": alt_text[:50],
            "structured_keywords": [word for word, _ in keyword_counter.most_common(25)],
            "source": self._source_metadata(primary) if primary else None,
        }

        total_fields = 5  # title, description, keywords, h1, h2/alt bucket
        collected = sum(
            1
            for item in (
                meta_title,
                meta_description,
                meta_keywords,
                headings["h1"],
                headings["h2"],
            )
            if item
        )
        coverage = round((collected / total_fields) * 100, 2)

        gaps: List[str] = []
        if coverage < 90.0:
            gaps.append(f"SEO signals incomplete ({coverage:.1f}% < 90% threshold).")

        stats = {"coverage": coverage, "gaps": gaps}
        return seo_payload, stats

    def _collect_competitors(
        self,
        base_url: str,
        responses: Dict[str, CrawlerResponse],
    ) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
        parsed_base = urlparse(base_url)
        base_host = parsed_base.hostname or ""
        competitors: Dict[str, Dict[str, Any]] = {}

        link_pattern = re.compile(r"<a[^>]*href=\"([^\"]+)\"[^>]*>(.*?)</a>", re.IGNORECASE | re.DOTALL)

        for response in responses.values():
            for href, anchor in link_pattern.findall(response.body):
                absolute = urljoin(response.url, href)
                parsed = urlparse(absolute)
                if not parsed.hostname or parsed.hostname.endswith(base_host):
                    continue
                host = parsed.hostname.lower()
                if host not in competitors:
                    snippet = self._sanitize_text(self._strip_tags(anchor))
                    context = self._infer_competitor_context(response.body, anchor)
                    competitors[host] = {
                        "name": snippet or host,
                        "url": absolute,
                        "price_positioning": context["price"],
                        "shipping_model": context["shipping"],
                        "differentiators": context["differentiators"],
                        "source": self._source_metadata(response),
                    }

        competitor_list = list(competitors.values())[:7]
        coverage = round(min(1.0, len(competitor_list) / 3) * 100, 2) if competitor_list else 0.0

        gaps: List[str] = []
        if coverage < 100.0:
            gaps.append(
                "Competitor discovery yielded fewer than three distinct brands."
            )

        stats = {"coverage": coverage, "gaps": gaps}
        return competitor_list, stats

    def _collect_tone(
        self,
        responses: Dict[str, CrawlerResponse],
        seo_payload: Dict[str, Any],
    ) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        descriptor_bank = {
            "luxury",
            "minimal",
            "sustainable",
            "playful",
            "bold",
            "vibrant",
            "heritage",
            "inclusive",
            "innovative",
            "artisan",
        }

        descriptors: List[str] = []
        evidence: List[str] = []

        for response in responses.values():
            tokens = extract_text_segments(response.body)
            matches = descriptor_bank.intersection(tokens)
            if matches:
                descriptors.extend(sorted(matches))
                evidence.append(self._summarize_text(response.body, limit=160))

        descriptors = sorted(set(descriptors))
        if not descriptors and seo_payload.get("meta", {}).get("description"):
            descriptors = self._fallback_descriptors(seo_payload["meta"]["description"])

        tone_payload = {
            "descriptors": descriptors,
            "evidence": evidence[:5],
        }

        coverage = round(min(1.0, len(descriptors) / 5) * 100, 2) if descriptors else 0.0
        gaps: List[str] = []
        if coverage < 80.0:
            gaps.append("Tone analysis produced fewer than five distinct descriptors.")

        stats = {"coverage": coverage, "gaps": gaps}
        return tone_payload, stats

    # ------------------------------------------------------------------
    # Persistence helpers
    # ------------------------------------------------------------------
    def _persist_payloads(
        self,
        run_id: Any,
        payloads: Dict[str, Any],
    ) -> List[Dict[str, Any]]:
        base_path = Path("/data/raw/research") / str(run_id)
        base_path.mkdir(parents=True, exist_ok=True)

        manifest_entries: List[Dict[str, Any]] = []
        for name, content in payloads.items():
            file_path = base_path / f"{name}.json"
            encoded = json.dumps(content, indent=2, ensure_ascii=False).encode("utf-8")
            file_path.write_bytes(encoded)
            checksum = hashlib.sha256(encoded).hexdigest()
            if isinstance(content, list):
                records = len(content)
            elif isinstance(content, dict):
                records = len(content)
            else:
                records = 1
            manifest_entries.append(
                {
                    "name": name,
                    "path": str(file_path),
                    "records": records,
                    "checksum": checksum,
                }
            )
        return manifest_entries

    def _upload_manifest(
        self,
        run_id: Any,
        entries: List[Dict[str, Any]],
        coverage: Dict[str, float],
        metrics: Dict[str, Any],
    ) -> str:
        manifest = {
            "run_id": str(run_id),
            "generated_at": dt.datetime.utcnow().isoformat(),
            "datasets": entries,
            "coverage": coverage,
            "crawler_metrics": metrics,
        }
        payload = json.dumps(manifest, indent=2, ensure_ascii=False).encode("utf-8")
        stream = io.BytesIO(payload)
        key = f"research/{run_id}/manifest.json"
        return self._storage_put(key, stream, len(payload), "application/json")

    def _record_asset(
        self,
        run_id: Any,
        manifest_uri: str,
        coverage: Dict[str, float],
        metrics: Dict[str, Any],
    ) -> None:
        record = AssetRecord(
            id=uuid.uuid4(),
            run_id=run_id,
            stage=self.name,
            asset_type="raw_research_manifest",
            storage_key=manifest_uri,
            extra={
                "coverage": coverage,
                "request_count": metrics.get("request_count", 0),
                "cache_hits": metrics.get("cache_hits", 0),
            },
        )
        self.context.session.add(record)
        self.context.session.commit()

    def _store_notes(self, notes: List[str]) -> None:
        state = next((s for s in self.context.run.stages if s.name == self.name), None)
        if state is None:
            return
        state.notes = "\n".join(notes)
        self.context.session.commit()

    # ------------------------------------------------------------------
    # Utility helpers
    # ------------------------------------------------------------------
    def _estimate_spend(self, metrics: Dict[str, Any]) -> float:
        request_count = metrics.get("request_count", 0)
        return request_count * 0.25  # cents per request (approximation)

    def _source_metadata(self, response: Optional[CrawlerResponse]) -> Optional[Dict[str, Any]]:
        if response is None:
            return None
        return {
            "url": response.url,
            "retrieved_at": response.fetched_at.isoformat(),
            "method": "cache" if response.from_cache else "crawl",
        }

    def _extract_title(self, body: str) -> Optional[str]:
        match = re.search(r"<title[^>]*>(.*?)</title>", body, re.IGNORECASE | re.DOTALL)
        if match:
            return self._sanitize_text(html.unescape(match.group(1))).strip()
        return None

    def _extract_h1(self, body: str) -> Optional[str]:
        match = re.search(r"<h1[^>]*>(.*?)</h1>", body, re.IGNORECASE | re.DOTALL)
        if match:
            return self._sanitize_text(self._strip_tags(match.group(1))).strip()
        return None

    def _extract_meta(self, body: str, name: str) -> Optional[str]:
        pattern = re.compile(
            rf"<meta[^>]+name=\"{re.escape(name)}\"[^>]+content=\"([^\"]+)\"",
            re.IGNORECASE,
        )
        match = pattern.search(body)
        if match:
            return self._sanitize_text(match.group(1).strip())
        return None

    def _extract_price(self, body: str) -> Dict[str, Any]:
        pattern = re.compile(r"(\$|€|£)\s?(\d{1,3}(?:[\d,]*)(?:\.\d{2})?)")
        match = pattern.search(body)
        if not match:
            return {"currency": None, "value_minor": None, "display": None}

        currency_symbol = match.group(1)
        raw_value = match.group(2).replace(",", "")
        try:
            value = float(raw_value)
        except ValueError:
            value = 0.0
        minor = int(round(value * 100))
        currency_map = {"$": "USD", "€": "EUR", "£": "GBP"}
        return {
            "currency": currency_map.get(currency_symbol, currency_symbol),
            "value_minor": minor,
            "display": f"{currency_symbol}{value:0.2f}",
        }

    def _extract_dimensions(self, body: str) -> Dict[str, Any]:
        pattern = re.compile(
            r"(\d+(?:\.\d+)?)\s?(cm|mm|in|inch|kg|g|lb|oz)",
            re.IGNORECASE,
        )
        normalized: List[Dict[str, Any]] = []
        for value, unit in pattern.findall(body):
            try:
                numeric = float(value)
            except ValueError:
                continue
            unit_lower = unit.lower()
            if unit_lower in {"cm"}:
                normalized.append({"value": round(numeric, 2), "unit": "cm"})
            elif unit_lower in {"mm"}:
                normalized.append({"value": round(numeric / 10.0, 2), "unit": "cm"})
            elif unit_lower in {"in", "inch"}:
                normalized.append({"value": round(numeric * 2.54, 2), "unit": "cm"})
            elif unit_lower == "kg":
                normalized.append({"value": round(numeric, 2), "unit": "kg"})
            elif unit_lower == "g":
                normalized.append({"value": round(numeric / 1000.0, 2), "unit": "kg"})
            elif unit_lower == "lb":
                normalized.append({"value": round(numeric * 0.453592, 2), "unit": "kg"})
            elif unit_lower == "oz":
                normalized.append({"value": round(numeric * 0.0283495, 2), "unit": "kg"})

        return {"normalized": normalized}

    def _summarize_text(self, body: str, limit: int = 240) -> str:
        text = self._sanitize_text(self._strip_tags(body))
        return text[:limit].strip()

    def _strip_tags(self, html_fragment: str) -> str:
        return re.sub(r"<[^>]+>", " ", html_fragment)

    def _sanitize_text(self, text: str) -> str:
        without_emails = re.sub(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+", "[redacted-email]", text)
        without_phone = re.sub(r"(?:\+?\d[\d -]{7,}\d)", "[redacted-phone]", without_emails)
        return " ".join(without_phone.split())

    def _fallback_descriptors(self, description: Optional[str]) -> List[str]:
        if not description:
            return []
        tokens = [token.strip(".,!").lower() for token in description.split() if token]
        stopwords = {"the", "and", "with", "from", "that", "this", "your", "for"}
        descriptors = [t for t in tokens if len(t) > 4 and t not in stopwords]
        return sorted(set(descriptors[:5]))

    def _infer_competitor_context(self, body: str, anchor: str) -> Dict[str, Any]:
        snippet = self._strip_tags(anchor)
        window_size = 120
        index = body.find(anchor)
        if index == -1:
            context = ""
        else:
            start = max(0, index - window_size)
            end = min(len(body), index + len(anchor) + window_size)
            context = self._strip_tags(body[start:end])
        context_clean = self._sanitize_text(context.lower())

        price_hint = "premium" if context_clean.count("$") >= 3 else "mid" if "$" in context_clean else None
        shipping_hint = "free shipping" if "free shipping" in context_clean else None
        differentiators = []
        for keyword in ("sustainable", "handmade", "bespoke", "fast shipping"):
            if keyword in context_clean:
                differentiators.append(keyword)

        return {
            "price": price_hint,
            "shipping": shipping_hint,
            "differentiators": differentiators,
        }

