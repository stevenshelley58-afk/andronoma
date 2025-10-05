"""Redis-backed response caching utilities for the scraping stage."""
from __future__ import annotations

import hashlib
import json
import os
from typing import Any, Dict, Optional

import redis
from redis import Redis
from redis.exceptions import RedisError


class ResponseCache:
    """Persist crawl responses in Redis so reruns can reuse prior data."""

    def __init__(
        self,
        client: Optional[Redis] = None,
        *,
        ttl_seconds: int = 60 * 60 * 24,
        namespace: str = "scrape",
        redis_url: Optional[str] = None,
    ) -> None:
        self.namespace = namespace
        self.ttl_seconds = ttl_seconds
        self._hits = 0
        self._misses = 0
        self._fallback_store: Dict[str, str] = {}

        if client is not None:
            self._client = client
            self._available = True
            return

        url = (
            redis_url
            or os.getenv("ANDRONOMA_CACHE_URL")
            or os.getenv("REDIS_URL")
            or "redis://localhost:6379/2"
        )
        try:
            candidate = redis.Redis.from_url(url)
            candidate.ping()
        except RedisError:
            # Redis is unavailable – fall back to an in-memory cache while still
            # allowing the caller to inspect metrics for hit/miss ratios.
            self._client = None
            self._available = False
        else:
            self._client = candidate
            self._available = True

    @property
    def available(self) -> bool:
        """Return whether the backend Redis store is reachable."""

        return self._available

    @property
    def stats(self) -> Dict[str, int]:
        """Expose cache hit/miss counts for telemetry."""

        return {"hits": self._hits, "misses": self._misses}

    def _key_for(self, url: str) -> str:
        digest = hashlib.sha256(url.encode("utf-8")).hexdigest()
        return f"{self.namespace}:{digest}"

    def get(self, url: str) -> Optional[Dict[str, Any]]:
        """Return a cached payload for ``url`` if one exists."""

        key = self._key_for(url)
        payload: Optional[str] = None

        if self._available and self._client is not None:
            try:
                result = self._client.get(key)
            except RedisError:
                self._available = False
                result = None
            if result is not None:
                payload = result.decode("utf-8") if isinstance(result, bytes) else str(result)

        if payload is None:
            payload = self._fallback_store.get(key)

        if payload is None:
            self._misses += 1
            return None

        self._hits += 1
        try:
            return json.loads(payload)
        except json.JSONDecodeError:
            # Corrupted payloads should be treated as a miss so callers refetch.
            self._misses += 1
            return None

    def set(self, url: str, value: Dict[str, Any]) -> None:
        """Persist ``value`` for ``url`` in Redis (and fall back to memory)."""

        key = self._key_for(url)
        encoded = json.dumps(value, ensure_ascii=False)

        if self._available and self._client is not None:
            try:
                self._client.setex(key, self.ttl_seconds, encoded)
            except RedisError:
                # Flip the availability flag so future lookups use the fallback.
                self._available = False

        if not self._available:
            self._fallback_store[key] = encoded

