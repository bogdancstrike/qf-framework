"""
API service layer — Redis result caching, OTel tracing, request stats.

Mirrors the pattern from poc_app/src/service/api_handler.py.
See that file for full documentation of the caching lifecycle.

Key conventions
---------------
  <app>:cache:<16-hex>    — cached result for (endpoint, payload) pair
  <app>:stats:<endpoint>  — integer call counter per endpoint
"""

import hashlib
import json
import time
from typing import Callable, Optional

from framework.commons.logger import logger
from framework.tracing import get_tracer

CACHE_TTL_SEC = 60

# Namespace prefix — change APP_NAME or set via config if running multiple apps.
_PREFIX = "qf"

CACHE_KEY_PREFIX = f"{_PREFIX}:cache:"
STATS_KEY_PREFIX = f"{_PREFIX}:stats:"


def cached_enrich(
    worker_fn: Callable,
    payload: dict,
    endpoint_name: str,
    consumer_name: str = "api",
    extra_metadatas: Optional[dict] = None,
) -> dict:
    """Call a worker function with Redis result caching and OTel tracing.

    Cache key is derived from (endpoint_name + JSON-serialised payload) so two
    endpoints with identical payloads never share a cache entry.

    Args:
        worker_fn:       The worker function to call on a cache miss.
        payload:         The request body dict.
        endpoint_name:   Short name used for spans, stats, and cache keys.
        consumer_name:   Passed to worker_fn as consumer_name kwarg.
        extra_metadatas: Additional metadata merged into {"via": "http"}.

    Returns:
        The enriched dict (from cache or freshly computed).

    Raises:
        Any exception raised by worker_fn propagates to the caller.
    """
    from instances import get_redis

    tracer = get_tracer()
    metadatas = {"via": "http", **(extra_metadatas or {})}
    redis = get_redis()

    raw = json.dumps({"ep": endpoint_name, "payload": payload}, sort_keys=True)
    cache_key = CACHE_KEY_PREFIX + hashlib.sha256(raw.encode()).hexdigest()[:16]

    with tracer.start_as_current_span(f"api.{endpoint_name}") as span:
        span.set_attribute("endpoint", endpoint_name)
        span.set_attribute("payload.id", str(payload.get("id", "")))

        try:
            call_count = redis.increment_key(f"{STATS_KEY_PREFIX}{endpoint_name}")
            span.set_attribute("stats.call_count", call_count)
        except Exception:
            call_count = -1

        t0 = time.perf_counter()

        # Cache lookup
        try:
            cached_raw = redis.get_key(cache_key)
            if cached_raw:
                result = json.loads(cached_raw)
                elapsed_ms = (time.perf_counter() - t0) * 1000
                logger.debug(
                    f"[api_handler] CACHE HIT endpoint={endpoint_name} "
                    f"key={cache_key} elapsed={elapsed_ms:.1f}ms"
                )
                span.set_attribute("cache.hit", True)
                return result
        except Exception as exc:
            logger.warning(f"[api_handler] cache read error: {exc}")
            span.set_attribute("cache.read_error", str(exc))

        span.set_attribute("cache.hit", False)

        # Worker call
        try:
            result = worker_fn(payload, consumer_name=consumer_name, metadatas=metadatas)
        except Exception as exc:
            elapsed_ms = (time.perf_counter() - t0) * 1000
            logger.error(
                f"[api_handler] ERROR endpoint={endpoint_name} "
                f"elapsed={elapsed_ms:.1f}ms error={exc}"
            )
            span.record_exception(exc)
            raise

        # Cache store
        try:
            redis.set_key(cache_key, json.dumps(result), expire=CACHE_TTL_SEC)
        except Exception as exc:
            logger.warning(f"[api_handler] cache write error: {exc}")

        elapsed_ms = (time.perf_counter() - t0) * 1000
        logger.debug(
            f"[api_handler] OK endpoint={endpoint_name} elapsed={elapsed_ms:.1f}ms"
        )
        span.set_attribute("elapsed_ms", round(elapsed_ms, 1))
        return result


def get_stats() -> dict:
    """Read per-endpoint call counters from Redis."""
    from instances import get_redis
    tracer = get_tracer()
    with tracer.start_as_current_span("api.stats") as span:
        try:
            redis = get_redis()
            keys = redis.list_all_keys(pattern=f"{STATS_KEY_PREFIX}*")
            stats = {k[len(STATS_KEY_PREFIX):]: int(redis.get_key(k) or 0) for k in keys}
            span.set_attribute("stats.endpoints", len(stats))
            return stats
        except Exception as exc:
            logger.error(f"[api_handler] stats error: {exc}")
            span.record_exception(exc)
            return {"error": str(exc)}
