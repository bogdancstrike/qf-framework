"""
API service layer — Redis caching, OTel tracing, logging, and request stats.

This module sits between api_endpoints.py (HTTP boundary) and the worker
functions (business logic).  Keeping these concerns here — rather than in
each endpoint function — means:
  - api_endpoints.py stays thin and readable.
  - Caching/tracing/stats logic is tested independently of HTTP plumbing.
  - Adding a new endpoint only requires delegating to cached_enrich().

Request lifecycle (happy path)
-------------------------------
  HTTP → api_endpoints.worker_X()
       → cached_enrich(worker_fn, payload, endpoint_name)
           ├── increment Redis stats counter                  (RedisUtils)
           ├── start OTel span "api.<endpoint_name>"          (get_tracer)
           ├── check Redis for cached result                  (RedisUtils.get_key)
           │     └── CACHE HIT  → deserialise + return immediately
           ├── call worker_fn(payload, ...)                   (workers.py)
           ├── store result in Redis with TTL=60s             (RedisUtils.set_key)
           └── return result

Redis key conventions
---------------------
  poc:cache:<16-hex>     — cached result for a specific (endpoint, payload) pair
  poc:stats:<endpoint>   — integer call counter per endpoint name

Tracing
-------
  get_tracer() returns either a real OTel tracer (ENABLE_TRACING=true) or
  a NoOpTracer (ENABLE_TRACING=false, default).  All span calls are always
  safe regardless of the tracing mode.

Logging
-------
  All log lines are emitted at DEBUG level via the framework logger.
  The logger automatically injects the W3C traceparent into each line when
  a real OTel span is active, enabling log-to-trace correlation in Jaeger.
"""

import hashlib
import json
import time
from typing import Callable, Optional

from framework.commons.logger import logger
from framework.redis.redis_utils import RedisUtils
from framework.tracing import get_tracer

# ---------------------------------------------------------------------------
# Redis client (lazy singleton)
# ---------------------------------------------------------------------------

# Lazily initialised on first use so imports during tests do not require
# a live Redis connection.
_redis_client: Optional[RedisUtils] = None


def _get_redis() -> RedisUtils:
    """Return the shared RedisUtils instance, creating it on first call.

    Configuration is read from config.Config so that env-var overrides
    (REDIS_HOST, REDIS_PORT, etc.) are respected at runtime.
    """
    global _redis_client
    if _redis_client is None:
        # Late import avoids a circular dependency at module load time:
        # api_endpoints → api_handler → config → (no further deps).
        from config import Config  # type: ignore

        _redis_client = RedisUtils(
            host=Config.REDIS_HOST,
            port=int(Config.REDIS_PORT),
            db=int(Config.REDIS_DB),
            max_connections=int(getattr(Config, "REDIS_MAX_CONNECTIONS", 50)),
            socket_timeout=float(getattr(Config, "REDIS_SOCKET_TIMEOUT", 5.0)),
            socket_connect_timeout=float(getattr(Config, "REDIS_CONNECT_TIMEOUT", 5.0)),
            retry_on_timeout=getattr(Config, "REDIS_RETRY_ON_TIMEOUT", "true").lower() == "true",
        )
    return _redis_client


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# How long cached results live in Redis.  Short enough that stale data
# won't persist past a model update; long enough to help under burst traffic.
CACHE_TTL_SEC = 60

# Prefix for per-endpoint call counters stored as Redis strings.
STATS_KEY_PREFIX = "poc:stats:"

# Prefix for cached result keys.
CACHE_KEY_PREFIX = "poc:cache:"


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def cached_enrich(
    worker_fn: Callable,
    payload: dict,
    endpoint_name: str,
    consumer_name: str = "api",
    extra_metadatas: Optional[dict] = None,
) -> dict:
    """Call a worker function with Redis result caching and OTel tracing.

    Cache key is derived from (endpoint_name + JSON-serialised payload), so
    two different endpoints with identical payloads never share cache entries.
    The first 16 hex digits of the SHA-256 hash give a 64-bit bucket — ample
    for a PoC cache with no cryptographic requirements.

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
    tracer = get_tracer()
    metadatas = {"via": "http", **(extra_metadatas or {})}
    redis = _get_redis()

    # Build a stable, deterministic cache key.
    raw = json.dumps({"ep": endpoint_name, "payload": payload}, sort_keys=True)
    cache_key = CACHE_KEY_PREFIX + hashlib.sha256(raw.encode()).hexdigest()[:16]

    with tracer.start_as_current_span(f"api.{endpoint_name}") as span:
        span.set_attribute("endpoint", endpoint_name)
        span.set_attribute("payload.id", str(payload.get("id", "")))

        # ---- stats counter ------------------------------------------------
        # Increment even on cache hits so the counter reflects total calls,
        # not just cache misses.  Redis INCR is atomic, so concurrent
        # Flask threads never lose a count.
        try:
            call_count = redis.increment_key(f"{STATS_KEY_PREFIX}{endpoint_name}")
            span.set_attribute("stats.call_count", call_count)
        except Exception:
            call_count = -1  # Redis unavailable — non-fatal

        t0 = time.perf_counter()

        # ---- cache lookup -------------------------------------------------
        try:
            cached_raw = redis.get_key(cache_key)
            if cached_raw:
                result = json.loads(cached_raw)
                elapsed_ms = (time.perf_counter() - t0) * 1000
                logger.debug(
                    f"[api_handler] CACHE HIT  endpoint={endpoint_name} "
                    f"id={payload.get('id')} key={cache_key} "
                    f"elapsed={elapsed_ms:.1f}ms count={call_count}"
                )
                span.set_attribute("cache.hit", True)
                return result
        except Exception as exc:
            # Cache read failure is non-fatal: fall through to live call.
            logger.warning(f"[api_handler] cache read error endpoint={endpoint_name}: {exc}")
            span.set_attribute("cache.read_error", str(exc))

        span.set_attribute("cache.hit", False)
        logger.debug(
            f"[api_handler] CACHE MISS endpoint={endpoint_name} "
            f"id={payload.get('id')} key={cache_key}"
        )

        # ---- worker call --------------------------------------------------
        try:
            result = worker_fn(payload, consumer_name=consumer_name, metadatas=metadatas)
        except Exception as exc:
            elapsed_ms = (time.perf_counter() - t0) * 1000
            logger.error(
                f"[api_handler] ERROR endpoint={endpoint_name} "
                f"id={payload.get('id')} elapsed={elapsed_ms:.1f}ms error={exc}"
            )
            span.record_exception(exc)
            raise  # propagate to Flask so it can return a 500

        # ---- cache store --------------------------------------------------
        # TTL ensures stale results are eventually evicted even if the cache
        # is never explicitly invalidated (which the PoC doesn't do).
        try:
            redis.set_key(cache_key, json.dumps(result), expire=CACHE_TTL_SEC)
        except Exception as exc:
            # Non-fatal: result is already computed; we just skip the caching speedup.
            logger.warning(f"[api_handler] cache write error endpoint={endpoint_name}: {exc}")

        elapsed_ms = (time.perf_counter() - t0) * 1000
        logger.debug(
            f"[api_handler] OK  endpoint={endpoint_name} "
            f"id={payload.get('id')} elapsed={elapsed_ms:.1f}ms count={call_count}"
        )
        span.set_attribute("elapsed_ms", round(elapsed_ms, 1))
        return result


def get_stats() -> dict:
    """Read per-endpoint call counters from Redis.

    Scans for all keys matching poc:stats:* and returns a dict of
    {endpoint_name: call_count}.  Using pattern-scan rather than a hard-coded
    list means new endpoints appear automatically without code changes here.

    Returns:
        {"ner": 42, "translate": 13, ...}  or  {"error": "..."} on failure.
    """
    tracer = get_tracer()
    with tracer.start_as_current_span("api.stats") as span:
        try:
            redis = _get_redis()
            keys = redis.list_all_keys(pattern=f"{STATS_KEY_PREFIX}*")
            stats = {}
            for key in keys:
                # Strip the prefix to get the bare endpoint name.
                name = key[len(STATS_KEY_PREFIX):]
                stats[name] = int(redis.get_key(key) or 0)
            span.set_attribute("stats.endpoints", len(stats))
            logger.debug(f"[api_handler] stats={stats}")
            return stats
        except Exception as exc:
            logger.error(f"[api_handler] stats error: {exc}")
            span.record_exception(exc)
            return {"error": str(exc)}


def health_check() -> dict:
    """Check liveness of dependent services: Redis.

    Suitable for Kubernetes liveness/readiness probes.  Tries a Redis PING
    to confirm the connection pool is healthy.

    Returns:
        {"status": "ok",      "redis": "ok"}
        {"status": "degraded","redis": "error: <message>"}
    """
    redis_status = "ok"
    try:
        # PING is the lightest possible check — one round-trip, no data.
        _get_redis().redis.ping()
    except Exception as exc:
        redis_status = f"error: {exc}"

    overall = "ok" if redis_status == "ok" else "degraded"
    result = {"status": overall, "redis": redis_status}
    logger.debug(f"[api_handler] health={result}")
    return result
