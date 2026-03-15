"""
Health check service — verifies connectivity to all third-party dependencies.

Each check is isolated: a failure in one does not prevent the others from
running.  The overall status is "ok" only when every component reports "ok".

Used by:
  GET /health  — liveness / readiness probe for Kubernetes or load balancers

Check implementations
----------------------
  Redis   — PING via the connection pool; fast, single round-trip
  Kafka   — list_topics() via KafkaAdminClient; confirms broker reachability
  Postgres — SELECT 1 via SQLAlchemy engine; confirms DB + auth

All checks run synchronously.  Each has a 5s wall-clock budget enforced by
the socket_timeout / connect_timeout config values on the underlying clients.
"""

from __future__ import annotations

from typing import Optional

from framework.commons.logger import logger
from framework.tracing import get_tracer


# ---------------------------------------------------------------------------
# Individual checks
# ---------------------------------------------------------------------------

def _check_redis() -> dict:
    """PING the Redis connection pool.  Returns {"status": "ok|error", ...}."""
    try:
        from instances import get_redis
        get_redis().redis.ping()
        return {"status": "ok"}
    except Exception as exc:
        logger.warning(f"[health] Redis check failed: {exc}")
        return {"status": "error", "detail": str(exc)}


def _check_kafka() -> dict:
    """List Kafka topics via AdminClient.  Returns {"status": "ok|error", ...}."""
    try:
        from instances import get_kafka
        client = get_kafka()
        # list_topics() is the lightest admin operation — one metadata request.
        topics = client.admin_client.list_topics()
        return {"status": "ok", "topics": len(topics)}
    except Exception as exc:
        logger.warning(f"[health] Kafka check failed: {exc}")
        return {"status": "error", "detail": str(exc)}


def _check_postgres() -> dict:
    """Execute SELECT 1 via SQLAlchemy.  Returns {"status": "ok|error", ...}."""
    try:
        from sqlalchemy import text
        from instances import get_engine
        with get_engine().connect() as conn:
            conn.execute(text("SELECT 1"))
        return {"status": "ok"}
    except Exception as exc:
        logger.warning(f"[health] Postgres check failed: {exc}")
        return {"status": "error", "detail": str(exc)}


# ---------------------------------------------------------------------------
# Aggregated check
# ---------------------------------------------------------------------------

def check_all() -> dict:
    """Run all health checks and return a combined status dict.

    Returns:
        {
            "status":   "ok" | "degraded",
            "redis":    {"status": "ok"} | {"status": "error", "detail": "..."},
            "kafka":    {"status": "ok", "topics": N} | {"status": "error", ...},
            "postgres": {"status": "ok"} | {"status": "error", "detail": "..."},
        }

    HTTP status code guidance:
        200 when status == "ok"
        200 or 503 when status == "degraded" (your choice — 503 is more correct
        for readiness probes that should fail closed)
    """
    tracer = get_tracer()
    with tracer.start_as_current_span("health.check_all") as span:
        redis_result    = _check_redis()
        kafka_result    = _check_kafka()
        postgres_result = _check_postgres()

        all_ok = all(
            r["status"] == "ok"
            for r in (redis_result, kafka_result, postgres_result)
        )
        overall = "ok" if all_ok else "degraded"

        span.set_attribute("health.status", overall)
        span.set_attribute("health.redis",    redis_result["status"])
        span.set_attribute("health.kafka",    kafka_result["status"])
        span.set_attribute("health.postgres", postgres_result["status"])

        result = {
            "status":   overall,
            "redis":    redis_result,
            "kafka":    kafka_result,
            "postgres": postgres_result,
        }
        logger.debug(f"[health] {result}")
        return result
