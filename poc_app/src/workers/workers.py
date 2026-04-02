# workers.py
import random
import time
from framework.commons.logger import logger
from framework.decorators import (
    kafka_handler,
    kafka_aggregator,
    rate_limit,
    circuit_breaker,
    retry_to_dlq,
    call_retry,
    call_circuit_breaker,
    call_rate_limit,
    RetryExhaustedError,
    CircuitOpenError,
    RateLimitExceededError,
)

# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------
def _should_fail(msg: dict, *, default_prob: float) -> bool:
    """
    Failure control:
      - force_fail=True -> always fail
      - fail_prob=<0..1> -> probabilistic
      - else uses default_prob
    """
    if msg.get("force_fail") is True:
        return True
    p = msg.get("fail_prob")
    if p is None:
        p = default_prob
    try:
        p = float(p)
    except Exception:
        p = default_prob
    return random.random() < max(0.0, min(1.0, p))


def _touch_enrichment(msg: dict, key: str, value):
    msg.setdefault("enrichment", {})[key] = value
    return msg


# ---------------------------------------------------------------------
# Simulated external service calls — decorated with common.py policies
#
# In a real application these would call external HTTP APIs, databases,
# ML models, etc.  Here they do lightweight dict work to demonstrate
# the decorator behaviour without external dependencies.
#
# call_retry     — retries on transient errors (tenacity)
# call_circuit_breaker — trips open after consecutive failures (pybreaker)
# call_rate_limit      — caps calls per second (limits)
# ---------------------------------------------------------------------

@call_retry(max_attempts=3, wait_fixed=0.01, exceptions=(RuntimeError,))
@call_circuit_breaker(fail_max=10, reset_timeout=5, name="enrichment-breaker")
def _enrich_single(msg: dict, worker_tag: str) -> dict:
    """Simulated enrichment call — retried up to 3× on RuntimeError."""
    return _touch_enrichment(msg, worker_tag, {"ok": True, "ts": time.time()})


@call_retry(max_attempts=2, wait_fixed=0.005, exceptions=(RuntimeError,))
@call_circuit_breaker(fail_max=20, reset_timeout=5, name="bulk-enrichment-breaker")
def _enrich_batch(messages: list, worker_tag: str) -> list:
    """Simulated bulk enrichment call — retried up to 2× on RuntimeError."""
    return [
        _touch_enrichment(dict(m), worker_tag, {"ok": True, "ts": time.time()})
        for m in messages
    ]


@call_rate_limit(per_second=10_000, key="agg-postprocess")
def _postprocess_merged(merged: dict, worker_tag: str) -> dict:
    """Simulated post-processing after aggregation — rate limited."""
    return _touch_enrichment(merged, worker_tag, {"merged": True, "ts": time.time()})


# ============================================================
# A) ONLY kafka_handler (single + bulk) - no policies
# ============================================================

@kafka_handler(
    name="echo_single",
    topics_in=["poc.echo.single.in"],
    topics_out=["poc.echo.single.out"],
    max_workers=100,
    bulk_mode=False,
    metadatas={"worker": "echo_single"},
)
def echo_single(message: dict, consumer_name: str, metadatas: dict) -> dict:
    return _enrich_single(message, "echo_single")


@kafka_handler(
    name="echo_bulk",
    topics_in=["poc.echo.bulk.in"],
    topics_out=["poc.echo.bulk.out"],
    max_workers=100,
    bulk_mode=True,
    batch_size=100,
    batch_timeout_ms=1000,
    metadatas={"worker": "echo_bulk", "mode": "bulk"},
)
def echo_bulk(messages: list[dict], consumer_name: str, metadatas: dict):
    return _enrich_batch(messages, "echo_bulk")


# # ============================================================
# # B) ONLY kafka_aggregator - no policies
# # ============================================================

@kafka_aggregator(
    name="agg_basic",
    topics_in=["poc.agg.basic.a", "poc.agg.basic.b"],
    topics_out=["poc.agg.basic.out"],
    aggregate_by="id",
    aggregator_timeout_sec=3600 * 24,
    max_workers=100,
    metadatas={"worker": "agg_basic", "mode": "aggregator"},
)
def agg_basic_after_merge(merged: dict, consumer_name: str, metadatas: dict) -> dict:
    return _postprocess_merged(merged, "agg_basic")


# ============================================================
# C) kafka_handler + retry_to_dlq (single + bulk)
#    - random fails -> retries -> eventually DLQ
#
# Layers:
#   @retry_to_dlq    — Kafka-level retry: re-queues message back to
#                      input topic up to max_attempts, then DLQ
#   @call_retry      — call-level retry: retries the enrichment helper
#                      up to 3× on RuntimeError before raising to the
#                      Kafka runtime (which then applies retry_to_dlq)
# ============================================================

@kafka_handler(
    name="retry_single",
    topics_in=["poc.retry.single.in"],
    topics_out=["poc.retry.single.out"],
    max_workers=100,
    bulk_mode=False,
    metadatas={"worker": "retry_single"},
)
@retry_to_dlq(max_attempts=2, dlq_topic="poc.dlq.retry.single", retry_count_field="retry_count")
def retry_single(message: dict, consumer_name: str, metadatas: dict) -> dict:
    if _should_fail(message, default_prob=0.10):
        raise RuntimeError("random failure (retry_single)")
    return _enrich_single(message, "retry_single")


# # ============================================================
# # D) kafka_handler + rate_limit (single + bulk)
# # ============================================================

@kafka_handler(
    name="rl_single",
    topics_in=["poc.rl.single.in"],
    topics_out=["poc.rl.single.out"],
    max_workers=100,
    bulk_mode=False,
    metadatas={"worker": "rl_single"},
)
@rate_limit(rps=5000, burst=5000)  # interpret as "dispatches per second" in your current runtime
def rl_single(message: dict, consumer_name: str, metadatas: dict) -> dict:
    return _enrich_single(message, "rl_single")


@kafka_handler(
    name="rl_bulk",
    topics_in=["poc.rl.bulk.in"],
    topics_out=["poc.rl.bulk.out"],
    max_workers=100,
    bulk_mode=True,
    batch_size=25,
    batch_timeout_ms=1000,
    metadatas={"worker": "rl_bulk", "mode": "bulk"},
)
@rate_limit(rps=5000, burst=5000)  # with current runtime, this is ~10 BATCHES/sec, not messages/sec
def rl_bulk(messages: list[dict], consumer_name: str, metadatas: dict):
    return _enrich_batch(messages, "rl_bulk")
