"""
Template workers — replace with your domain logic.

Each function decorated with @kafka_handler or @kafka_aggregator is
automatically discovered by the ETL runtime when its module is listed
in FrameworkSettings.worker_modules.

Decorator layering guide
------------------------
  Kafka-runtime policies (intern.py) — go ABOVE @kafka_handler:
    @retry_to_dlq   — retry N times inside Kafka, then route to DLQ
    @circuit_breaker — pause partition on consecutive failures
    @rate_limit      — token-bucket throttle on dispatch rate

  Function-level call policies (common.py) — wrap individual helper calls:
    @call_retry          — retry on transient exception (tenacity)
    @call_circuit_breaker — trip open on consecutive failures (pybreaker)
    @call_rate_limit      — moving-window rate limit (limits)

Topic naming convention (adjust to your project):
  <app>.input       — raw incoming messages
  <app>.output      — enriched / processed messages
  <app>.part_a      — first part for aggregation
  <app>.part_b      — second part for aggregation
  <app>.merged      — fully merged / aggregated messages
  <app>.dlq         — dead-letter queue (unrecoverable failures)
"""

import time
from framework.decorators import (
    kafka_handler,
    kafka_aggregator,
    retry_to_dlq,
    call_retry,
    call_circuit_breaker,
)
from framework.commons.logger import logger


# ---------------------------------------------------------------------------
# Helper — simulated external enrichment call
#
# Decorate your real external service calls (HTTP, DB, ML model) here.
# call_retry     — handles transient 5xx / network errors
# call_circuit_breaker — stops hammering a down service
# ---------------------------------------------------------------------------

@call_retry(max_attempts=3, wait_fixed=0.05, exceptions=(RuntimeError,))
@call_circuit_breaker(fail_max=10, reset_timeout=30, name="enrich-breaker")
def _enrich(message: dict, tag: str) -> dict:
    """Simulate an enrichment step (replace with your logic).

    Args:
        message: Input message dict.
        tag:     Worker name for tagging the enrichment result.

    Returns:
        Message dict with an "enrichment" key added.
    """
    # TODO: replace with real enrichment (HTTP call, DB lookup, ML inference…)
    message.setdefault("enrichment", {})[tag] = {
        "ok": True,
        "ts": time.time(),
    }
    return message


# ---------------------------------------------------------------------------
# Single-message handler
#
# Receives one message at a time from qf-template.input.
# Enriches it and publishes to qf-template.output.
# Retries up to 3 times before routing to the DLQ.
# ---------------------------------------------------------------------------

@retry_to_dlq(max_attempts=3, dlq_topic="qf-template.dlq")
@kafka_handler(
    name="process_single",
    topics_in=["qf-template.input"],
    topics_out=["qf-template.output"],
    max_workers=8,
)
def process_single(message: dict, consumer_name: str, metadatas: dict) -> dict:
    """Process a single message from qf-template.input.

    This is the most common worker pattern.  Replace the _enrich() call
    with your actual business logic.

    Args:
        message:       The deserialized Kafka message payload (dict).
        consumer_name: Consumer group name (injected by the ETL runtime).
        metadatas:     Runtime metadata dict from the ETL (topic, partition, offset…).

    Returns:
        The processed dict published to qf-template.output.
    """
    logger.debug(f"[process_single] id={message.get('id')}")
    return _enrich(message, tag="process_single")


# ---------------------------------------------------------------------------
# Bulk handler
#
# Buffers up to 50 messages per partition (or 250 ms) before processing.
# More efficient than single-message for high-throughput topics.
# ---------------------------------------------------------------------------

@kafka_handler(
    name="process_bulk",
    topics_in=["qf-template.bulk.input"],
    topics_out=["qf-template.bulk.output"],
    max_workers=4,
    bulk_mode=True,
    batch_size=50,
    batch_timeout_ms=250,
)
def process_bulk(messages: list[dict], consumer_name: str, metadatas: dict) -> list[dict]:
    """Process a batch of messages from qf-template.bulk.input.

    Receives a list of up to batch_size messages.  Return a list of the
    same length (one output per input) or an empty list to drop all.

    Args:
        messages:      List of deserialized message dicts.
        consumer_name: Consumer group name.
        metadatas:     Runtime metadata dict.

    Returns:
        List of enriched dicts published to qf-template.bulk.output.
    """
    logger.debug(f"[process_bulk] batch_size={len(messages)}")
    return [_enrich(m, tag="process_bulk") for m in messages]


# ---------------------------------------------------------------------------
# Aggregator
#
# Waits for matching messages from two topics (keyed on "id"), merges them,
# and publishes the combined record to qf-template.merged.
# Aggregation state is stored in Redis with a 10-minute TTL.
# ---------------------------------------------------------------------------

@kafka_aggregator(
    name="merge_parts",
    topics_in=["qf-template.part_a", "qf-template.part_b"],
    topics_out=["qf-template.merged"],
    aggregate_by="id",
    aggregator_timeout_sec=600,
    max_workers=4,
)
def merge_parts(merged: dict, consumer_name: str, metadatas: dict) -> dict:
    """Merge messages from part_a and part_b that share the same "id".

    Called once both parts of a message pair have arrived within the timeout.
    The merged dict already contains all fields from both parts.

    Args:
        merged:        Combined dict containing all fields from both parts.
        consumer_name: Consumer group name.
        metadatas:     Runtime metadata dict.

    Returns:
        Final merged dict published to qf-template.merged.
    """
    logger.debug(f"[merge_parts] id={merged.get('id')}")
    merged["merged"] = True
    return merged
