"""
Kafka service layer — one-off produce/consume for HTTP request handlers.

This module provides a thin wrapper around KafkaClient for use from HTTP
endpoints.  It is distinct from the ETL layer (framework_etl.py) which
runs its own persistent KafkaConsumer in a background thread.

Why a separate wrapper?
  - ETL consumers are long-lived; HTTP handlers need one-off produce/consume.
  - Endpoint code stays focused on request/response; broker details live here.
  - OTel spans and log lines are consistent across all service modules.

Connection lifecycle
--------------------
  KafkaClient is a singleton: the first call to _get_kafka() creates the
  connection; subsequent calls reuse it.  If Kafka is unavailable the
  constructor raises NoBrokersAvailable — callers should handle this and
  return a degraded response rather than a 500.

Topic conventions (PoC)
-----------------------
  poc.echo    — publish/consume demo messages
  poc.dlq     — dead-letter queue (written by ETL on error)

Tracing
-------
  Each operation opens a child span ("kafka.publish" / "kafka.consume") so
  the HTTP → Kafka path is visible as a single trace in Jaeger when
  ENABLE_TRACING=true.
"""

import json
import time
from typing import Optional

from framework.commons.logger import logger
from framework.streams.kafka_client import KafkaClient
from framework.tracing import get_tracer

# ---------------------------------------------------------------------------
# Kafka client (lazy singleton)
# ---------------------------------------------------------------------------

_kafka_client: Optional[KafkaClient] = None


def _get_kafka() -> KafkaClient:
    """Return the shared KafkaClient instance, creating it on first call.

    Late import of Config avoids circular imports at module load time.
    Uses security_protocol='NONE' for the local docker-compose Kafka
    (no TLS/SASL required in dev).
    """
    global _kafka_client
    if _kafka_client is None:
        from config import Config  # type: ignore

        _kafka_client = KafkaClient.get_instance(
            security_protocol="NONE",
            bootstrap_servers=Config.KAFKA_BOOTSTRAP_SERVERS,
            auto_offset_reset="earliest",
            group_id="poc-http-consumer",
        )
    return _kafka_client


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def publish(topic: str, message: dict, key: Optional[str] = None) -> bool:
    """Produce a single message to *topic*.

    The message dict is JSON-serialised before sending.  Returns True on
    success, False if the broker is unavailable or the send fails.

    Args:
        topic:   Kafka topic name (e.g. "poc.echo").
        message: Dict to publish; will be JSON-encoded.
        key:     Optional message key for partition routing.

    Returns:
        True on success, False on any error.
    """
    tracer = get_tracer()
    with tracer.start_as_current_span("kafka.publish") as span:
        span.set_attribute("kafka.topic", topic)
        span.set_attribute("kafka.key", str(key or ""))
        t0 = time.perf_counter()
        try:
            client = _get_kafka()
            payload = json.dumps(message)
            client.put_message(topic, payload, key=key)
            elapsed_ms = (time.perf_counter() - t0) * 1000
            span.set_attribute("elapsed_ms", round(elapsed_ms, 1))
            logger.debug(
                f"[kafka_service] PUBLISHED topic={topic} key={key} "
                f"elapsed={elapsed_ms:.1f}ms"
            )
            return True
        except Exception as exc:
            elapsed_ms = (time.perf_counter() - t0) * 1000
            logger.error(
                f"[kafka_service] PUBLISH ERROR topic={topic} "
                f"elapsed={elapsed_ms:.1f}ms error={exc}"
            )
            span.record_exception(exc)
            return False


def consume_last(topic: str, group_id: str = "poc-http-consumer") -> Optional[dict]:
    """Consume the most recent message from *topic*.

    Seeks to the end of each partition and reads one message back.
    Useful for demo endpoints that show the last published payload.
    Returns None if the topic is empty, unavailable, or an error occurs.

    Args:
        topic:    Kafka topic to read from.
        group_id: Consumer group; use a dedicated group so HTTP reads
                  do not interfere with the ETL consumer group offset.

    Returns:
        Parsed dict from the last message, or None.
    """
    tracer = get_tracer()
    with tracer.start_as_current_span("kafka.consume") as span:
        span.set_attribute("kafka.topic", topic)
        span.set_attribute("kafka.group_id", group_id)
        t0 = time.perf_counter()
        try:
            client = _get_kafka()
            raw = client.consume_message(topic, group_id=group_id)
            elapsed_ms = (time.perf_counter() - t0) * 1000
            if raw is None:
                logger.debug(
                    f"[kafka_service] CONSUME EMPTY topic={topic} "
                    f"elapsed={elapsed_ms:.1f}ms"
                )
                span.set_attribute("kafka.empty", True)
                return None
            result = json.loads(raw)
            span.set_attribute("elapsed_ms", round(elapsed_ms, 1))
            logger.debug(
                f"[kafka_service] CONSUMED topic={topic} "
                f"elapsed={elapsed_ms:.1f}ms"
            )
            return result
        except Exception as exc:
            elapsed_ms = (time.perf_counter() - t0) * 1000
            logger.error(
                f"[kafka_service] CONSUME ERROR topic={topic} "
                f"elapsed={elapsed_ms:.1f}ms error={exc}"
            )
            span.record_exception(exc)
            return None
