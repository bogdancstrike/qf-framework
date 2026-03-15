"""
HTTP endpoint handlers for the PoC application.

Routing lifecycle
-----------------
  1. Flask-RESTX receives the HTTP request and dispatches to the function
     registered for the matching (method, path) in endpoint.json.
  2. The function here extracts the JSON body via _json().
  3. It delegates to service/api_handler.py (cached_enrich) or
     service/kafka_service.py — never calling workers directly.
     This keeps HTTP plumbing separate from business logic.
  4. The service layer handles Redis caching, OTel spans, and logging;
     workers handle actual enrichment; this file stays thin.

Endpoint map (see maps/endpoint.json for canonical source):
  POST /workers/ner          — echo_single worker, cached
  POST /workers/translate    — rl_single worker, cached (list-aware)
  POST /workers/sentiment    — agg_basic_after_merge worker, cached
  GET  /workers/health       — Redis liveness probe
  GET  /workers/stats        — per-endpoint call counters from Redis
  POST /workers/echo         — echo payload back (no worker, no cache)
  POST /workers/publish      — publish payload to poc.echo Kafka topic
  GET  /workers/consume      — read last message from poc.echo Kafka topic
"""

from flask import jsonify, request as flask_request

from service.api_handler import cached_enrich, get_stats, health_check
from service.kafka_service import publish, consume_last
from workers.workers import echo_single, rl_single, agg_basic_after_merge


def _json():
    """Extract the JSON body; raise 400 if the body is missing or malformed."""
    return flask_request.get_json(force=True, silent=False)


# ---------------------------------------------------------------------------
# Worker endpoints — delegate to cached_enrich so every call gets
# Redis caching, OTel tracing, and stats counting automatically.
# ---------------------------------------------------------------------------

def worker_ner(app, operation, request, **kwargs):
    """POST /workers/ner — echo_single worker with Redis result cache."""
    payload = _json()
    out = cached_enrich(echo_single, payload, endpoint_name="ner")
    return jsonify(out)


def worker_translate(app, operation, request, **kwargs):
    """POST /workers/translate — rl_single worker; handles list payloads."""
    payload = _json()
    if isinstance(payload, list):
        # Batch mode: each item is cached independently so partial cache
        # hits are possible when only some items were previously processed.
        out = [
            cached_enrich(rl_single, item, endpoint_name="translate")
            for item in payload
        ]
        return jsonify(out)
    out = cached_enrich(rl_single, payload, endpoint_name="translate")
    return jsonify(out)


def worker_sentiment(app, operation, request, **kwargs):
    """POST /workers/sentiment — agg_basic_after_merge worker with cache."""
    payload = _json()
    out = cached_enrich(agg_basic_after_merge, payload, endpoint_name="sentiment")
    return jsonify(out)


# ---------------------------------------------------------------------------
# Utility endpoints — health, stats, echo
# ---------------------------------------------------------------------------

def worker_health(app, operation, request, **kwargs):
    """GET /workers/health — Redis liveness probe for k8s readiness checks."""
    return jsonify(health_check())


def worker_stats(app, operation, request, **kwargs):
    """GET /workers/stats — per-endpoint call counters stored in Redis."""
    return jsonify(get_stats())


def worker_echo(app, operation, request, **kwargs):
    """POST /workers/echo — return the request payload unchanged.

    Useful for testing the HTTP stack end-to-end without touching workers
    or Redis.  Not cached (no meaningful speedup for a trivial echo).
    """
    payload = _json()
    return jsonify({"echo": payload})


# ---------------------------------------------------------------------------
# Kafka demo endpoints — show produce/consume from an HTTP handler
# ---------------------------------------------------------------------------

def worker_publish(app, operation, request, **kwargs):
    """POST /workers/publish — publish the request body to poc.echo topic.

    Demonstrates how an HTTP handler can produce a Kafka message using
    the kafka_service layer.  The OTel span "kafka.publish" is a child of
    the active HTTP span, so the full path is visible in Jaeger.
    """
    payload = _json()
    ok = publish("poc.echo", payload)
    status = "published" if ok else "error"
    return jsonify({"status": status, "topic": "poc.echo", "payload": payload})


def worker_consume(app, operation, request, **kwargs):
    """GET /workers/consume — read the last message from poc.echo topic.

    Demonstrates a one-off HTTP→Kafka consume.  The 'poc-http-consumer'
    group_id is separate from the ETL consumer group so reading here does
    not advance the ETL offset.
    """
    msg = consume_last("poc.echo")
    if msg is None:
        return jsonify({"status": "empty", "topic": "poc.echo", "message": None})
    return jsonify({"status": "ok", "topic": "poc.echo", "message": msg})
