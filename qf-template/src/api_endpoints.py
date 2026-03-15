"""
HTTP endpoint handlers.

Routing lifecycle
-----------------
  1. Flask-RESTX receives the request and dispatches to the function registered
     in maps/endpoint.json for the matching (method, url) pair.
  2. The handler here extracts the request body and delegates to the service
     layer (service/health_service.py, service/api_handler.py).
  3. All caching, tracing, and stats logic lives in the service layer so this
     file stays thin.

Adding an endpoint
------------------
  1. Add a handler function here (signature: fn(app, operation, request, **kwargs)).
  2. Add an entry in maps/endpoint.json pointing to this module and function.
  3. Restart — the endpoint appears in Swagger UI automatically.

Current endpoints (see maps/endpoint.json):
  GET  /app/health        — liveness/readiness probe (Redis + Kafka + Postgres)
  GET  /app/stats         — per-endpoint Redis call counters
  POST /workers/process   — example worker endpoint with Redis caching
"""

from flask import jsonify, request as flask_request

from service.health_service import check_all
from service.api_handler import cached_enrich, get_stats
from workers.workers import process_single


def _json() -> dict:
    return flask_request.get_json(force=True, silent=False)


# ---------------------------------------------------------------------------
# Health
# ---------------------------------------------------------------------------

def health(app, operation, request, **kwargs):
    """GET /health — check connectivity to Redis, Kafka, and Postgres.

    Returns 200 in both ok and degraded states so Kubernetes liveness probes
    don't restart the pod on dependency hiccups.  Use a separate /ready
    endpoint (not included here) if you want fail-closed readiness probes.
    """
    return jsonify(check_all())


# ---------------------------------------------------------------------------
# Stats
# ---------------------------------------------------------------------------

def stats(app, operation, request, **kwargs):
    """GET /stats — per-endpoint call counters stored in Redis."""
    return jsonify(get_stats())


# ---------------------------------------------------------------------------
# Worker endpoints
# ---------------------------------------------------------------------------

def worker_process(app, operation, request, **kwargs):
    """POST /workers/process — run process_single with Redis result caching.

    Example request body:
        {"id": "abc123", "text": "hello world"}

    The response is cached for 60 s keyed on (endpoint, payload).  Identical
    requests within the TTL are served from Redis without calling the worker.
    """
    payload = _json()
    return jsonify(cached_enrich(process_single, payload, endpoint_name="process"))
