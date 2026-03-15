from flask import jsonify, request as flask_request
from workers.workers import echo_single, rl_single, agg_basic_after_merge

def _json():
    return flask_request.get_json(force=True, silent=False)

def worker_ner(app, operation, request, **kwargs):
    payload = _json()
    out = echo_single(payload, consumer_name="api", metadatas={"via": "http"})
    return jsonify(out)

def worker_translate(app, operation, request, **kwargs):
    payload = _json()
    # For API, allow either single message or list; call rl_single per message
    if isinstance(payload, list):
        out = [rl_single(m, consumer_name="api", metadatas={"via": "http"}) for m in payload]
        return jsonify(out)
    out = rl_single(payload, consumer_name="api", metadatas={"via": "http"})
    return jsonify(out)

def worker_sentiment(app, operation, request, **kwargs):
    payload = _json()
    # Expects merged message (both parts already in payload)
    out = agg_basic_after_merge(payload, consumer_name="api", metadatas={"via": "http"})
    return jsonify(out)
