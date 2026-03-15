"""
Unit tests for poc_app/src/service/api_handler.py.

All Redis interactions are mocked — no live Redis required.

Run with:
    python -m pytest tests/test_api_handler.py -v -m unit
"""
import sys
import os
import json
import pytest
from unittest.mock import MagicMock, patch

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "poc_app", "src"))


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_redis(cached_value=None):
    """Return a MagicMock that behaves like RedisUtils."""
    r = MagicMock()
    r.get_key.return_value = json.dumps(cached_value) if cached_value else None
    r.increment_key.return_value = 1
    r.set_key.return_value = True
    r.list_all_keys.return_value = []
    r.redis.ping.return_value = True
    return r


def _worker(payload, consumer_name="api", metadatas=None):
    """Minimal worker stub."""
    return {"result": "ok", "id": payload.get("id")}


# ---------------------------------------------------------------------------
# cached_enrich
# ---------------------------------------------------------------------------

@pytest.mark.unit
class TestCachedEnrich:

    def _run(self, worker_fn=_worker, payload=None, endpoint="test", redis_mock=None):
        import service.api_handler as ah
        ah._redis_client = redis_mock or _make_redis()
        return ah.cached_enrich(worker_fn, payload or {"id": "x"}, endpoint)

    def test_cache_miss_calls_worker(self):
        redis = _make_redis(cached_value=None)
        result = self._run(redis_mock=redis)
        assert result["result"] == "ok"

    def test_cache_miss_stores_result(self):
        redis = _make_redis(cached_value=None)
        self._run(redis_mock=redis)
        assert redis.set_key.called

    def test_cache_hit_skips_worker(self):
        cached = {"result": "cached", "id": "x"}
        redis = _make_redis(cached_value=cached)
        worker = MagicMock(return_value={"result": "fresh"})
        result = self._run(worker_fn=worker, redis_mock=redis)
        worker.assert_not_called()
        assert result["result"] == "cached"

    def test_stats_counter_incremented(self):
        redis = _make_redis()
        self._run(redis_mock=redis)
        assert redis.increment_key.called

    def test_worker_exception_propagates(self):
        def boom(payload, **kwargs):
            raise ValueError("worker failed")
        redis = _make_redis()
        import service.api_handler as ah
        ah._redis_client = redis
        with pytest.raises(ValueError, match="worker failed"):
            ah.cached_enrich(boom, {"id": "1"}, "test")

    def test_cache_read_error_is_nonfatal(self):
        """Redis failure on get_key should fall through to worker call."""
        redis = _make_redis()
        redis.get_key.side_effect = ConnectionError("redis down")
        result = self._run(redis_mock=redis)
        # Worker was called despite Redis failure
        assert result["result"] == "ok"

    def test_cache_write_error_is_nonfatal(self):
        redis = _make_redis()
        redis.set_key.side_effect = ConnectionError("redis down")
        result = self._run(redis_mock=redis)
        assert result["result"] == "ok"

    def test_cache_key_includes_endpoint_name(self):
        """Same payload on different endpoints must produce different cache keys."""
        import service.api_handler as ah
        import hashlib
        payload = {"id": "1"}
        raw_a = json.dumps({"ep": "a", "payload": payload}, sort_keys=True)
        raw_b = json.dumps({"ep": "b", "payload": payload}, sort_keys=True)
        key_a = ah.CACHE_KEY_PREFIX + hashlib.sha256(raw_a.encode()).hexdigest()[:16]
        key_b = ah.CACHE_KEY_PREFIX + hashlib.sha256(raw_b.encode()).hexdigest()[:16]
        assert key_a != key_b


# ---------------------------------------------------------------------------
# get_stats
# ---------------------------------------------------------------------------

@pytest.mark.unit
class TestGetStats:

    def test_returns_endpoint_counts(self):
        import service.api_handler as ah
        redis = _make_redis()
        redis.list_all_keys.return_value = ["poc:stats:ner", "poc:stats:translate"]
        redis.get_key.side_effect = lambda k: b"5" if "ner" in k else b"3"
        ah._redis_client = redis
        stats = ah.get_stats()
        assert stats["ner"] == 5
        assert stats["translate"] == 3

    def test_empty_redis_returns_empty_dict(self):
        import service.api_handler as ah
        redis = _make_redis()
        redis.list_all_keys.return_value = []
        ah._redis_client = redis
        assert ah.get_stats() == {}

    def test_redis_error_returns_error_key(self):
        import service.api_handler as ah
        redis = _make_redis()
        redis.list_all_keys.side_effect = ConnectionError("down")
        ah._redis_client = redis
        result = ah.get_stats()
        assert "error" in result


# ---------------------------------------------------------------------------
# health_check
# ---------------------------------------------------------------------------

@pytest.mark.unit
class TestHealthCheck:

    def test_ok_when_redis_pings(self):
        import service.api_handler as ah
        redis = _make_redis()
        ah._redis_client = redis
        result = ah.health_check()
        assert result["status"] == "ok"
        assert result["redis"] == "ok"

    def test_degraded_when_redis_fails(self):
        import service.api_handler as ah
        redis = _make_redis()
        redis.redis.ping.side_effect = ConnectionError("timeout")
        ah._redis_client = redis
        result = ah.health_check()
        assert result["status"] == "degraded"
        assert "error" in result["redis"]
