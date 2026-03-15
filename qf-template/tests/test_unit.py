"""
Unit tests — no external services required.

All Redis, Kafka, and Postgres calls are mocked via unittest.mock.
Run with:
    pytest tests/test_unit.py -v -m unit
"""
import json
import sys
import types
import pytest
from unittest.mock import MagicMock, patch


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_redis(cached=None, ping_ok=True):
    r = MagicMock()
    r.get_key.return_value = json.dumps(cached) if cached else None
    r.increment_key.return_value = 1
    r.set_key.return_value = True
    r.list_all_keys.return_value = []
    if ping_ok:
        r.redis.ping.return_value = True
    else:
        r.redis.ping.side_effect = ConnectionError("ping failed")
    return r


def _make_kafka(topics=10):
    k = MagicMock()
    k.admin_client.list_topics.return_value = list(range(topics))
    return k


def _make_engine(ok=True):
    engine = MagicMock()
    conn = MagicMock()
    conn.__enter__ = MagicMock(return_value=conn)
    conn.__exit__ = MagicMock(return_value=False)
    if ok:
        conn.execute.return_value = MagicMock()
    else:
        conn.execute.side_effect = Exception("connection refused")
    engine.connect.return_value = conn
    return engine


def _inject_mock_config(**kwargs):
    """Inject a mock 'config' module into sys.modules so late imports work."""
    cfg = types.ModuleType("config")
    Config = type("Config", (), {
        "REDIS_HOST": "localhost",
        "REDIS_PORT": 6379,
        "REDIS_DB": 0,
        "REDIS_MAX_CONNECTIONS": 50,
        "REDIS_SOCKET_TIMEOUT": 5.0,
        "REDIS_CONNECT_TIMEOUT": 5.0,
        "REDIS_RETRY_ON_TIMEOUT": True,
        "KAFKA_BOOTSTRAP_SERVERS": "localhost:9094",
        "WORKER_NAME": "test-app",
        "DB_HOST": "localhost",
        "DB_PORT": 5432,
        "DB_NAME": "qf",
        "DB_USER": "qf",
        "DB_PASSWORD": "qf",
        "postgres_url": classmethod(lambda cls: "postgresql+psycopg2://qf:qf@localhost/qf"),
        **kwargs,
    })
    cfg.Config = Config
    sys.modules["config"] = cfg
    return cfg


# ---------------------------------------------------------------------------
# health_service — unit tests
# ---------------------------------------------------------------------------

@pytest.mark.unit
class TestHealthService:
    """health_service.check_all() with all dependencies mocked."""

    def _run(self, redis_mock=None, kafka_mock=None, engine_mock=None):
        import service.health_service as hs
        # Patch the late imports inside each check function
        with patch("instances.get_redis", return_value=redis_mock or _make_redis()):
            with patch("instances.get_kafka", return_value=kafka_mock or _make_kafka()):
                with patch("instances.get_engine", return_value=engine_mock or _make_engine()):
                    return hs.check_all()

    def test_all_ok(self):
        result = self._run()
        assert result["status"] == "ok"
        assert result["redis"]["status"] == "ok"
        assert result["kafka"]["status"] == "ok"
        assert result["postgres"]["status"] == "ok"

    def test_kafka_topic_count_in_response(self):
        result = self._run(kafka_mock=_make_kafka(topics=42))
        assert result["kafka"]["topics"] == 42

    def test_redis_down_gives_degraded(self):
        result = self._run(redis_mock=_make_redis(ping_ok=False))
        assert result["status"] == "degraded"
        assert result["redis"]["status"] == "error"
        assert "detail" in result["redis"]

    def test_kafka_down_gives_degraded(self):
        k = _make_kafka()
        k.admin_client.list_topics.side_effect = Exception("broker unavailable")
        result = self._run(kafka_mock=k)
        assert result["status"] == "degraded"
        assert result["kafka"]["status"] == "error"

    def test_postgres_down_gives_degraded(self):
        result = self._run(engine_mock=_make_engine(ok=False))
        assert result["status"] == "degraded"
        assert result["postgres"]["status"] == "error"

    def test_multiple_failures_all_reported(self):
        """All three checks run independently — one failure does not skip others."""
        k = _make_kafka()
        k.admin_client.list_topics.side_effect = Exception("kafka down")
        result = self._run(
            redis_mock=_make_redis(ping_ok=False),
            kafka_mock=k,
            engine_mock=_make_engine(ok=False),
        )
        assert result["status"] == "degraded"
        assert result["redis"]["status"] == "error"
        assert result["kafka"]["status"] == "error"
        assert result["postgres"]["status"] == "error"

    def test_ok_components_have_no_detail_key(self):
        k = _make_kafka()
        k.admin_client.list_topics.side_effect = Exception("broker down")
        result = self._run(kafka_mock=k)
        assert "detail" not in result["redis"]   # ok → no detail
        assert "detail" in result["kafka"]        # error → has detail


# ---------------------------------------------------------------------------
# health endpoint handler
# ---------------------------------------------------------------------------

@pytest.mark.unit
class TestHealthEndpoint:
    """api_endpoints.health() returns a Flask JSON response backed by check_all()."""

    def _app(self):
        from flask import Flask
        app = Flask(__name__)
        app.config["TESTING"] = True
        return app

    def test_returns_ok_payload(self):
        import api_endpoints
        payload = {
            "status": "ok",
            "redis": {"status": "ok"},
            "kafka": {"status": "ok", "topics": 5},
            "postgres": {"status": "ok"},
        }
        with self._app().test_request_context("/health"):
            with patch("api_endpoints.check_all", return_value=payload):
                resp = api_endpoints.health(self._app(), "health", None)
                data = json.loads(resp.get_data(as_text=True))
        assert data["status"] == "ok"

    def test_returns_degraded_payload(self):
        import api_endpoints
        payload = {
            "status": "degraded",
            "redis": {"status": "error", "detail": "timeout"},
            "kafka": {"status": "ok", "topics": 3},
            "postgres": {"status": "ok"},
        }
        with self._app().test_request_context("/health"):
            with patch("api_endpoints.check_all", return_value=payload):
                resp = api_endpoints.health(self._app(), "health", None)
                data = json.loads(resp.get_data(as_text=True))
        assert data["status"] == "degraded"
        assert data["redis"]["status"] == "error"


# ---------------------------------------------------------------------------
# api_handler — cached_enrich
# ---------------------------------------------------------------------------

@pytest.mark.unit
class TestCachedEnrich:

    def _run(self, worker_fn=None, payload=None, redis_mock=None, endpoint="ep"):
        import service.api_handler as ah
        if worker_fn is None:
            worker_fn = lambda p, **kw: {"result": "ok", "id": p.get("id")}
        # get_redis is a late import inside cached_enrich — patch it at the source
        with patch("instances.get_redis", return_value=redis_mock or _make_redis()):
            return ah.cached_enrich(worker_fn, payload or {"id": "x"}, endpoint)

    def test_cache_miss_calls_worker(self):
        assert self._run()["result"] == "ok"

    def test_cache_miss_stores_result(self):
        redis = _make_redis()
        self._run(redis_mock=redis)
        assert redis.set_key.called

    def test_cache_hit_skips_worker(self):
        worker = MagicMock(return_value={"result": "fresh"})
        result = self._run(worker_fn=worker, redis_mock=_make_redis(cached={"result": "cached"}))
        worker.assert_not_called()
        assert result["result"] == "cached"

    def test_worker_exception_propagates(self):
        import service.api_handler as ah
        def boom(p, **kw): raise ValueError("boom")
        with patch("instances.get_redis", return_value=_make_redis()):
            with pytest.raises(ValueError, match="boom"):
                ah.cached_enrich(boom, {"id": "1"}, "ep")

    def test_redis_read_error_is_nonfatal(self):
        redis = _make_redis()
        redis.get_key.side_effect = ConnectionError("down")
        assert self._run(redis_mock=redis)["result"] == "ok"

    def test_stats_counter_incremented(self):
        redis = _make_redis()
        self._run(redis_mock=redis)
        assert redis.increment_key.called

    def test_cache_key_differs_per_endpoint(self):
        import service.api_handler as ah
        import hashlib
        payload = {"id": "1"}
        raw_a = json.dumps({"ep": "a", "payload": payload}, sort_keys=True)
        raw_b = json.dumps({"ep": "b", "payload": payload}, sort_keys=True)
        key_a = ah.CACHE_KEY_PREFIX + hashlib.sha256(raw_a.encode()).hexdigest()[:16]
        key_b = ah.CACHE_KEY_PREFIX + hashlib.sha256(raw_b.encode()).hexdigest()[:16]
        assert key_a != key_b


# ---------------------------------------------------------------------------
# instances — lazy singleton tests
# ---------------------------------------------------------------------------

@pytest.mark.unit
class TestInstances:

    def setup_method(self):
        _inject_mock_config()
        # Reset singletons before each test
        import instances.instances as inst
        inst._redis = None
        inst._kafka = None
        inst._engine = None
        inst._SessionLocal = None

    def teardown_method(self):
        import instances.instances as inst
        inst._redis = None
        inst._kafka = None
        inst._engine = None
        inst._SessionLocal = None

    def test_get_redis_returns_redis_utils_instance(self):
        import instances.instances as inst
        mock_ru = MagicMock()
        with patch("instances.instances.RedisUtils", return_value=mock_ru):
            result = inst.get_redis()
        assert result is mock_ru

    def test_get_redis_is_singleton(self):
        import instances.instances as inst
        mock_ru = MagicMock()
        with patch("instances.instances.RedisUtils", return_value=mock_ru):
            r1 = inst.get_redis()
            r2 = inst.get_redis()
        assert r1 is r2

    def test_get_engine_creates_sqlalchemy_engine(self):
        import instances.instances as inst
        mock_engine = MagicMock()
        with patch("instances.instances.create_engine", return_value=mock_engine):
            result = inst.get_engine()
        assert result is mock_engine

    def test_get_engine_is_singleton(self):
        import instances.instances as inst
        mock_engine = MagicMock()
        with patch("instances.instances.create_engine", return_value=mock_engine):
            e1 = inst.get_engine()
            e2 = inst.get_engine()
        assert e1 is e2

    def test_get_db_yields_and_closes_session(self):
        import instances.instances as inst
        mock_session = MagicMock()
        mock_factory = MagicMock(return_value=mock_session)
        with patch("instances.instances._get_session_factory", return_value=mock_factory):
            gen = inst.get_db()
            db = next(gen)
            assert db is mock_session
            try:
                next(gen)
            except StopIteration:
                pass
        mock_session.close.assert_called_once()

    def test_get_kafka_is_singleton(self):
        import instances.instances as inst
        mock_client = MagicMock()
        with patch("instances.instances.KafkaClient") as MockKafka:
            MockKafka._instance = None
            MockKafka.get_instance.return_value = mock_client
            k1 = inst.get_kafka()
            k2 = inst.get_kafka()
        assert k1 is k2
