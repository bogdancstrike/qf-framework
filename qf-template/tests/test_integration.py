"""
Integration tests — require live Docker services.

Start services before running:
    docker compose up -d
    # wait for healthchecks to pass (~30s)

Run with:
    pytest tests/test_integration.py -v -m integration --timeout=60
"""
import json
import time
import pytest
from tests.conftest import BOOTSTRAP, REDIS_HOST, REDIS_PORT, DB_URL


# ---------------------------------------------------------------------------
# Redis
# ---------------------------------------------------------------------------

@pytest.mark.integration
class TestRedisIntegration:
    """Live Redis round-trip tests via get_redis()."""

    def setup_method(self):
        import instances.instances as inst
        inst._redis = None  # reset singleton so each test gets a fresh connection

    def test_ping(self):
        from instances import get_redis
        r = get_redis()
        assert r.redis.ping() is True

    def test_set_and_get(self):
        from instances import get_redis
        r = get_redis()
        key = "qft:test:set_get"
        r.set_key(key, json.dumps({"hello": "world"}), expire=10)
        val = json.loads(r.get_key(key))
        assert val == {"hello": "world"}
        r.delete_key(key)

    def test_increment(self):
        from instances import get_redis
        r = get_redis()
        key = "qft:test:counter"
        r.delete_key(key)
        assert r.increment_key(key) == 1
        assert r.increment_key(key) == 2
        r.delete_key(key)

    def test_key_expiry(self):
        from instances import get_redis
        r = get_redis()
        key = "qft:test:expiry"
        r.set_key(key, "temp", expire=1)
        assert r.get_key(key) is not None
        time.sleep(1.2)
        assert r.get_key(key) is None


# ---------------------------------------------------------------------------
# Kafka
# ---------------------------------------------------------------------------

@pytest.mark.integration
class TestKafkaIntegration:
    """Live Kafka produce/consume tests via get_kafka()."""

    def setup_method(self):
        import instances.instances as inst
        inst._kafka = None

    def test_list_topics(self):
        from instances import get_kafka
        k = get_kafka()
        topics = k.admin_client.list_topics()
        assert isinstance(topics, (list, set, dict))

    def test_produce_and_consume(self):
        from instances import get_kafka
        k = get_kafka()
        topic = "qft.integration.test"
        msg = json.dumps({"id": "inttest-1", "ts": time.time()})
        k.create_topic(topic)
        k.put_message(topic, msg)
        # Give the broker a moment then consume
        time.sleep(0.5)
        raw = k.consume_message(topic)
        assert raw is not None
        parsed = json.loads(raw)
        assert "id" in parsed


# ---------------------------------------------------------------------------
# Postgres
# ---------------------------------------------------------------------------

@pytest.mark.integration
class TestPostgresIntegration:
    """Live Postgres tests via get_engine() and get_db()."""

    def setup_method(self):
        import instances.instances as inst
        inst._engine = None
        inst._SessionLocal = None

    def test_select_one(self):
        from sqlalchemy import text
        from instances import get_engine
        with get_engine().connect() as conn:
            result = conn.execute(text("SELECT 1 AS val"))
            row = result.fetchone()
            assert row[0] == 1

    def test_create_table_and_insert(self):
        from instances import get_engine, get_db
        from models.base import Base, ExampleRecord
        engine = get_engine()
        Base.metadata.create_all(engine)
        db = next(get_db())
        try:
            rec = ExampleRecord(name="integration-test", payload='{"ok": true}')
            db.add(rec)
            db.commit()
            db.refresh(rec)
            assert rec.id is not None
            assert rec.name == "integration-test"
            # clean up
            db.delete(rec)
            db.commit()
        finally:
            db.close()


# ---------------------------------------------------------------------------
# Health service — full stack
# ---------------------------------------------------------------------------

@pytest.mark.integration
class TestHealthServiceIntegration:
    """check_all() against live services."""

    def setup_method(self):
        import instances.instances as inst
        inst._redis = None
        inst._kafka = None
        inst._engine = None
        inst._SessionLocal = None

    def test_all_ok_when_services_running(self):
        from service.health_service import check_all
        result = check_all()
        # All services should be healthy when docker compose is up
        assert result["status"] == "ok", f"Unexpected degraded state: {result}"
        assert result["redis"]["status"] == "ok"
        assert result["kafka"]["status"] == "ok"
        assert result["postgres"]["status"] == "ok"

    def test_kafka_reports_topic_count(self):
        from service.health_service import check_all
        result = check_all()
        assert "topics" in result["kafka"]
        assert result["kafka"]["topics"] >= 0
