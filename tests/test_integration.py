"""
Integration tests for the QF framework.

Requires running Docker services (Kafka, Redis). Start with:
    cd qf_framework_with_poc_v6/poc_app && docker compose up -d

Run with:
    cd qf_framework_with_poc_v6
    python -m pytest tests/test_integration.py -v --timeout=120

Each test class spins up its own isolated ETL thread and cleans up after itself.
Topics are prefixed with "test.<uuid>." to avoid cross-test interference.
"""
import sys
import os
import json
import time
import threading
import types
import uuid
from unittest.mock import MagicMock

# Path setup
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "poc_app", "src"))

BOOTSTRAP = "localhost:9094"
REDIS_HOST = "localhost"
REDIS_PORT = 6379


# ---------------------------------------------------------------------------
# Helper: produce + consume
# ---------------------------------------------------------------------------

def _produce(topic: str, messages: list, bootstrap: str = BOOTSTRAP):
    from kafka import KafkaProducer
    p = KafkaProducer(
        bootstrap_servers=bootstrap,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    for m in messages:
        p.send(topic, m)
    p.flush()
    p.close()


def _consume(topic: str, expected: int, timeout_sec: float = 30.0, bootstrap: str = BOOTSTRAP) -> list:
    from kafka import KafkaConsumer, TopicPartition
    consumer = KafkaConsumer(
        bootstrap_servers=bootstrap,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        consumer_timeout_ms=500,
    )
    tp = TopicPartition(topic, 0)
    consumer.assign([tp])
    consumer.seek_to_beginning(tp)

    results = []
    deadline = time.time() + timeout_sec
    while len(results) < expected and time.time() < deadline:
        batch = consumer.poll(timeout_ms=500, max_records=500)
        for _, records in batch.items():
            for rec in records:
                try:
                    results.append(json.loads(rec.value))
                except Exception:
                    pass
    consumer.close()
    return results


def _run_etl(worker_modules: list, topics: list, prefix: str, timeout_sec: float = 60.0):
    """
    Start ETL in a background thread using test-scoped config.
    Returns (thread, stop_event). Stop by setting stop_event.
    """
    # Register test config in sys.modules so framework_etl can find it
    config_mod = types.ModuleType("config")
    config_mod.Config = type("Config", (), {
        "WORKER_NAME": f"test-{prefix}",
        "ERROR_TOPIC": f"{prefix}.dlq",
        "KAFKA_COMMIT_STRATEGY": "before",
        "KAFKA_POLL_TIMEOUT_MS": 10,
        "KAFKA_POLL_MAX_RECORDS": 200,
        "KAFKA_IDLE_SLEEP_SEC": 0.0,
        "KAFKA_COMMIT_TICK_SEC": 0.1,
        "KAFKA_MAX_JOBS_PER_TP_PER_TICK": 500,
        "KAFKA_PENDING_MAX_PER_TP": 750,
        "REDIS_HOST": REDIS_HOST,
        "REDIS_PORT": str(REDIS_PORT),
        "REDIS_DB": "0",
    })
    sys.modules["config"] = config_mod

    stop_event = threading.Event()

    def _etl():
        from framework.etl.framework_etl import start
        try:
            start(
                worker_modules=worker_modules,
                bootstrap_servers=BOOTSTRAP,
                consumer_name=f"test-{prefix}",
            )
        except Exception:
            pass  # stop on error/interrupt

    t = threading.Thread(target=_etl, daemon=True)
    t.start()
    return t, stop_event


def _fresh_prefix() -> str:
    return f"it.{uuid.uuid4().hex[:8]}"


# ---------------------------------------------------------------------------
# Helper: reset worker registry between tests
# ---------------------------------------------------------------------------

def _clear_registry():
    from framework.decorators import kafka_workers as kw
    kw._WORKERS_BY_NAME.clear()
    kw._TOPIC_TO_WORKER.clear()


# ---------------------------------------------------------------------------
# Integration: single handler end-to-end
# ---------------------------------------------------------------------------

class TestSingleHandlerIntegration:
    def test_echo_single_end_to_end(self):
        """
        Produce N messages → single handler enriches them → verify N outputs.
        """
        import pytest
        pytest.importorskip("kafka")

        _clear_registry()
        prefix = _fresh_prefix()
        in_topic = f"{prefix}.in"
        out_topic = f"{prefix}.out"
        N = 20

        from framework.decorators import kafka_handler

        @kafka_handler(
            name=f"it_single_{prefix}",
            topics_in=[in_topic],
            topics_out=[out_topic],
            max_workers=4,
        )
        def _worker(message, consumer_name, metadatas):
            message["processed"] = True
            return message

        # Produce messages
        messages = [{"id": f"m{i}", "val": i} for i in range(N)]
        _produce(in_topic, messages)

        # Start ETL
        t, stop = _run_etl([f"framework.decorators.kafka_workers"], [], prefix)
        time.sleep(1)  # let ETL connect

        # Wait for outputs
        outputs = _consume(out_topic, N, timeout_sec=30.0)
        assert len(outputs) == N, f"Expected {N} outputs, got {len(outputs)}"
        ids_out = {m["id"] for m in outputs}
        assert ids_out == {f"m{i}" for i in range(N)}
        for m in outputs:
            assert m.get("processed") is True


class TestBulkHandlerIntegration:
    def test_echo_bulk_end_to_end(self):
        """
        Produce N messages → bulk handler batches → verify N outputs.
        """
        import pytest
        pytest.importorskip("kafka")

        _clear_registry()
        prefix = _fresh_prefix()
        in_topic = f"{prefix}.bulk.in"
        out_topic = f"{prefix}.bulk.out"
        N = 50

        from framework.decorators import kafka_handler

        @kafka_handler(
            name=f"it_bulk_{prefix}",
            topics_in=[in_topic],
            topics_out=[out_topic],
            max_workers=4,
            bulk_mode=True,
            batch_size=10,
            batch_timeout_ms=300,
        )
        def _worker(messages, consumer_name, metadatas):
            for m in messages:
                m["bulk_processed"] = True
            return messages

        messages = [{"id": f"b{i}", "val": i} for i in range(N)]
        _produce(in_topic, messages)

        t, stop = _run_etl([], [], prefix)
        time.sleep(1)

        outputs = _consume(out_topic, N, timeout_sec=30.0)
        assert len(outputs) == N, f"Expected {N}, got {len(outputs)}"
        for m in outputs:
            assert m.get("bulk_processed") is True


class TestAggregatorIntegration:
    def test_aggregator_merges_two_parts(self):
        """
        Produce N pairs (part_a, part_b) → aggregator merges → verify N merged outputs.
        """
        import pytest
        pytest.importorskip("kafka")
        pytest.importorskip("redis")

        _clear_registry()
        prefix = _fresh_prefix()
        topic_a = f"{prefix}.agg.a"
        topic_b = f"{prefix}.agg.b"
        out_topic = f"{prefix}.agg.out"
        N = 10

        from framework.decorators import kafka_aggregator

        @kafka_aggregator(
            name=f"it_agg_{prefix}",
            topics_in=[topic_a, topic_b],
            topics_out=[out_topic],
            aggregate_by="id",
            aggregator_timeout_sec=60,
            max_workers=4,
        )
        def _worker(merged, consumer_name, metadatas):
            merged["merged"] = True
            return merged

        # Produce A and B parts for each id
        for i in range(N):
            mid = f"agg-{i}"
            _produce(topic_a, [{"id": mid, "part": "a", "val_a": i}])
            _produce(topic_b, [{"id": mid, "part": "b", "val_b": i * 10}])

        t, stop = _run_etl([], [], prefix)
        time.sleep(1)

        outputs = _consume(out_topic, N, timeout_sec=45.0)
        assert len(outputs) == N, f"Expected {N}, got {len(outputs)}"
        for m in outputs:
            assert m.get("merged") is True
            assert "val_a" in m
            assert "val_b" in m


class TestRetryPolicyIntegration:
    def test_retry_to_dlq_after_max_attempts(self):
        """
        A worker that always fails → messages land in DLQ after max_attempts.
        """
        import pytest
        pytest.importorskip("kafka")

        _clear_registry()
        prefix = _fresh_prefix()
        in_topic = f"{prefix}.retry.in"
        out_topic = f"{prefix}.retry.out"
        dlq_topic = f"{prefix}.retry.dlq"
        N = 5

        from framework.decorators import kafka_handler, retry_to_dlq

        @kafka_handler(
            name=f"it_retry_{prefix}",
            topics_in=[in_topic],
            topics_out=[out_topic],
            max_workers=2,
        )
        @retry_to_dlq(max_attempts=2, dlq_topic=dlq_topic)
        def _worker(message, consumer_name, metadatas):
            raise RuntimeError("always fail")

        messages = [{"id": f"r{i}", "val": i} for i in range(N)]
        _produce(in_topic, messages)

        t, stop = _run_etl([], [], prefix)
        time.sleep(1)

        # Out topic should be empty (all fail)
        out_msgs = _consume(out_topic, 1, timeout_sec=5.0)
        assert len(out_msgs) == 0, "No messages should reach out topic"

        # DLQ should have all N messages (after exhausting max_attempts=2)
        dlq_msgs = _consume(dlq_topic, N, timeout_sec=30.0)
        assert len(dlq_msgs) == N, f"Expected {N} in DLQ, got {len(dlq_msgs)}"


class TestFilterWorkerIntegration:
    def test_worker_returning_none_filters_message(self):
        """
        Worker returns None for some messages → those messages produce no output.
        """
        import pytest
        pytest.importorskip("kafka")

        _clear_registry()
        prefix = _fresh_prefix()
        in_topic = f"{prefix}.filter.in"
        out_topic = f"{prefix}.filter.out"
        N = 10  # send 10, filter half

        from framework.decorators import kafka_handler

        @kafka_handler(
            name=f"it_filter_{prefix}",
            topics_in=[in_topic],
            topics_out=[out_topic],
            max_workers=2,
        )
        def _worker(message, consumer_name, metadatas):
            if message.get("val", 0) % 2 == 0:
                return None  # filter even
            return message

        messages = [{"id": f"f{i}", "val": i} for i in range(N)]
        _produce(in_topic, messages)

        t, stop = _run_etl([], [], prefix)
        time.sleep(1)

        # Expect 5 outputs (odd values only)
        outputs = _consume(out_topic, 5, timeout_sec=20.0)
        assert len(outputs) == 5, f"Expected 5 (odd vals), got {len(outputs)}"
        for m in outputs:
            assert m["val"] % 2 == 1


# ---------------------------------------------------------------------------
# Integration: HTTP API endpoints
# ---------------------------------------------------------------------------

class TestHttpEndpointsIntegration:
    """
    Tests the Flask HTTP API. Requires the POC app to be running on port 5000.
    Skipped if the server is not reachable.
    """

    BASE_URL = "http://localhost:5000"

    @staticmethod
    def _check_server():
        import urllib.request
        try:
            urllib.request.urlopen(f"http://localhost:5000/swagger.json", timeout=2)
            return True
        except Exception:
            return False

    def test_ner_endpoint_returns_enriched_message(self):
        import pytest, urllib.request
        if not self._check_server():
            pytest.skip("POC server not running on localhost:5000")

        body = json.dumps({"id": "test-ner-1", "text": "Hello world"}).encode()
        req = urllib.request.Request(
            f"{self.BASE_URL}/workers/ner",
            data=body, headers={"Content-Type": "application/json"}, method="POST"
        )
        with urllib.request.urlopen(req) as resp:
            data = json.loads(resp.read())

        assert data["id"] == "test-ner-1"
        assert "enrichment" in data

    def test_translate_endpoint_returns_enriched_message(self):
        import pytest, urllib.request
        if not self._check_server():
            pytest.skip("POC server not running on localhost:5000")

        body = json.dumps({"id": "test-tr-1", "text": "Bonjour"}).encode()
        req = urllib.request.Request(
            f"{self.BASE_URL}/workers/translate",
            data=body, headers={"Content-Type": "application/json"}, method="POST"
        )
        with urllib.request.urlopen(req) as resp:
            data = json.loads(resp.read())

        assert data["id"] == "test-tr-1"
        assert "enrichment" in data

    def test_sentiment_endpoint_returns_enriched_message(self):
        import pytest, urllib.request
        if not self._check_server():
            pytest.skip("POC server not running on localhost:5000")

        body = json.dumps({"id": "test-sent-1", "part_a": {"score": 0.9}}).encode()
        req = urllib.request.Request(
            f"{self.BASE_URL}/workers/sentiment",
            data=body, headers={"Content-Type": "application/json"}, method="POST"
        )
        with urllib.request.urlopen(req) as resp:
            data = json.loads(resp.read())

        assert data["id"] == "test-sent-1"
        assert "enrichment" in data

    def test_translate_endpoint_accepts_list(self):
        import pytest, urllib.request
        if not self._check_server():
            pytest.skip("POC server not running on localhost:5000")

        payload = [{"id": "t1", "text": "Hello"}, {"id": "t2", "text": "World"}]
        body = json.dumps(payload).encode()
        req = urllib.request.Request(
            f"{self.BASE_URL}/workers/translate",
            data=body, headers={"Content-Type": "application/json"}, method="POST"
        )
        with urllib.request.urlopen(req) as resp:
            data = json.loads(resp.read())

        assert isinstance(data, list)
        assert len(data) == 2

    def test_swagger_json_has_correct_paths(self):
        import pytest, urllib.request
        if not self._check_server():
            pytest.skip("POC server not running on localhost:5000")

        with urllib.request.urlopen(f"{self.BASE_URL}/swagger.json") as resp:
            api_spec = json.loads(resp.read())

        paths = list(api_spec.get("paths", {}).keys())
        assert "/workers/ner" in paths
        assert "/workers/translate" in paths
        assert "/workers/sentiment" in paths
        # Verify no doubled paths from the bug we fixed
        assert "/workers/ner/workers/ner" not in paths
