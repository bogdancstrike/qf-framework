"""
Unit tests for the QF framework core modules.

Run with:
    cd qf_framework_with_poc_v6
    python -m pytest tests/test_unit.py -v
"""
import sys
import os
import threading
import time
import json
import uuid
from unittest.mock import MagicMock, patch, call
from dataclasses import replace

# Make framework importable without installing
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "poc_app", "src"))


# ---------------------------------------------------------------------------
# Helpers / stubs
# ---------------------------------------------------------------------------

def _make_tp(topic: str = "t", partition: int = 0):
    from kafka import TopicPartition
    return TopicPartition(topic, partition)


# ---------------------------------------------------------------------------
# framework.decorators.kafka_workers
# ---------------------------------------------------------------------------

class TestWorkerRegistry:
    """Tests for @kafka_handler and @kafka_aggregator registry."""

    def setup_method(self):
        from framework.decorators import kafka_workers as kw
        kw._WORKERS_BY_NAME.clear()
        kw._TOPIC_TO_WORKER.clear()

    def test_register_handler(self):
        from framework.decorators import kafka_workers as kw
        from framework.decorators import kafka_handler

        @kafka_handler(
            name="test_h",
            topics_in=["in.a"],
            topics_out=["out.a"],
            max_workers=2,
        )
        def _fn(m, c, meta):
            return m

        spec = kw._WORKERS_BY_NAME["test_h"]
        assert spec.name == "test_h"
        assert spec.topics_in == ["in.a"]
        assert spec.topics_out == ["out.a"]
        assert spec.max_workers == 2
        assert spec.kind == "handler"
        assert spec.bulk_mode is False

    def test_register_bulk_handler(self):
        from framework.decorators import kafka_handler

        @kafka_handler(
            name="bulk_h",
            topics_in=["in.b"],
            topics_out=["out.b"],
            bulk_mode=True,
            batch_size=10,
            batch_timeout_ms=500,
        )
        def _fn(msgs, c, meta):
            return msgs

        from framework.decorators import kafka_workers as kw
        spec = kw._WORKERS_BY_NAME["bulk_h"]
        assert spec.bulk_mode is True
        assert spec.batch_size == 10
        assert spec.batch_timeout_ms == 500

    def test_register_aggregator(self):
        from framework.decorators import kafka_aggregator

        @kafka_aggregator(
            name="agg_h",
            topics_in=["in.x", "in.y"],
            topics_out=["out.xy"],
            aggregate_by="id",
            aggregator_timeout_sec=300,
        )
        def _fn(merged, c, meta):
            return merged

        from framework.decorators import kafka_workers as kw
        spec = kw._WORKERS_BY_NAME["agg_h"]
        assert spec.kind == "aggregator"
        assert spec.aggregate_by == "id"
        assert spec.aggregator_timeout_sec == 300
        assert kw._TOPIC_TO_WORKER["in.x"] == "agg_h"
        assert kw._TOPIC_TO_WORKER["in.y"] == "agg_h"

    def test_duplicate_worker_name_raises(self):
        from framework.decorators import kafka_handler
        import pytest

        @kafka_handler(name="dup", topics_in=["in.1"], topics_out=["out.1"])
        def _a(m, c, meta):
            return m

        with pytest.raises(RuntimeError, match="Duplicate worker name"):
            @kafka_handler(name="dup", topics_in=["in.2"], topics_out=["out.2"])
            def _b(m, c, meta):
                return m

    def test_duplicate_topic_raises(self):
        from framework.decorators import kafka_handler
        import pytest

        @kafka_handler(name="w1", topics_in=["shared.topic"], topics_out=["out.1"])
        def _a(m, c, meta):
            return m

        with pytest.raises(RuntimeError, match="already handled"):
            @kafka_handler(name="w2", topics_in=["shared.topic"], topics_out=["out.2"])
            def _b(m, c, meta):
                return m

    def test_aggregator_requires_two_topics(self):
        from framework.decorators import kafka_aggregator
        import pytest

        with pytest.raises(ValueError, match="2\\+ topics_in"):
            @kafka_aggregator(name="agg_bad", topics_in=["only.one"], topics_out=["out"])
            def _fn(m, c, meta):
                return m

    def test_worker_for_topic(self):
        from framework.decorators import kafka_handler
        from framework.decorators.kafka_workers import worker_for_topic

        @kafka_handler(name="lookup_w", topics_in=["lookup.in"], topics_out=["lookup.out"])
        def _fn(m, c, meta):
            return m

        spec = worker_for_topic("lookup.in")
        assert spec is not None
        assert spec.name == "lookup_w"
        assert worker_for_topic("nonexistent") is None

    def test_all_topics(self):
        from framework.decorators import kafka_handler
        from framework.decorators.kafka_workers import all_topics

        @kafka_handler(name="at_w", topics_in=["at.in"], topics_out=["at.out"])
        def _fn(m, c, meta):
            return m

        assert "at.in" in all_topics()

    def test_ensure_message_id_adds_id(self):
        from framework.decorators.kafka_workers import ensure_message_id
        msg = {"data": "x"}
        mid = ensure_message_id(msg)
        assert "id" in msg
        assert msg["id"] == mid
        assert len(mid) > 0

    def test_ensure_message_id_preserves_existing(self):
        from framework.decorators.kafka_workers import ensure_message_id
        msg = {"id": "existing-123", "data": "x"}
        ensure_message_id(msg)
        assert msg["id"] == "existing-123"

    def test_compute_aggregate_key_by_path(self):
        from framework.decorators import kafka_aggregator
        from framework.decorators.kafka_workers import compute_aggregate_key

        @kafka_aggregator(
            name="agg_key",
            topics_in=["ak.a", "ak.b"],
            topics_out=["ak.out"],
            aggregate_by="meta.session",
        )
        def _fn(m, c, meta):
            return m

        from framework.decorators.kafka_workers import _WORKERS_BY_NAME
        spec = _WORKERS_BY_NAME["agg_key"]

        msg = {"meta": {"session": "sess-42"}, "payload": "x"}
        key = compute_aggregate_key(spec, msg)
        assert key == "sess-42"

    def test_compute_aggregate_key_fallback_to_id(self):
        from framework.decorators import kafka_aggregator
        from framework.decorators.kafka_workers import compute_aggregate_key

        @kafka_aggregator(
            name="agg_key2",
            topics_in=["ak2.a", "ak2.b"],
            topics_out=["ak2.out"],
            aggregate_by="missing.field",
        )
        def _fn(m, c, meta):
            return m

        from framework.decorators.kafka_workers import _WORKERS_BY_NAME
        spec = _WORKERS_BY_NAME["agg_key2"]

        msg = {"id": "fallback-id", "data": "y"}
        key = compute_aggregate_key(spec, msg)
        assert key == "fallback-id"


# ---------------------------------------------------------------------------
# framework.decorators.policies
# ---------------------------------------------------------------------------

class TestPolicies:
    def setup_method(self):
        from framework.decorators import kafka_workers as kw
        kw._WORKERS_BY_NAME.clear()
        kw._TOPIC_TO_WORKER.clear()

    def test_retry_to_dlq_attaches_config(self):
        from framework.decorators import retry_to_dlq, kafka_handler

        @kafka_handler(name="rp_w", topics_in=["rp.in"], topics_out=["rp.out"])
        @retry_to_dlq(max_attempts=5, dlq_topic="rp.dlq")
        def _fn(m, c, meta):
            return m

        from framework.decorators.kafka_workers import _WORKERS_BY_NAME
        spec = _WORKERS_BY_NAME["rp_w"]
        assert spec.retry_to_dlq is not None
        assert spec.retry_to_dlq.max_attempts == 5
        assert spec.retry_to_dlq.dlq_topic == "rp.dlq"

    def test_rate_limit_attaches_config(self):
        from framework.decorators import rate_limit, kafka_handler

        @kafka_handler(name="rl_w", topics_in=["rl.in"], topics_out=["rl.out"])
        @rate_limit(rps=100.0, burst=50)
        def _fn(m, c, meta):
            return m

        from framework.decorators.kafka_workers import _WORKERS_BY_NAME
        spec = _WORKERS_BY_NAME["rl_w"]
        assert spec.rate_limit is not None
        assert spec.rate_limit.rps == 100.0
        assert spec.rate_limit.burst == 50

    def test_policy_above_decorator(self):
        """Policy above @kafka_handler should still attach correctly."""
        from framework.decorators import retry_to_dlq, kafka_handler

        @retry_to_dlq(max_attempts=3, dlq_topic="above.dlq")
        @kafka_handler(name="above_w", topics_in=["above.in"], topics_out=["above.out"])
        def _fn(m, c, meta):
            return m

        from framework.decorators.kafka_workers import _WORKERS_BY_NAME
        spec = _WORKERS_BY_NAME["above_w"]
        assert spec.retry_to_dlq is not None
        assert spec.retry_to_dlq.max_attempts == 3


# ---------------------------------------------------------------------------
# framework.etl - CommitCoordinator
# ---------------------------------------------------------------------------

class TestCommitCoordinator:
    def _make_coordinator(self, tick=0.0):
        """Create a CommitCoordinator with a mock consumer and zero tick for eager commits."""
        import sys, os
        sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "poc_app", "src"))
        # Provide a minimal Config stub so framework_etl can import
        import types
        config_mod = types.ModuleType("config")
        config_mod.Config = type("Config", (), {
            "WORKER_NAME": "test",
            "ERROR_TOPIC": "test.error",
            "REDIS_HOST": "localhost",
            "REDIS_PORT": "6379",
            "REDIS_DB": "0",
        })
        sys.modules.setdefault("config", config_mod)

        from framework.etl.framework_etl import CommitCoordinator
        mock_consumer = MagicMock()
        return CommitCoordinator(mock_consumer, commit_tick_sec=tick)

    def test_init_tp_sets_correct_start_offset(self):
        coord = self._make_coordinator(tick=0.0)
        tp = _make_tp("t", 0)

        coord.init_tp(tp, first_offset=10)
        assert coord.next_commit[tp] == 10

    def test_concurrent_jobs_all_committed(self):
        """With init_tp called first, all offsets (even if done out of order) should commit."""
        coord = self._make_coordinator(tick=0.0)
        tp = _make_tp("t", 0)
        N = 20
        coord.init_tp(tp, first_offset=0)

        # Mark done in reverse order (simulates jobs finishing out of order)
        for offset in reversed(range(N)):
            coord.mark_done(tp, offset)

        coord.try_commit(force=True)
        assert coord.next_commit[tp] == N

    def test_no_commit_without_progress(self):
        """try_commit should not call commit if no offsets advanced."""
        coord = self._make_coordinator(tick=0.0)
        tp = _make_tp("t", 0)
        coord.init_tp(tp, first_offset=5)
        # Don't mark any as done

        coord.try_commit(force=True)
        # Consumer.commit should not be called since nothing advanced
        coord.consumer.commit.assert_not_called()

    def test_partial_progress_commits_only_contiguous(self):
        coord = self._make_coordinator(tick=0.0)
        tp = _make_tp("t", 0)
        coord.init_tp(tp, first_offset=0)

        # Mark offsets 0,1,2 done but NOT 3; 4,5 done
        for off in [0, 1, 2, 4, 5]:
            coord.mark_done(tp, off)

        coord.try_commit(force=True)
        # Should advance to 3 (next after contiguous run 0,1,2)
        assert coord.next_commit[tp] == 3

    def test_commit_tick_batching(self):
        """try_commit should skip if called within tick window."""
        coord = self._make_coordinator(tick=10.0)  # 10s tick
        tp = _make_tp("t", 0)
        coord.init_tp(tp, first_offset=0)
        coord.mark_done(tp, 0)

        coord.try_commit()  # first call — fires
        coord.try_commit()  # second call within tick — should skip
        coord.try_commit()  # third call — should skip

        # Only 1 actual commit should have happened
        assert coord.consumer.commit.call_count <= 1

    def test_force_commit_bypasses_tick(self):
        coord = self._make_coordinator(tick=10.0)
        tp = _make_tp("t", 0)
        coord.init_tp(tp, first_offset=0)

        # Mark two distinct offsets done at different times
        coord.mark_done(tp, 0)
        coord.try_commit(force=True)  # commits offset 0 → advances to 1
        assert coord.consumer.commit.call_count == 1

        coord.mark_done(tp, 1)
        coord.try_commit(force=True)  # commits offset 1 → advances to 2
        assert coord.consumer.commit.call_count == 2


# ---------------------------------------------------------------------------
# framework.etl - _forward_result
# ---------------------------------------------------------------------------

class TestForwardResult:
    def _get_forward_result(self):
        import sys, os, types
        sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "poc_app", "src"))
        config_mod = types.ModuleType("config")
        config_mod.Config = type("Config", (), {
            "WORKER_NAME": "test",
            "ERROR_TOPIC": "test.error",
            "REDIS_HOST": "localhost",
            "REDIS_PORT": "6379",
            "REDIS_DB": "0",
        })
        sys.modules.setdefault("config", config_mod)
        from framework.etl.framework_etl import _forward_result
        return _forward_result

    def _make_spec(self, topics_out=None):
        from framework.decorators import kafka_workers as kw
        kw._WORKERS_BY_NAME.clear()
        kw._TOPIC_TO_WORKER.clear()
        from framework.decorators import kafka_handler

        @kafka_handler(name="fr_w", topics_in=["fr.in"], topics_out=topics_out or ["fr.out"])
        def _fn(m, c, meta):
            return m

        return kw._WORKERS_BY_NAME["fr_w"]

    def test_none_result_returns_true_no_send(self):
        """Worker returning None should not produce output."""
        _forward_result = self._get_forward_result()
        spec = self._make_spec()
        producer = MagicMock()

        ok = _forward_result(producer, spec, None, wait_for_acks=False, ack_timeout_sec=10.0)
        assert ok is True
        producer.send.assert_not_called()

    def test_dict_result_sends_to_all_topics(self):
        _forward_result = self._get_forward_result()
        spec = self._make_spec(topics_out=["out.1", "out.2"])
        mock_future = MagicMock()
        producer = MagicMock()
        producer.send.return_value = mock_future

        result = {"id": "x", "data": "y"}
        ok = _forward_result(producer, spec, result, wait_for_acks=False, ack_timeout_sec=10.0)

        assert ok is True
        assert producer.send.call_count == 2  # once per output topic

    def test_list_result_sends_each_item(self):
        _forward_result = self._get_forward_result()
        spec = self._make_spec(topics_out=["out.1"])
        mock_future = MagicMock()
        producer = MagicMock()
        producer.send.return_value = mock_future

        result = [{"id": f"m{i}"} for i in range(5)]
        ok = _forward_result(producer, spec, result, wait_for_acks=False, ack_timeout_sec=10.0)

        assert ok is True
        assert producer.send.call_count == 5

    def test_list_with_none_items_skips_nones(self):
        """None items in a list result should not be sent."""
        _forward_result = self._get_forward_result()
        spec = self._make_spec(topics_out=["out.1"])
        mock_future = MagicMock()
        producer = MagicMock()
        producer.send.return_value = mock_future

        result = [{"id": "a"}, None, {"id": "b"}, None, {"id": "c"}]
        ok = _forward_result(producer, spec, result, wait_for_acks=False, ack_timeout_sec=10.0)

        assert ok is True
        assert producer.send.call_count == 3


# ---------------------------------------------------------------------------
# framework.commons.utils
# ---------------------------------------------------------------------------

class TestUtils:
    def test_deep_merge_flat(self):
        from framework.commons.utils import deep_merge
        a = {"x": 1, "y": 2}
        b = {"y": 99, "z": 3}
        result = deep_merge(a, b)
        assert result == {"x": 1, "y": 99, "z": 3}

    def test_deep_merge_nested(self):
        from framework.commons.utils import deep_merge
        a = {"meta": {"a": 1, "b": 2}, "top": "x"}
        b = {"meta": {"b": 99, "c": 3}}
        result = deep_merge(a, b)
        assert result["meta"] == {"a": 1, "b": 99, "c": 3}
        assert result["top"] == "x"

    def test_deep_merge_aggregation_pattern(self):
        """Simulate aggregating two topic parts."""
        from framework.commons.utils import deep_merge
        part_a = {"id": "msg-1", "from_a": "data_a"}
        part_b = {"id": "msg-1", "from_b": "data_b"}
        merged = deep_merge({}, part_a)
        merged = deep_merge(merged, part_b)
        assert merged["from_a"] == "data_a"
        assert merged["from_b"] == "data_b"
        assert merged["id"] == "msg-1"

    def test_load_json(self, tmp_path):
        from framework.commons.utils import load_json
        f = tmp_path / "test.json"
        f.write_text('{"key": "value", "num": 42}')
        result = load_json(str(f))
        assert result == {"key": "value", "num": 42}

    def test_getor_returns_value(self):
        from framework.commons.utils import getor
        obj = {"key": "value"}
        assert getor(obj, "key") == "value"

    def test_getor_returns_default_on_missing(self):
        from framework.commons.utils import getor
        assert getor({"x": 1}, "missing", "default") == "default"

    def test_getor_returns_default_on_none_value(self):
        from framework.commons.utils import getor
        assert getor({"key": None}, "key", "fallback") == "fallback"

    def test_getor_returns_default_on_empty_obj(self):
        from framework.commons.utils import getor
        assert getor(None, "key", "d") == "d"


# ---------------------------------------------------------------------------
# framework.etl - rate limiter helpers
# ---------------------------------------------------------------------------

class TestRateLimiter:
    def _setup(self):
        import sys, os, types
        sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "poc_app", "src"))
        config_mod = types.ModuleType("config")
        config_mod.Config = type("Config", (), {"WORKER_NAME": "test", "ERROR_TOPIC": "e",
                                                 "REDIS_HOST": "localhost", "REDIS_PORT": "6379", "REDIS_DB": "0"})
        sys.modules.setdefault("config", config_mod)

    def test_rl_try_take_within_burst(self):
        self._setup()
        from framework.etl.framework_etl import WorkerState, RateLimitConfig, rl_try_take
        cfg = RateLimitConfig(rps=10.0, burst=5)
        state = WorkerState(name="test", lock=threading.Lock())
        # Take 5 tokens (burst) — all should succeed
        for _ in range(5):
            assert rl_try_take(state, cfg) is True

    def test_rl_try_take_exhausted(self):
        self._setup()
        from framework.etl.framework_etl import WorkerState, RateLimitConfig, rl_try_take
        cfg = RateLimitConfig(rps=10.0, burst=3)
        state = WorkerState(name="test", lock=threading.Lock())
        # Exhaust burst
        for _ in range(3):
            rl_try_take(state, cfg)
        # Next should fail
        assert rl_try_take(state, cfg) is False

    def test_rl_tokens_refill_over_time(self):
        self._setup()
        from framework.etl.framework_etl import WorkerState, RateLimitConfig, rl_try_take
        cfg = RateLimitConfig(rps=1000.0, burst=1)
        state = WorkerState(name="test", lock=threading.Lock())
        assert rl_try_take(state, cfg) is True   # use the one token
        assert rl_try_take(state, cfg) is False  # exhausted
        time.sleep(0.01)  # 10ms at 1000 rps = ~10 tokens refilled
        assert rl_try_take(state, cfg) is True   # should have refilled


# ---------------------------------------------------------------------------
# framework.etl - circuit breaker helpers
# ---------------------------------------------------------------------------

class TestCircuitBreaker:
    def _setup(self):
        import sys, os, types
        sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "poc_app", "src"))
        config_mod = types.ModuleType("config")
        config_mod.Config = type("Config", (), {"WORKER_NAME": "test", "ERROR_TOPIC": "e",
                                                 "REDIS_HOST": "localhost", "REDIS_PORT": "6379", "REDIS_DB": "0"})
        sys.modules.setdefault("config", config_mod)

    def test_cb_opens_after_threshold(self):
        self._setup()
        from framework.etl.framework_etl import WorkerState, CircuitBreakerConfig, cb_on_failure, cb_is_open
        cfg = CircuitBreakerConfig(failures=3, reset_sec=5)
        state = WorkerState(name="test", lock=threading.Lock())

        for _ in range(3):
            cb_on_failure(state, cfg)

        assert cb_is_open(state) is True

    def test_cb_resets_after_success(self):
        self._setup()
        from framework.etl.framework_etl import WorkerState, CircuitBreakerConfig, cb_on_failure, cb_on_success, cb_is_open
        cfg = CircuitBreakerConfig(failures=2, reset_sec=5)
        state = WorkerState(name="test", lock=threading.Lock())

        cb_on_failure(state, cfg)
        cb_on_success(state)  # reset consecutive failures
        cb_on_failure(state, cfg)  # only 1 failure now, shouldn't open

        assert cb_is_open(state) is False

    def test_cb_not_open_before_threshold(self):
        self._setup()
        from framework.etl.framework_etl import WorkerState, CircuitBreakerConfig, cb_on_failure, cb_is_open
        cfg = CircuitBreakerConfig(failures=5, reset_sec=5)
        state = WorkerState(name="test", lock=threading.Lock())

        for _ in range(4):
            cb_on_failure(state, cfg)

        assert cb_is_open(state) is False


# ---------------------------------------------------------------------------
# framework.api.dynamic - URL path generation fix
# ---------------------------------------------------------------------------

class TestDynamicEndpointUrl:
    """Tests for the api_url path stripping fix in dynamic.py."""

    def test_namespace_prefix_stripped(self):
        """generate_endpoints_from_config must not double the namespace prefix."""
        import sys, os
        sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

        from framework.api.dynamic import generate_endpoints_from_config
        from flask import Flask
        from flask_restx import Api
        import json, tempfile

        app = Flask(__name__)
        api = Api(app, version="1.0", title="Test")

        cfg = {
            "namespaces": [{"name": "workers", "description": "workers"}],
            "models": {"Empty": {}},
            "endpoints": [
                {
                    "namespace": "workers",
                    "operation_name": "ner",
                    "model_name": "Empty",
                    "request_method": ["POST"],
                    "api_url": "/workers/ner",
                    "exec_method": {"module_name": "builtins", "method_name": "str"},
                }
            ],
        }

        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump(cfg, f)
            path = f.name

        generate_endpoints_from_config(api, path)

        with app.test_client() as c:
            resp = c.get("/swagger.json")
            data = json.loads(resp.data)
            paths = list(data.get("paths", {}).keys())

        # Should be /workers/ner, NOT /workers/ner/workers/ner
        assert "/workers/ner" in paths, f"Got paths: {paths}"
        assert "/workers/ner/workers/ner" not in paths, f"Doubled path found: {paths}"


# ---------------------------------------------------------------------------
# framework.decorators.common — function-level call policies
# ---------------------------------------------------------------------------

class TestCallRetry:
    """Tests for @call_retry (tenacity-backed)."""

    def test_retries_on_exception(self):
        from framework.decorators.common import retry

        call_count = [0]

        @retry(max_attempts=3, wait_fixed=0)
        def flaky():
            call_count[0] += 1
            if call_count[0] < 3:
                raise ValueError("fail")
            return "ok"

        assert flaky() == "ok"
        assert call_count[0] == 3

    def test_raises_retry_exhausted(self):
        from framework.decorators.common import retry, RetryExhaustedError

        @retry(max_attempts=2, wait_fixed=0)
        def always_fails():
            raise RuntimeError("boom")

        try:
            always_fails()
            assert False, "Should have raised"
        except RetryExhaustedError:
            pass

    def test_reraise_option(self):
        from framework.decorators.common import retry

        @retry(max_attempts=2, wait_fixed=0, reraise=True)
        def always_fails():
            raise ValueError("original")

        try:
            always_fails()
            assert False, "Should have raised"
        except ValueError as exc:
            assert "original" in str(exc)

    def test_only_retries_specified_exceptions(self):
        from framework.decorators.common import retry

        call_count = [0]

        @retry(max_attempts=3, wait_fixed=0, exceptions=(TypeError,))
        def raises_value_error():
            call_count[0] += 1
            raise ValueError("not retried")

        try:
            raises_value_error()
        except ValueError:
            pass

        # Should not retry — ValueError not in exceptions tuple
        assert call_count[0] == 1

    def test_no_retry_on_success(self):
        from framework.decorators.common import retry

        call_count = [0]

        @retry(max_attempts=5, wait_fixed=0)
        def succeeds():
            call_count[0] += 1
            return "done"

        result = succeeds()
        assert result == "done"
        assert call_count[0] == 1

    def test_exponential_backoff_configured(self):
        """Decorator is accepted and callable with exponential mode (no actual wait)."""
        from framework.decorators.common import retry

        @retry(max_attempts=2, wait_exponential=True, wait_multiplier=0.001, wait_max=0.01)
        def fn():
            return 42

        assert fn() == 42


class TestCallCircuitBreaker:
    """Tests for @call_circuit_breaker (pybreaker-backed)."""

    def test_passes_through_on_success(self):
        from framework.decorators.common import call_circuit_breaker

        @call_circuit_breaker(fail_max=3, reset_timeout=60)
        def fn(x):
            return x * 2

        assert fn(5) == 10

    def test_opens_after_fail_max(self):
        from framework.decorators.common import call_circuit_breaker, CircuitOpenError

        @call_circuit_breaker(fail_max=2, reset_timeout=60)
        def always_fails():
            raise RuntimeError("err")

        # pybreaker raises CircuitBreakerError (→ CircuitOpenError) on the call
        # that reaches fail_max, and on all subsequent calls while open.
        open_count = [0]
        for _ in range(3):
            try:
                always_fails()
            except CircuitOpenError:
                open_count[0] += 1
            except RuntimeError:
                pass

        assert open_count[0] >= 1, "Circuit should have opened"

    def test_circuit_breaker_attribute_exposed(self):
        from framework.decorators.common import call_circuit_breaker
        import pybreaker

        @call_circuit_breaker(fail_max=3, reset_timeout=30)
        def fn():
            return 1

        assert hasattr(fn, "__circuit_breaker__")
        assert isinstance(fn.__circuit_breaker__, pybreaker.CircuitBreaker)

    def test_exclude_exceptions_not_counted(self):
        from framework.decorators.common import call_circuit_breaker, CircuitOpenError

        @call_circuit_breaker(fail_max=2, reset_timeout=60, exclude=(ValueError,))
        def raises_value_error():
            raise ValueError("excluded")

        for _ in range(5):
            try:
                raises_value_error()
            except ValueError:
                pass

        # Circuit should still be closed — ValueError is excluded
        try:
            raises_value_error()
        except ValueError:
            pass  # expected
        except CircuitOpenError:
            assert False, "Circuit should NOT be open for excluded exceptions"


class TestCallRateLimit:
    """Tests for @call_rate_limit (limits-backed)."""

    def test_allows_calls_within_limit(self):
        from framework.decorators.common import call_rate_limit

        @call_rate_limit(per_second=100)
        def fn():
            return "ok"

        for _ in range(5):
            assert fn() == "ok"

    def test_raises_on_exceeded(self):
        from framework.decorators.common import call_rate_limit, RateLimitExceededError

        @call_rate_limit(per_second=2, key="test_raises_on_exceeded")
        def fn():
            return "ok"

        fn()
        fn()
        try:
            fn()
            assert False, "Should have raised RateLimitExceededError"
        except RateLimitExceededError:
            pass

    def test_per_minute_limit(self):
        from framework.decorators.common import call_rate_limit, RateLimitExceededError

        @call_rate_limit(per_minute=3, key="test_per_minute_limit")
        def fn():
            return "ok"

        fn()
        fn()
        fn()
        try:
            fn()
            assert False
        except RateLimitExceededError:
            pass

    def test_requires_exactly_one_window(self):
        from framework.decorators.common import call_rate_limit

        try:
            @call_rate_limit(per_second=1, per_minute=1)
            def fn():
                pass
            assert False, "Should raise ValueError"
        except ValueError:
            pass

        try:
            @call_rate_limit()
            def fn2():
                pass
            assert False, "Should raise ValueError"
        except ValueError:
            pass

    def test_invalid_on_exceeded(self):
        from framework.decorators.common import call_rate_limit

        try:
            @call_rate_limit(per_second=1, on_exceeded="invalid")
            def fn():
                pass
            assert False, "Should raise ValueError"
        except ValueError:
            pass

    def test_callable_key(self):
        from framework.decorators.common import call_rate_limit, RateLimitExceededError

        call_counts: dict = {}

        @call_rate_limit(per_second=2, key=lambda user: f"rl_test_callable_{user}")
        def fn(user: str):
            call_counts[user] = call_counts.get(user, 0) + 1
            return user

        # Each user has their own bucket
        fn("alice")
        fn("alice")
        fn("bob")
        fn("bob")

        # alice's bucket exhausted
        try:
            fn("alice")
            assert False
        except RateLimitExceededError:
            pass

        # bob's bucket also exhausted
        try:
            fn("bob")
            assert False
        except RateLimitExceededError:
            pass


class TestCommonDecoratorImports:
    """Verify all public names are importable from framework.decorators."""

    def test_call_retry_importable(self):
        from framework.decorators import call_retry
        assert callable(call_retry)

    def test_call_circuit_breaker_importable(self):
        from framework.decorators import call_circuit_breaker
        assert callable(call_circuit_breaker)

    def test_call_rate_limit_importable(self):
        from framework.decorators import call_rate_limit
        assert callable(call_rate_limit)

    def test_exceptions_importable(self):
        from framework.decorators import RetryExhaustedError, CircuitOpenError, RateLimitExceededError
        assert issubclass(RetryExhaustedError, Exception)
        assert issubclass(CircuitOpenError, Exception)
        assert issubclass(RateLimitExceededError, Exception)

    def test_kafka_policies_still_importable(self):
        from framework.decorators import retry_to_dlq, circuit_breaker, rate_limit
        assert callable(retry_to_dlq)
        assert callable(circuit_breaker)
        assert callable(rate_limit)
