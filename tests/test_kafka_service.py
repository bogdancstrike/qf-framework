"""
Unit tests for poc_app/src/service/kafka_service.py.

All KafkaClient interactions are mocked — no live Kafka required.

Run with:
    python -m pytest tests/test_kafka_service.py -v -m unit
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

def _make_kafka_client(last_message=None):
    """Return a MagicMock that behaves like KafkaClient."""
    client = MagicMock()
    client.put_message.return_value = None
    client.consume_message.return_value = (
        json.dumps(last_message) if last_message else None
    )
    return client


# ---------------------------------------------------------------------------
# publish
# ---------------------------------------------------------------------------

@pytest.mark.unit
class TestPublish:

    def test_returns_true_on_success(self):
        import service.kafka_service as ks
        ks._kafka_client = _make_kafka_client()
        assert ks.publish("poc.echo", {"id": "1"}) is True

    def test_calls_put_message_with_json_payload(self):
        import service.kafka_service as ks
        client = _make_kafka_client()
        ks._kafka_client = client
        ks.publish("poc.echo", {"id": "1", "v": 42})
        args, kwargs = client.put_message.call_args
        assert args[0] == "poc.echo"
        decoded = json.loads(args[1])
        assert decoded["v"] == 42

    def test_passes_key_to_put_message(self):
        import service.kafka_service as ks
        client = _make_kafka_client()
        ks._kafka_client = client
        ks.publish("poc.echo", {"id": "2"}, key="my-key")
        _, kwargs = client.put_message.call_args
        assert kwargs.get("key") == "my-key"

    def test_returns_false_on_exception(self):
        import service.kafka_service as ks
        client = _make_kafka_client()
        client.put_message.side_effect = ConnectionError("broker down")
        ks._kafka_client = client
        assert ks.publish("poc.echo", {"id": "3"}) is False

    def test_exception_does_not_propagate(self):
        """publish() must not raise — callers rely on the bool return value."""
        import service.kafka_service as ks
        client = _make_kafka_client()
        client.put_message.side_effect = RuntimeError("unexpected")
        ks._kafka_client = client
        result = ks.publish("poc.echo", {"id": "4"})
        assert result is False


# ---------------------------------------------------------------------------
# consume_last
# ---------------------------------------------------------------------------

@pytest.mark.unit
class TestConsumeLast:

    def test_returns_parsed_dict_on_success(self):
        import service.kafka_service as ks
        msg = {"id": "5", "result": "ok"}
        ks._kafka_client = _make_kafka_client(last_message=msg)
        result = ks.consume_last("poc.echo")
        assert result == msg

    def test_returns_none_when_topic_empty(self):
        import service.kafka_service as ks
        ks._kafka_client = _make_kafka_client(last_message=None)
        result = ks.consume_last("poc.echo")
        assert result is None

    def test_returns_none_on_exception(self):
        import service.kafka_service as ks
        client = _make_kafka_client()
        client.consume_message.side_effect = ConnectionError("broker down")
        ks._kafka_client = client
        result = ks.consume_last("poc.echo")
        assert result is None

    def test_uses_provided_group_id(self):
        import service.kafka_service as ks
        client = _make_kafka_client(last_message={"id": "x"})
        ks._kafka_client = client
        ks.consume_last("poc.echo", group_id="my-group")
        _, kwargs = client.consume_message.call_args
        assert kwargs.get("group_id") == "my-group"
