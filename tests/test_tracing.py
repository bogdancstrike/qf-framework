"""
Unit tests for framework.tracing — NoOpTracer, NoOpSpan, get_tracer().

Run with:
    python -m pytest tests/test_tracing.py -v -m unit
"""
import sys
import os
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))


@pytest.mark.unit
class TestNoOpSpan:
    """NoOpSpan must silently accept every OTel Span method."""

    def setup_method(self):
        from framework.tracing import NoOpSpan
        self.span = NoOpSpan()

    def test_set_attribute_returns_self(self):
        result = self.span.set_attribute("key", "value")
        assert result is self.span

    def test_set_attribute_chainable(self):
        # Chaining multiple set_attribute calls should not raise.
        self.span.set_attribute("a", 1).set_attribute("b", 2)

    def test_record_exception_noop(self):
        self.span.record_exception(ValueError("test"))

    def test_set_status_noop(self):
        self.span.set_status("OK")

    def test_add_event_noop(self):
        self.span.add_event("my-event", attributes={"k": "v"})

    def test_update_name_noop(self):
        self.span.update_name("new-name")

    def test_is_recording_false(self):
        assert self.span.is_recording() is False

    def test_end_noop(self):
        self.span.end()

    def test_context_manager(self):
        from framework.tracing import NoOpSpan
        with NoOpSpan() as s:
            s.set_attribute("x", 1)

    def test_context_manager_propagates_exception(self):
        from framework.tracing import NoOpSpan
        with pytest.raises(RuntimeError):
            with NoOpSpan():
                raise RuntimeError("propagated")


@pytest.mark.unit
class TestNoOpTracer:
    """NoOpTracer must implement the OTel Tracer context-manager interface."""

    def setup_method(self):
        from framework.tracing import NoOpTracer
        self.tracer = NoOpTracer()

    def test_start_as_current_span_yields_noop_span(self):
        from framework.tracing import NoOpSpan
        with self.tracer.start_as_current_span("op") as span:
            assert isinstance(span, NoOpSpan)

    def test_span_accepts_attributes_in_context(self):
        with self.tracer.start_as_current_span("op") as span:
            span.set_attribute("endpoint", "test")
            span.set_attribute("count", 42)

    def test_start_span_returns_noop_span(self):
        from framework.tracing import NoOpSpan
        span = self.tracer.start_span("op")
        assert isinstance(span, NoOpSpan)
        span.end()

    def test_nested_spans(self):
        with self.tracer.start_as_current_span("outer") as outer:
            with self.tracer.start_as_current_span("inner") as inner:
                inner.set_attribute("k", "v")
            outer.set_attribute("done", True)

    def test_exception_propagates_through_span(self):
        with pytest.raises(ValueError):
            with self.tracer.start_as_current_span("op"):
                raise ValueError("should propagate")


@pytest.mark.unit
class TestGetTracer:
    """get_tracer() should return NoOpTracer when ENABLE_TRACING=false."""

    def setup_method(self):
        # Reset module-level tracer state before each test.
        import framework.tracing.tracing as _t
        _t.tracer = None

    def test_returns_noop_tracer_when_disabled(self, monkeypatch):
        monkeypatch.setenv("ENABLE_TRACING", "false")
        import framework.tracing.tracing as _t
        _t.tracer = None
        from framework.tracing import get_tracer, NoOpTracer
        t = get_tracer()
        assert isinstance(t, NoOpTracer)

    def test_returns_same_instance_on_repeated_calls(self, monkeypatch):
        monkeypatch.setenv("ENABLE_TRACING", "false")
        import framework.tracing.tracing as _t
        _t.tracer = None
        from framework.tracing import get_tracer
        t1 = get_tracer()
        t2 = get_tracer()
        assert t1 is t2

    def test_init_tracing_noop_when_disabled(self, monkeypatch):
        monkeypatch.setenv("ENABLE_TRACING", "false")
        import framework.tracing.tracing as _t
        _t.tracer = None
        from framework.tracing import init_tracing, get_tracer, NoOpTracer
        init_tracing("test-service")
        assert isinstance(get_tracer(), NoOpTracer)
