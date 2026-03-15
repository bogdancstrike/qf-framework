"""
OpenTelemetry tracing with ENABLE_TRACING toggle and NoOpTracer fallback.

ENABLE_TRACING environment variable controls which tracer is active:
  ENABLE_TRACING=true   -> real OTel TracerProvider; spans are exported via OTLP
                           (to Jaeger, OpenTelemetry Collector, etc.)
  ENABLE_TRACING=false  -> NoOpTracer; all span calls are accepted but silently
                           discarded. Zero allocations, zero I/O. (default)

This design means application code never needs to guard against a None tracer:

    tracer = get_tracer()
    with tracer.start_as_current_span("my-operation") as span:
        span.set_attribute("key", "value")  # safe whether tracing is on or off

Usage in the PoC:
  - Call init_tracing() once at startup (main.py → runner.py → init_tracing)
  - Call get_tracer() anywhere to obtain the active tracer
  - Wrap operations in start_as_current_span() context managers

OTLP endpoint (for Jaeger / OTel Collector):
  Set QSINT_OTLP_ENDPOINT=http://localhost:4317 and ENABLE_TRACING=true.
  The gRPC exporter is used (port 4317). For HTTP exporter use port 4318.

Auto-instrumentation (activated by init_tracing when ENABLE_TRACING=true):
  - Flask   — every HTTP request/response becomes a span (server-side).
  - requests — every outbound HTTP call becomes a child span (client-side).
  - Kafka   — every produce/consume via kafka-python becomes a span.
  - Redis   — every redis-py command becomes a child span.
  - SQLAlchemy — every DB query becomes a child span (if an engine is passed).
  Instrumentation libraries are imported lazily and silently skipped if not
  installed, so the framework works without them in minimal environments.
"""

from __future__ import annotations

import contextlib
import os
from typing import Optional

from opentelemetry import trace
from opentelemetry.sdk.resources import Resource, SERVICE_NAME
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor

# Optional OTLP gRPC exporter — gracefully absent if not installed.
try:
    from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
except Exception:  # pragma: no cover
    OTLPSpanExporter = None  # type: ignore


def _tracing_enabled() -> bool:
    """Return True if ENABLE_TRACING env var is set to a truthy value."""
    return os.environ.get("ENABLE_TRACING", "false").strip().lower() in (
        "1", "true", "yes", "y", "on"
    )


# ---------------------------------------------------------------------------
# NoOp tracer — zero-overhead stand-in used when ENABLE_TRACING=false
# ---------------------------------------------------------------------------

class NoOpSpan:
    """Silent span — accepts every OTel Span method but does nothing.

    The interface mirrors opentelemetry.trace.Span so code that calls
    span.set_attribute(), span.record_exception(), etc. works without changes
    regardless of whether real tracing is active.
    """

    def set_attribute(self, key: str, value) -> "NoOpSpan":
        return self

    def record_exception(self, exception, attributes=None, timestamp=None, escaped=False) -> None:
        pass

    def set_status(self, status, description: Optional[str] = None) -> None:
        pass

    def add_event(self, name: str, attributes=None, timestamp=None) -> None:
        pass

    def update_name(self, name: str) -> None:
        pass

    def is_recording(self) -> bool:
        return False

    def end(self, end_time=None) -> None:
        pass

    # Context-manager support so NoOpSpan can be used with start_span() manually.
    def __enter__(self) -> "NoOpSpan":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        # Return False to let exceptions propagate normally.
        return False


class NoOpTracer:
    """Tracer that implements the OTel Tracer interface but discards everything.

    Used when ENABLE_TRACING=false. Compatible with:
        tracer.start_as_current_span("name")   # context manager
        tracer.start_span("name")              # explicit, call span.end()

    No threads, no I/O, no allocations beyond the NoOpSpan instance itself.
    """

    @contextlib.contextmanager
    def start_as_current_span(self, name: str, **kwargs):
        """Context manager that yields a NoOpSpan.

        The 'with' block runs normally; the span object silently accepts all
        attribute and event calls. Exceptions are NOT suppressed.
        """
        yield NoOpSpan()

    def start_span(self, name: str, **kwargs) -> NoOpSpan:
        """Return a NoOpSpan. Caller must call span.end() when finished."""
        return NoOpSpan()


# ---------------------------------------------------------------------------
# Module-level tracer state
# ---------------------------------------------------------------------------

# Starts as None; set by init_tracing() or lazily by get_tracer().
tracer: Optional[object] = None


def _instrument_libraries() -> None:
    """Activate OTel auto-instrumentation for all supported libraries.

    Each instrumentor wraps the target library's internals so that every
    call (HTTP request, Redis command, Kafka produce/consume, SQL query)
    automatically creates a child span under the current active span.

    All imports are guarded so that missing packages are silently skipped
    rather than crashing at startup.
    """
    # Flask — instrument HTTP server spans (must be called before app starts).
    try:
        from opentelemetry.instrumentation.flask import FlaskInstrumentor
        FlaskInstrumentor().instrument()
    except Exception:
        pass

    # requests — instrument outbound HTTP calls (e.g. to external APIs).
    try:
        from opentelemetry.instrumentation.requests import RequestsInstrumentor
        RequestsInstrumentor().instrument()
    except Exception:
        pass

    # kafka-python — instrument producer.send() and consumer.poll().
    try:
        from opentelemetry.instrumentation.kafka import KafkaInstrumentor
        KafkaInstrumentor().instrument()
    except Exception:
        pass

    # redis-py — instrument every Redis command (GET, SET, INCR, SCAN, ...).
    try:
        from opentelemetry.instrumentation.redis import RedisInstrumentor
        RedisInstrumentor().instrument()
    except Exception:
        pass

    # SQLAlchemy — instrument DB queries (engine-level; no engine arg needed
    # when using the global auto-instrument mode).
    try:
        from opentelemetry.instrumentation.sqlalchemy import SQLAlchemyInstrumentor
        SQLAlchemyInstrumentor().instrument()
    except Exception:
        pass


def init_tracing(
    service_name: str,
    otlp_endpoint: Optional[str] = None,
    insecure: bool = True,
) -> None:
    """Initialize the global tracer and activate library auto-instrumentation.

    Should be called once at application startup (e.g. in main.py or runner.py).

    If ENABLE_TRACING=false (default), this sets the global tracer to a
    NoOpTracer and returns immediately — no OTel SDK objects are created.

    If ENABLE_TRACING=true:
      - Creates a TracerProvider with the given service_name.
      - If otlp_endpoint is set and the OTLP exporter package is installed,
        attaches a BatchSpanProcessor that exports to that endpoint.
      - Falls back to a no-export provider (spans are created but not sent)
        if otlp_endpoint is None or the exporter is unavailable.
      - Activates auto-instrumentation for Flask, requests, Kafka, Redis,
        and SQLAlchemy (any missing library is silently skipped).

    Args:
        service_name:  Logical name for this service (appears in Jaeger UI).
        otlp_endpoint: OTLP gRPC endpoint, e.g. "http://localhost:4317".
        insecure:      Skip TLS verification on OTLP connection (dev default).
    """
    global tracer

    if not _tracing_enabled():
        # Fast path: no OTel SDK involvement at all.
        tracer = NoOpTracer()
        return

    provider = TracerProvider(
        resource=Resource.create({SERVICE_NAME: service_name})
    )
    trace.set_tracer_provider(provider)

    if otlp_endpoint and OTLPSpanExporter is not None:
        # BatchSpanProcessor buffers spans and sends them in batches —
        # much cheaper than sending each span synchronously.
        exporter = OTLPSpanExporter(endpoint=otlp_endpoint, insecure=insecure)
        provider.add_span_processor(BatchSpanProcessor(exporter))

    # Wrap supported libraries so their calls appear as child spans in Jaeger.
    _instrument_libraries()

    tracer = trace.get_tracer(service_name)


def get_tracer():
    """Return the active tracer (real OTel or NoOpTracer).

    Safe to call anywhere — even before init_tracing(). If init_tracing()
    has not been called, this lazily initialises based on ENABLE_TRACING:
      - ENABLE_TRACING=true  -> real OTel tracer from the global provider
      - ENABLE_TRACING=false -> NoOpTracer (default)

    This means application code never needs to check if tracing is configured:

        tracer = get_tracer()
        with tracer.start_as_current_span("operation"):
            ...  # always works
    """
    global tracer
    if tracer is None:
        if _tracing_enabled():
            tracer = trace.get_tracer(__name__)
        else:
            tracer = NoOpTracer()
    return tracer
