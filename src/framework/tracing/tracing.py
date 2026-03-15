from __future__ import annotations

from typing import Optional
from opentelemetry import trace
from opentelemetry.sdk.resources import Resource, SERVICE_NAME
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor

# Optional exporter dependencies
try:
    from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
except Exception:  # pragma: no cover
    OTLPSpanExporter = None  # type: ignore

tracer = None

def init_tracing(service_name: str, otlp_endpoint: Optional[str]=None, insecure: bool=True) -> None:
    """Initialize OpenTelemetry tracing.

    - service_name: logical service identifier
    - otlp_endpoint: e.g. http://jaeger:4317 or http://otel-collector:4317
    If exporter isn't available, init becomes a no-op.
    """
    global tracer

    provider = TracerProvider(resource=Resource.create({SERVICE_NAME: service_name}))
    trace.set_tracer_provider(provider)

    if otlp_endpoint and OTLPSpanExporter is not None:
        exporter = OTLPSpanExporter(endpoint=otlp_endpoint, insecure=insecure)
        provider.add_span_processor(BatchSpanProcessor(exporter))

    tracer = trace.get_tracer(service_name)

def get_tracer():
    global tracer
    if tracer is None:
        tracer = trace.get_tracer(__name__)
    return tracer
