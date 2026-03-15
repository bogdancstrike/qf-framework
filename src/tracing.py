"""Compatibility shim.

Real apps should prefer: from framework.tracing import init_tracing, get_tracer
"""
from framework.tracing.tracing import init_tracing, get_tracer, tracer  # noqa
