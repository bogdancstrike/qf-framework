"""
Backward-compatibility shim — do not use in new code.

All symbols have moved to :mod:`framework.decorators.intern`.
"""
from .intern import (  # noqa: F401
    RetryToDlqConfig,
    CircuitBreakerConfig,
    RateLimitConfig,
    retry_to_dlq,
    circuit_breaker,
    rate_limit,
    read_policy_metadata,
)
