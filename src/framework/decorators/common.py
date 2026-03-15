"""
Function-level call policies backed by battle-tested libraries.

These decorators **wrap the function call** — unlike the Kafka-runtime policies
in :mod:`framework.decorators.intern`, they work in *any* context: inside Kafka
workers, HTTP handlers, cron jobs, standalone scripts, etc.

Gevent compatibility
--------------------
All three backing libraries (tenacity, pybreaker, limits) use standard
``threading`` primitives.  When gevent is used, call
``gevent.monkey.patch_all()`` **before** importing this module (or any
framework module).  After monkey-patching, ``threading.RLock``,
``threading.Lock``, and ``time.sleep`` are all greenlet-friendly.

Decorators
----------
``@retry``
    Retry a function on exception using *tenacity*.  Supports fixed wait,
    exponential back-off, and a configurable exception filter.

``@call_circuit_breaker``
    Trip open after N consecutive failures using *pybreaker*.  While open,
    calls raise ``CircuitOpenError`` immediately without invoking the function.

``@call_rate_limit``
    Enforce a per-key rate limit using *limits*.  On breach either raises
    ``RateLimitExceededError`` or sleeps until the window resets.

Custom exceptions
-----------------
``RetryExhaustedError``     — raised when all retry attempts are exhausted.
``CircuitOpenError``        — raised when the circuit is open.
``RateLimitExceededError``  — raised when the rate limit is exceeded.
"""

from __future__ import annotations

import functools
import time
from typing import Any, Callable, Optional, Tuple, Type, Union

# ---------------------------------------------------------------------------
# Custom exceptions
# ---------------------------------------------------------------------------


class RetryExhaustedError(Exception):
    """Raised when all retry attempts are exhausted."""


class CircuitOpenError(Exception):
    """Raised when the circuit breaker is open."""


class RateLimitExceededError(Exception):
    """Raised when the rate limit is exceeded."""


# ---------------------------------------------------------------------------
# @retry — backed by tenacity
# ---------------------------------------------------------------------------


def retry(
    *,
    max_attempts: int = 3,
    wait_fixed: float = 1.0,
    wait_exponential: bool = False,
    wait_multiplier: float = 1.0,
    wait_max: float = 60.0,
    exceptions: Tuple[Type[BaseException], ...] = (Exception,),
    reraise: bool = False,
) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """
    Retry ``fn`` on exception.

    Parameters
    ----------
    max_attempts:
        Total number of attempts (1 = no retry).
    wait_fixed:
        Seconds to wait between attempts when ``wait_exponential=False``.
    wait_exponential:
        Use exponential back-off instead of fixed wait.
    wait_multiplier:
        Multiplier for exponential back-off (seconds × attempt^2).
    wait_max:
        Cap for exponential back-off (seconds).
    exceptions:
        Only retry on these exception types.
    reraise:
        If ``True``, re-raise the last exception instead of raising
        ``RetryExhaustedError``.
    """
    import tenacity

    if wait_exponential:
        wait = tenacity.wait_exponential(multiplier=wait_multiplier, max=wait_max)
    else:
        wait = tenacity.wait_fixed(wait_fixed)

    # retry_error_cls and reraise=True are mutually exclusive in tenacity:
    # reraise re-raises the original exception, so a custom error class is unused.
    retry_kwargs: dict = dict(
        stop=tenacity.stop_after_attempt(max_attempts),
        wait=wait,
        retry=tenacity.retry_if_exception_type(exceptions),
        reraise=reraise,
    )
    if not reraise:
        retry_kwargs["retry_error_cls"] = RetryExhaustedError

    retry_obj = tenacity.retry(**retry_kwargs)

    def decorator(fn: Callable[..., Any]) -> Callable[..., Any]:
        wrapped = retry_obj(fn)
        # Preserve original function metadata
        functools.update_wrapper(wrapped, fn)
        return wrapped

    return decorator


# ---------------------------------------------------------------------------
# @call_circuit_breaker — backed by pybreaker
# ---------------------------------------------------------------------------


def call_circuit_breaker(
    *,
    fail_max: int = 5,
    reset_timeout: int = 30,
    name: Optional[str] = None,
    exclude: Tuple[Type[BaseException], ...] = (),
) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """
    Trip open after ``fail_max`` consecutive failures.

    Parameters
    ----------
    fail_max:
        Number of consecutive failures before the circuit opens.
    reset_timeout:
        Seconds in the open state before moving to half-open.
    name:
        Optional name for the breaker (useful for monitoring / logging).
    exclude:
        Exception types that do *not* count as failures.
    """
    import pybreaker

    listeners: list = []

    breaker = pybreaker.CircuitBreaker(
        fail_max=fail_max,
        reset_timeout=reset_timeout,
        name=name,
        listeners=listeners,
        exclude=list(exclude) if exclude else [],
    )

    def decorator(fn: Callable[..., Any]) -> Callable[..., Any]:
        @functools.wraps(fn)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            try:
                return breaker.call(fn, *args, **kwargs)
            except pybreaker.CircuitBreakerError as exc:
                raise CircuitOpenError(str(exc)) from exc

        # Expose the underlying breaker for inspection / testing
        wrapper.__circuit_breaker__ = breaker  # type: ignore[attr-defined]
        return wrapper

    return decorator


# ---------------------------------------------------------------------------
# @call_rate_limit — backed by limits
# ---------------------------------------------------------------------------


def call_rate_limit(
    *,
    per_second: Optional[float] = None,
    per_minute: Optional[float] = None,
    per_hour: Optional[float] = None,
    storage_url: str = "memory://",
    on_exceeded: str = "raise",
    key: Optional[str] = None,
) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """
    Enforce a moving-window rate limit.

    Exactly one of ``per_second``, ``per_minute``, ``per_hour`` must be set.

    Parameters
    ----------
    per_second / per_minute / per_hour:
        Maximum number of calls in the given window.
    storage_url:
        ``limits`` storage URI.  ``"memory://"`` (default) uses an in-process
        dict.  ``"redis://localhost:6379"`` uses Redis (gevent-safe after
        monkey-patch).
    on_exceeded:
        ``"raise"`` (default) — raise ``RateLimitExceededError``.
        ``"sleep"`` — block until the window resets then proceed.
    key:
        Rate-limit bucket key.  Defaults to the function's qualified name.
        Use a callable ``key=lambda *a, **kw: ...`` for per-argument buckets.
    """
    from limits import parse as _parse_limit
    from limits.storage import storage_from_string
    from limits.strategies import MovingWindowRateLimiter

    # Validate exactly one window is provided
    windows = [v for v in (per_second, per_minute, per_hour) if v is not None]
    if len(windows) != 1:
        raise ValueError(
            "Exactly one of per_second, per_minute, or per_hour must be set"
        )

    if per_second is not None:
        limit_str = f"{int(per_second)} per second"
        window_sec = 1.0
    elif per_minute is not None:
        limit_str = f"{int(per_minute)} per minute"
        window_sec = 60.0
    else:
        limit_str = f"{int(per_hour)} per hour"
        window_sec = 3600.0

    if on_exceeded not in ("raise", "sleep"):
        raise ValueError("on_exceeded must be 'raise' or 'sleep'")

    limit_item = _parse_limit(limit_str)
    storage = storage_from_string(storage_url)
    limiter = MovingWindowRateLimiter(storage)

    def decorator(fn: Callable[..., Any]) -> Callable[..., Any]:
        default_key = key if key is not None else f"{fn.__module__}.{fn.__qualname__}"

        @functools.wraps(fn)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            # key can be a callable for per-argument buckets
            bucket = default_key(*args, **kwargs) if callable(default_key) else default_key

            if limiter.hit(limit_item, bucket):
                return fn(*args, **kwargs)

            # Limit exceeded
            if on_exceeded == "sleep":
                # Sleep for the remainder of the window and retry once
                time.sleep(window_sec)
                if limiter.hit(limit_item, bucket):
                    return fn(*args, **kwargs)

            raise RateLimitExceededError(
                f"Rate limit exceeded: {limit_str} for key '{bucket}'"
            )

        return wrapper

    return decorator
