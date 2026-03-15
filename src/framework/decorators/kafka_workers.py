from __future__ import annotations

"""Kafka workers framework (registry + decorators)."""

import uuid
from dataclasses import dataclass, replace
from typing import Any, Callable, Dict, List, Optional, Literal

from .intern import RetryToDlqConfig, CircuitBreakerConfig, RateLimitConfig


WorkerKind = Literal["handler", "aggregator"]


def _get_by_path(obj: dict, path: str) -> Any:
    cur: Any = obj
    for part in path.split("."):
        if not isinstance(cur, dict):
            return None
        cur = cur.get(part)
    return cur


def _iter_wrapped(fn: Callable[..., Any]):
    cur = fn
    seen = set()
    while cur is not None and id(cur) not in seen:
        seen.add(id(cur))
        yield cur
        cur = getattr(cur, "__wrapped__", None)


def _get_attr_anywhere(fn: Callable[..., Any], attr: str):
    for w in _iter_wrapped(fn):
        if hasattr(w, attr):
            return getattr(w, attr)
    return getattr(fn, attr, None)


def ensure_message_id(message: dict) -> str:
    mid = message.get("id")
    if not mid:
        mid = str(uuid.uuid4())
        message["id"] = mid
    return str(mid)


@dataclass(frozen=True)
class WorkerSpec:
    kind: WorkerKind
    name: str
    topics_in: List[str]
    topics_out: List[str]
    metadatas: Dict[str, Any]
    max_workers: int

    retry_to_dlq: Optional[RetryToDlqConfig] = None
    circuit_breaker: Optional[CircuitBreakerConfig] = None
    rate_limit: Optional[RateLimitConfig] = None

    bulk_mode: bool = False
    batch_size: int = 1
    batch_timeout_ms: int = 200

    aggregate_by: Optional[str] = None
    aggregator_timeout_sec: int = 600

    fn: Callable[..., Any] = lambda *a, **k: None


_WORKERS_BY_NAME: Dict[str, WorkerSpec] = {}
_TOPIC_TO_WORKER: Dict[str, str] = {}


def update_registered_worker_policy(worker_name: str, key: str, value: Any) -> None:
    """
    Called by policy decorators if applied after @kafka_handler.
    WorkerSpec is frozen; we replace it in registry.
    """
    spec = _WORKERS_BY_NAME.get(worker_name)
    if not spec:
        return

    if key == "_qsint_retry_to_dlq" and isinstance(value, RetryToDlqConfig):
        _WORKERS_BY_NAME[worker_name] = replace(spec, retry_to_dlq=value)
    elif key == "_qsint_circuit_breaker" and isinstance(value, CircuitBreakerConfig):
        _WORKERS_BY_NAME[worker_name] = replace(spec, circuit_breaker=value)
    elif key == "_qsint_rate_limit" and isinstance(value, RateLimitConfig):
        _WORKERS_BY_NAME[worker_name] = replace(spec, rate_limit=value)


def register_worker(spec: WorkerSpec) -> None:
    if spec.name in _WORKERS_BY_NAME:
        raise RuntimeError(f"Duplicate worker name '{spec.name}'")

    if not spec.topics_in:
        raise ValueError(f"Worker '{spec.name}' must define topics_in")
    if not spec.topics_out:
        raise ValueError(f"Worker '{spec.name}' must define topics_out")

    for t in spec.topics_in:
        if t in _TOPIC_TO_WORKER:
            raise RuntimeError(
                f"Topic '{t}' already handled by '{_TOPIC_TO_WORKER[t]}'. "
                f"Only one worker per topic is allowed in this framework."
            )

    if spec.kind == "handler" and spec.bulk_mode:
        if spec.batch_size < 2:
            raise ValueError(f"Worker '{spec.name}': batch_size must be >= 2 when bulk_mode=True")
        if spec.batch_timeout_ms < 1:
            raise ValueError(f"Worker '{spec.name}': batch_timeout_ms must be >= 1")

    if spec.kind == "aggregator":
        if not spec.aggregate_by:
            raise ValueError(f"Aggregator '{spec.name}' must define aggregate_by (e.g. 'id')")
        if len(spec.topics_in) < 2:
            raise ValueError(f"Aggregator '{spec.name}' must define 2+ topics_in")

    _WORKERS_BY_NAME[spec.name] = spec
    for t in spec.topics_in:
        _TOPIC_TO_WORKER[t] = spec.name


def all_topics() -> List[str]:
    return sorted(_TOPIC_TO_WORKER.keys())


def all_workers() -> List[WorkerSpec]:
    return list(_WORKERS_BY_NAME.values())


def worker_for_topic(topic: str) -> Optional[WorkerSpec]:
    name = _TOPIC_TO_WORKER.get(topic)
    return _WORKERS_BY_NAME.get(name) if name else None


def compute_aggregate_key(spec: WorkerSpec, message: dict) -> str:
    if spec.aggregate_by:
        v = _get_by_path(message, spec.aggregate_by)
        if v is not None:
            return str(v)
    return ensure_message_id(message)


def kafka_handler(
    *,
    name: str,
    topics_in: List[str],
    topics_out: List[str],
    metadatas: Optional[Dict[str, Any]] = None,
    max_workers: int = 4,
    bulk_mode: bool = False,
    batch_size: int = 50,
    batch_timeout_ms: int = 200,
):
    def deco(fn: Callable[..., Any]) -> Callable[..., Any]:
        # Read policies from anywhere in wrapper chain
        retry_cfg = _get_attr_anywhere(fn, "_qsint_retry_to_dlq")
        cb_cfg = _get_attr_anywhere(fn, "_qsint_circuit_breaker")
        rl_cfg = _get_attr_anywhere(fn, "_qsint_rate_limit")

        register_worker(
            WorkerSpec(
                kind="handler",
                name=name,
                topics_in=list(topics_in),
                topics_out=list(topics_out),
                metadatas=dict(metadatas or {}),
                max_workers=int(max_workers),
                retry_to_dlq=retry_cfg,
                circuit_breaker=cb_cfg,
                rate_limit=rl_cfg,
                bulk_mode=bool(bulk_mode),
                batch_size=int(batch_size),
                batch_timeout_ms=int(batch_timeout_ms),
                fn=fn,
            )
        )

        # Tag fn so policy decorators applied later can update registry
        try:
            setattr(fn, "_qsint_worker_name", name)
        except Exception:
            pass

        return fn

    return deco


def kafka_aggregator(
    *,
    name: str,
    topics_in: List[str],
    topics_out: List[str],
    aggregate_by: str = "id",
    aggregator_timeout_sec: int = 600,
    metadatas: Optional[Dict[str, Any]] = None,
    max_workers: int = 4,
):
    def deco(fn: Callable[..., Any]) -> Callable[..., Any]:
        retry_cfg = _get_attr_anywhere(fn, "_qsint_retry_to_dlq")
        cb_cfg = _get_attr_anywhere(fn, "_qsint_circuit_breaker")
        rl_cfg = _get_attr_anywhere(fn, "_qsint_rate_limit")

        register_worker(
            WorkerSpec(
                kind="aggregator",
                name=name,
                topics_in=list(topics_in),
                topics_out=list(topics_out),
                metadatas=dict(metadatas or {}),
                max_workers=int(max_workers),
                retry_to_dlq=retry_cfg,
                circuit_breaker=cb_cfg,
                rate_limit=rl_cfg,
                aggregate_by=str(aggregate_by),
                aggregator_timeout_sec=int(aggregator_timeout_sec),
                fn=fn,
            )
        )

        try:
            setattr(fn, "_qsint_worker_name", name)
        except Exception:
            pass

        return fn

    return deco
