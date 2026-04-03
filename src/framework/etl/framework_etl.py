# framework_etl.py
from __future__ import annotations

import importlib
import json
import threading
import time
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from time import sleep
from typing import Deque, Dict, Iterable, List, Optional, Set

import redis as redis_lib

from kafka import KafkaConsumer, KafkaProducer, TopicPartition
from kafka.errors import KafkaError, NoBrokersAvailable

try:
    from kafka.structs import OffsetAndMetadata
except Exception:  # pragma: no cover
    OffsetAndMetadata = None  # type: ignore

from config import Config
from framework.commons.logger import logger
from framework.commons.utils import deep_merge
from framework.decorators.kafka_workers import (
    WorkerSpec,
    all_topics,
    all_workers,
    compute_aggregate_key,
    ensure_message_id,
    worker_for_topic,
)
from framework.decorators.intern import CircuitBreakerConfig, RateLimitConfig, RetryToDlqConfig
from framework.redis.redis_utils import RedisUtils
# from models.models import init_db


# ==========================================================
# Initialization
# ==========================================================

# init_db()

consumer_lock = threading.Lock()

redis_util = RedisUtils(
    host=Config.REDIS_HOST,
    port=int(Config.REDIS_PORT),
    db=int(Config.REDIS_DB),
    password=None,
    max_connections=int(getattr(Config, "REDIS_MAX_CONNECTIONS", 50)),
    socket_timeout=float(getattr(Config, "REDIS_SOCKET_TIMEOUT", 5.0)),
    socket_connect_timeout=float(getattr(Config, "REDIS_CONNECT_TIMEOUT", 5.0)),
    retry_on_timeout=str(getattr(Config, "REDIS_RETRY_ON_TIMEOUT", "true")).lower() == "true",
)


def _tp(tp: TopicPartition) -> str:
    return f"{tp.topic}[{tp.partition}]"


def _safe_decode(b: Optional[bytes]) -> str:
    if b is None:
        return ""
    try:
        return b.decode("utf-8", errors="replace")
    except Exception:
        return repr(b)


# ==========================================================
# Kafka connect helpers
# ==========================================================

def create_kafka_consumer(topics_input: List[str], bootstrap_servers: str, *, enable_auto_commit: bool) -> KafkaConsumer:
    retries = 0
    max_retries = int(getattr(Config, "KAFKA_CONNECT_RETRIES", 10))
    retry_sleep = float(getattr(Config, "KAFKA_CONNECT_RETRY_SLEEP_SEC", 5))

    while retries < max_retries:
        try:
            consumer = KafkaConsumer(
                *topics_input,
                bootstrap_servers=bootstrap_servers,
                auto_offset_reset="earliest",
                enable_auto_commit=enable_auto_commit,
                group_id=Config.WORKER_NAME,
                max_poll_interval_ms=900_000,
                session_timeout_ms=30_000,
                consumer_timeout_ms=0,
            )
            logger.debug(f"🔵 Kafka consumer connected | topics={topics_input} auto_commit={enable_auto_commit}", "blue")
            return consumer
        except (NoBrokersAvailable, KafkaError) as e:
            retries += 1
            logger.debug(f"🟡 Kafka consumer error: {e} | retry in {retry_sleep}s ({retries}/{max_retries})", "yellow")
            sleep(retry_sleep)

    raise RuntimeError(f"Failed to connect to Kafka consumer after {max_retries} retries.")


def create_kafka_producer(bootstrap_servers: str) -> KafkaProducer:
    """
    Producer tuned for throughput.
    NOTE:
      - Normal outputs are async (no wait) for throughput.
      - Bulk outputs can be configured to be ACK-gated per batch (recommended for correctness).
      - Retry/DLQ sends use _send_sync to reduce loss risk.
    """
    retries = 0
    max_retries = int(getattr(Config, "KAFKA_CONNECT_RETRIES", 10))
    retry_sleep = float(getattr(Config, "KAFKA_CONNECT_RETRY_SLEEP_SEC", 5))

    acks = getattr(Config, "KAFKA_PRODUCER_ACKS", 1)
    linger_ms = int(getattr(Config, "KAFKA_PRODUCER_LINGER_MS", 5))
    batch_size = int(getattr(Config, "KAFKA_PRODUCER_BATCH_SIZE", 64 * 1024))

    while retries < max_retries:
        try:
            producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                acks=acks,
                linger_ms=linger_ms,
                batch_size=batch_size,
                max_in_flight_requests_per_connection=5,
                retries=5,
                request_timeout_ms=30_000,
            )
            logger.debug(
                f"🔵 Kafka producer connected | acks={acks} linger_ms={linger_ms} batch_size={batch_size}",
                "blue",
            )
            return producer
        except (NoBrokersAvailable, KafkaError) as e:
            retries += 1
            logger.debug(f"🟡 Kafka producer error: {e} | retry in {retry_sleep}s ({retries}/{max_retries})", "yellow")
            sleep(retry_sleep)

    raise RuntimeError(f"Failed to connect to Kafka producer after {max_retries} retries.")


# ----------------------------------------------------------
# Producer send helpers
# ----------------------------------------------------------

def _send_async(producer: KafkaProducer, topic: str, payload: dict, *, label: str):
    """
    Async send (batched). Returns kafka-python FutureRecordMetadata.
    IMPORTANT: this does NOT guarantee delivery; it's only queued in the producer.
    """
    mid = payload.get("id")
    fut = producer.send(topic, payload)
    logger.debug(f"🟢 [OUT] {label} topic={topic} id={mid}", "green")
    return fut


def _send(producer: KafkaProducer, topic: str, payload: dict, *, label: str) -> bool:
    """Backward-compatible wrapper: async fire-and-forget."""
    try:
        _send_async(producer, topic, payload, label=label)
        return True
    except Exception as e:
        logger.debug(f"🟡 [OUT FAIL] {label} topic={topic} id={payload.get('id')} err={e}", "yellow")
        return False


def _send_sync(producer: KafkaProducer, topic: str, payload: dict, *, label: str, timeout_sec: float = 10.0) -> bool:
    """
    Sync send (wait for broker ack). Use for retry/DLQ paths to reduce message loss
    before committing input offsets.
    """
    mid = payload.get("id")
    try:
        fut = producer.send(topic, payload)
        fut.get(timeout=timeout_sec)
        logger.debug(f"🟢 [OUT SYNC] {label} topic={topic} id={mid}", "green")
        return True
    except Exception as e:
        logger.debug(f"🟡 [OUT SYNC FAIL] {label} topic={topic} id={mid} err={e}", "yellow")
        return False


def _wait_futures(futures: List, *, timeout_sec: float) -> bool:
    """
    Wait for all kafka futures (ACK-gating). If any fails -> return False.
    This is the key correctness improvement for bulk mode.
    """
    ok = True
    deadline = time.time() + max(0.1, float(timeout_sec))
    for fut in futures:
        remaining = max(0.1, deadline - time.time())
        try:
            fut.get(timeout=remaining)
        except Exception as e:
            ok = False
            logger.debug(f"🟡 [OUT ACK FAIL] err={e}", "yellow")
    return ok


def _commit_offsets(consumer: KafkaConsumer, offsets: Dict[TopicPartition, int]) -> None:
    if OffsetAndMetadata is None or not offsets:
        return
    try:
        with consumer_lock:
            consumer.commit(offsets={tp: OffsetAndMetadata(off, None) for tp, off in offsets.items()})
    except Exception as e:
        logger.debug(f"🟡 [COMMIT FAIL] err={e}", "yellow")


# ==========================================================
# Worker pools
# ==========================================================

@dataclass
class WorkerPool:
    name: str
    max_workers: int
    executor: ThreadPoolExecutor
    lock: threading.Lock
    active: Set  # futures
    _was_full: bool = False

    @classmethod
    def create(cls, name: str, max_workers: int) -> "WorkerPool":
        return cls(
            name=name,
            max_workers=max_workers,
            executor=ThreadPoolExecutor(max_workers=max_workers),
            lock=threading.Lock(),
            active=set(),
        )

    def free_slots(self) -> int:
        with self.lock:
            done = {f for f in self.active if f.done()}
            freed = len(done)
            if done:
                self.active.difference_update(done)

            active_now = len(self.active)
            maxw = self.max_workers
            full = active_now >= maxw

            if full and not self._was_full:
                logger.debug(f"🟡 [POOL FULL] worker={self.name} active={active_now}/{maxw}", "yellow")
                self._was_full = True

            if freed > 0:
                logger.debug(
                    f"🟢 [POOL FREED] worker={self.name} active={active_now}/{maxw} freed={freed}",
                    "green",
                )

            if (not full) and self._was_full:
                self._was_full = False

            return max(0, maxw - active_now)

    def submit(self, fn, *args, **kwargs) -> None:
        with self.lock:
            fut = self.executor.submit(fn, *args, **kwargs)
            self.active.add(fut)


# ==========================================================
# Policies runtime
# ==========================================================

@dataclass
class WorkerState:
    name: str
    lock: threading.Lock
    consecutive_failures: int = 0
    open_until_ts: float = 0.0
    tokens: float = 0.0
    last_refill_ts: float = 0.0


def cb_is_open(state: WorkerState) -> bool:
    return time.time() < state.open_until_ts


def cb_on_success(state: WorkerState) -> None:
    with state.lock:
        state.consecutive_failures = 0


def cb_on_failure(state: WorkerState, cfg: CircuitBreakerConfig) -> None:
    now = time.time()
    with state.lock:
        if now < state.open_until_ts:
            return
        state.consecutive_failures += 1
        if state.consecutive_failures >= cfg.failures:
            state.open_until_ts = now + float(cfg.reset_sec)
            state.consecutive_failures = 0
            logger.debug(f"🟣 [CB OPEN] worker={state.name} open_for={cfg.reset_sec}s", "magenta")


def rl_init(state: WorkerState, cfg: RateLimitConfig) -> None:
    now = time.time()
    with state.lock:
        if state.last_refill_ts == 0.0:
            state.last_refill_ts = now
            state.tokens = float(cfg.burst)


def rl_try_take(state: WorkerState, cfg: RateLimitConfig, amount: float = 1.0) -> bool:
    now = time.time()
    with state.lock:
        if state.last_refill_ts == 0.0:
            state.last_refill_ts = now
            state.tokens = float(cfg.burst)

        elapsed = now - state.last_refill_ts
        if elapsed > 0:
            state.tokens = min(float(cfg.burst), state.tokens + elapsed * float(cfg.rps))
            state.last_refill_ts = now

        if state.tokens >= amount:
            state.tokens -= amount
            return True
        return False


# ==========================================================
# Retry-to-DLQ runtime
# ==========================================================

def _handle_retry_to_dlq(
    producer: KafkaProducer,
    spec: WorkerSpec,
    original_topic: str,
    message_value: dict,
    *,
    label_prefix: str,
) -> bool:
    """
    Return True if failure was successfully handled (requeued or DLQed),
    so input offset may be committed.
    Return False if we failed to requeue/DLQ -> do NOT commit offset.
    """
    cfg: Optional[RetryToDlqConfig] = spec.retry_to_dlq
    if not cfg:
        # No retry policy: send to global error topic and commit offset (best-effort, avoid infinite loop)
        _send_sync(producer, Config.ERROR_TOPIC, message_value, label=f"{label_prefix} no_retry_cfg")
        return True

    field = cfg.retry_count_field or "retry_count"
    cur = message_value.get(field, 0)
    try:
        cur_i = int(cur)
    except Exception:
        cur_i = 0
    next_i = cur_i + 1
    message_value[field] = next_i

    if next_i < int(cfg.max_attempts):
        ok = _send_sync(producer, original_topic, message_value, label=f"{label_prefix} retry {next_i}/{cfg.max_attempts}")
        return ok

    ok = _send_sync(producer, cfg.dlq_topic, message_value, label=f"{label_prefix} DLQ {next_i}/{cfg.max_attempts}")
    return ok


# ==========================================================
# Aggregation (Redis Lua)
# ==========================================================

_AGG_LUA = r"""
redis.call('HSET', KEYS[1], ARGV[1], ARGV[2])
redis.call('EXPIRE', KEYS[1], tonumber(ARGV[3]))
local n = redis.call('HLEN', KEYS[1])
if n == tonumber(ARGV[4]) then
  local all = redis.call('HGETALL', KEYS[1])
  return all
end
return {}
"""


def _aggregate_merge(messages: List[dict]) -> dict:
    out: dict = {}
    for m in messages:
        out = deep_merge(out, m)
    return out


# ==========================================================
# Commit coordinator (after_success)
# ==========================================================

class CommitCoordinator:
    """
    Commits offsets only after successful processing, and in-order per TopicPartition.

    IMPORTANT:
    - init_tp() must be called at DISPATCH time with the first offset being dispatched,
      before any worker thread calls mark_done(). This ensures next_commit starts at the
      correct (lowest) offset rather than whichever offset happens to finish first.
    - mark_done() is called by worker threads when a job completes.
    - try_commit() is tick-batched: it only flushes to the broker every commit_tick_sec
      to avoid per-message round-trips under high throughput.
    """

    def __init__(self, consumer: KafkaConsumer, commit_tick_sec: float = 0.2):
        self.consumer = consumer
        self.lock = threading.Lock()
        self.next_commit: Dict[TopicPartition, int] = {}
        self.done: Dict[TopicPartition, Set[int]] = {}
        self._commit_tick_sec = commit_tick_sec
        self._last_commit_ts: float = 0.0

    def init_tp(self, tp: TopicPartition, first_offset: int) -> None:
        """Record the first dispatched offset for a TP before any worker thread runs."""
        with self.lock:
            if tp not in self.next_commit:
                self.next_commit[tp] = first_offset
                self.done[tp] = set()

    def mark_done(self, tp: TopicPartition, offset: int) -> None:
        with self.lock:
            if tp not in self.next_commit:
                # Fallback: init not called before mark_done (shouldn't happen normally)
                self.next_commit[tp] = offset
                self.done[tp] = set()
            self.done[tp].add(offset)

    def try_commit(self, *, force: bool = False) -> None:
        if OffsetAndMetadata is None:
            return
        now = time.time()
        if not force and (now - self._last_commit_ts) < self._commit_tick_sec:
            return  # batch commits: skip if called too frequently
        self._last_commit_ts = now
        with self.lock:
            updates: Dict[TopicPartition, int] = {}
            for tp, nxt in list(self.next_commit.items()):
                original_nxt = nxt
                done_set = self.done.get(tp, set())
                while nxt in done_set:
                    done_set.remove(nxt)
                    nxt += 1
                self.next_commit[tp] = nxt
                if nxt > original_nxt:  # only commit if we actually advanced
                    updates[tp] = nxt
        if updates:
            _commit_offsets(self.consumer, updates)


# ==========================================================
# Worker runners
# ==========================================================

def _forward_result(
    producer: KafkaProducer,
    spec: WorkerSpec,
    result,
    *,
    wait_for_acks: bool,
    ack_timeout_sec: float,
) -> bool:
    """
    Forward worker results to output topics.

    If wait_for_acks=True:
      - collect producer futures
      - wait for broker ACKs before returning True
      - if any send fails -> return False (so we DON'T commit input offsets)
    """
    if result is None:
        return True  # worker filtered this message intentionally — no output, offset can be committed

    futures = []
    try:
        if isinstance(result, list):
            for item in result:
                if item is None:
                    continue
                for t in spec.topics_out:
                    futures.append(_send_async(producer, t, item, label=f"worker={spec.name}"))
        else:
            for t in spec.topics_out:
                futures.append(_send_async(producer, t, result, label=f"worker={spec.name}"))
    except Exception as e:
        logger.debug(f"🟡 [OUT FAIL] worker={spec.name} err={e}", "yellow")
        return False

    if not wait_for_acks:
        return True

    return _wait_futures(futures, timeout_sec=ack_timeout_sec)


def _run_handler_single(
    spec: WorkerSpec,
    producer: KafkaProducer,
    consumer_name: str,
    tp: TopicPartition,
    offset: int,
    message_value: dict,
    state: WorkerState,
    *,
    output_sync: bool,
    output_sync_timeout_sec: float,
) -> bool:
    mid = message_value.get("id")
    try:
        result = spec.fn(message_value, consumer_name, dict(spec.metadatas))
        ok = _forward_result(
            producer,
            spec,
            result,
            wait_for_acks=output_sync,
            ack_timeout_sec=output_sync_timeout_sec,
        )
        if ok and spec.circuit_breaker:
            cb_on_success(state)
        return ok
    except Exception as e:
        logger.debug(f"🟡 [HANDLER EXC] worker={spec.name} tp={_tp(tp)} off={offset} id={mid} err={e}", "yellow")
        if spec.circuit_breaker:
            cb_on_failure(state, spec.circuit_breaker)

        handled = _handle_retry_to_dlq(
            producer,
            spec,
            tp.topic,
            message_value,
            label_prefix=f"handler_exc worker={spec.name}",
        )
        return handled


def _run_handler_bulk(
    spec: WorkerSpec,
    producer: KafkaProducer,
    consumer_name: str,
    tp: TopicPartition,
    offsets: List[int],
    batch: List[dict],
    state: WorkerState,
    *,
    output_sync: bool,
    output_sync_timeout_sec: float,
) -> bool:
    try:
        result = spec.fn(batch, consumer_name, dict(spec.metadatas))
        ok = _forward_result(
            producer,
            spec,
            result,
            wait_for_acks=output_sync,
            ack_timeout_sec=output_sync_timeout_sec,
        )
        if ok and spec.circuit_breaker:
            cb_on_success(state)
        return ok
    except Exception as e:
        logger.debug(f"🟡 [BULK EXC] worker={spec.name} tp={_tp(tp)} batch={len(batch)} err={e}", "yellow")
        if spec.circuit_breaker:
            cb_on_failure(state, spec.circuit_breaker)

        all_handled = True
        for msg in batch:
            handled = _handle_retry_to_dlq(
                producer,
                spec,
                tp.topic,
                msg,
                label_prefix=f"bulk_exc worker={spec.name}",
            )
            all_handled = all_handled and handled

        return all_handled


def _run_aggregator(
    spec: WorkerSpec,
    producer: KafkaProducer,
    consumer_name: str,
    tp: TopicPartition,
    offset: int,
    message_value: dict,
    redis_client,
    agg_sha: str,
    state: WorkerState,
    *,
    output_sync: bool,
    output_sync_timeout_sec: float,
) -> bool:
    mid = message_value.get("id")
    agg_key_val = compute_aggregate_key(spec, message_value)
    agg_key = f"agg:{spec.name}:{agg_key_val}"

    try:
        try:
            res = redis_client.evalsha(
                agg_sha,
                1,
                agg_key,
                tp.topic,
                json.dumps(message_value),
                str(spec.aggregator_timeout_sec),
                str(len(spec.topics_in)),
            )
        except redis_lib.exceptions.ResponseError as _e:
            if "NOSCRIPT" not in str(_e):
                raise
            # Redis was restarted; script evicted — fall back to eval() which re-sends the script body
            logger.debug("🟡 [AGG] NOSCRIPT: falling back to eval()", "yellow")
            res = redis_client.eval(
                _AGG_LUA,
                1,
                agg_key,
                tp.topic,
                json.dumps(message_value),
                str(spec.aggregator_timeout_sec),
                str(len(spec.topics_in)),
            )

        if not res:
            return True

        msgs: List[dict] = []
        for i in range(0, len(res), 2):
            payload_json = res[i + 1]
            if isinstance(payload_json, bytes):
                payload_json = payload_json.decode("utf-8", errors="ignore")
            try:
                msgs.append(json.loads(payload_json))
            except Exception:
                continue

        if len(msgs) < len(spec.topics_in):
            return True

        aggregated = _aggregate_merge(msgs)
        result = spec.fn(aggregated, consumer_name, dict(spec.metadatas))
        ok = _forward_result(
            producer,
            spec,
            result,
            wait_for_acks=output_sync,
            ack_timeout_sec=output_sync_timeout_sec,
        )

        if ok:
            try:
                redis_client.delete(agg_key)
            except Exception:
                pass
            if spec.circuit_breaker:
                cb_on_success(state)

        return ok

    except Exception as e:
        logger.debug(f"🟡 [AGG EXC] worker={spec.name} tp={_tp(tp)} off={offset} id={mid} err={e}", "yellow")
        if spec.circuit_breaker:
            cb_on_failure(state, spec.circuit_breaker)

        handled = _handle_retry_to_dlq(
            producer,
            spec,
            tp.topic,
            message_value,
            label_prefix=f"agg_exc worker={spec.name}",
        )
        return handled


# ==========================================================
# Pending queues (bounded, per TopicPartition)
# ==========================================================

@dataclass
class PendingItem:
    offset: int
    raw: str
    ts: float  # enqueue time (strict batching)


# ==========================================================
# Main loop
# ==========================================================

def _validate_config() -> None:
    """Validate required config keys and basic numeric ranges at startup."""
    errors = []
    for key in ("WORKER_NAME", "ERROR_TOPIC"):
        if not getattr(Config, key, None):
            errors.append(f"Config.{key} is required but not set")
    for key, min_val in (
        ("KAFKA_POLL_TIMEOUT_MS", 1),
        ("KAFKA_POLL_MAX_RECORDS", 1),
        ("KAFKA_PENDING_MAX_PER_TP", 1),
    ):
        val = getattr(Config, key, None)
        if val is not None:
            try:
                if int(val) < min_val:
                    errors.append(f"Config.{key}={val} must be >= {min_val}")
            except (ValueError, TypeError):
                errors.append(f"Config.{key}={val!r} must be a valid integer")
    if errors:
        raise RuntimeError("ETL config validation failed:\n  " + "\n  ".join(errors))


def start(*, worker_modules: Iterable[str], bootstrap_servers: str, consumer_name: str = Config.WORKER_NAME) -> None:
    _validate_config()
    for mod in worker_modules:
        importlib.import_module(mod)

    topics = all_topics()
    if not topics:
        raise RuntimeError("No workers registered. Did you import the module with @kafka_handler/@kafka_aggregator?")

    workers = list(all_workers())
    pools: Dict[str, WorkerPool] = {w.name: WorkerPool.create(w.name, w.max_workers) for w in workers}
    states: Dict[str, WorkerState] = {w.name: WorkerState(name=w.name, lock=threading.Lock()) for w in workers}

    consumer = create_kafka_consumer(list(topics), bootstrap_servers, enable_auto_commit=False)
    producer = create_kafka_producer(bootstrap_servers)
    commit_tick_sec = float(getattr(Config, "KAFKA_COMMIT_TICK_SEC", 0.2))
    coordinator = CommitCoordinator(consumer, commit_tick_sec=commit_tick_sec)

    redis_client = redis_util.redis
    agg_sha = redis_client.script_load(_AGG_LUA)

    # ==========================================================
    # Output correctness knobs
    # ==========================================================
    bulk_output_sync = bool(getattr(Config, "KAFKA_BULK_OUTPUT_SYNC", True))
    output_ack_timeout_sec = float(getattr(Config, "KAFKA_OUTPUT_ACK_TIMEOUT_SEC", 30.0))

    # ==========================================================
    # Tuning knobs
    # ==========================================================
    poll_timeout_ms = int(getattr(Config, "KAFKA_POLL_TIMEOUT_MS", 20))
    idle_sleep_sec = float(getattr(Config, "KAFKA_IDLE_SLEEP_SEC", 0.03))
    max_records = int(getattr(Config, "KAFKA_POLL_MAX_RECORDS", 2000))
    pending_max_per_tp = int(getattr(Config, "KAFKA_PENDING_MAX_PER_TP", 750))
    max_jobs_per_tp_per_tick = int(getattr(Config, "KAFKA_MAX_JOBS_PER_TP_PER_TICK", 500))

    pending: Dict[TopicPartition, Deque[PendingItem]] = {}

    paused: Set[TopicPartition] = set()
    pause_reason: Dict[TopicPartition, str] = {}
    pause_until: Dict[TopicPartition, float] = {}

    # Lock during initialization poll
    with consumer_lock:
        consumer.poll(timeout_ms=0)  # prime assignment

    # Local helper functions defined inside start() maintain scope
    def _pause(tp: TopicPartition, reason: str, *, retry_in_sec: float = 0.0) -> None:
        if tp in paused and pause_reason.get(tp) == reason:
            return
        pause_reason[tp] = reason
        if retry_in_sec > 0:
            pause_until[tp] = time.time() + retry_in_sec

        with consumer_lock:  # Thread-safe pause
            consumer.pause(tp)
        paused.add(tp)
        logger.debug(f"🟡 [PAUSE] tp={_tp(tp)} reason={reason}", "yellow")

    def _resume(tp: TopicPartition, reason: str) -> None:
        if tp not in paused:
            return
        prev = pause_reason.pop(tp, None)
        pause_until.pop(tp, None)

        with consumer_lock:  # Thread-safe resume
            consumer.resume(tp)
        paused.discard(tp)
        logger.debug(f"🟣 [RESUME] tp={_tp(tp)} reason={reason} prev={prev}", "magenta")

    def _pause_and_seek_backlog(tp: TopicPartition, reason: str, first_unenqueued_offset: int) -> None:
        try:
            _pause(tp, reason)
            with consumer_lock:  # Thread-safe seek
                consumer.seek(tp, first_unenqueued_offset)
            logger.debug(
                f"🟡 [SEEK BACK] tp={_tp(tp)} reason={reason} seek_to_off={first_unenqueued_offset}",
                "yellow",
            )
        except Exception as e:
            logger.debug(
                f"🟡 [SEEK BACK FAIL] tp={_tp(tp)} reason={reason} off={first_unenqueued_offset} err={e}",
                "yellow",
            )
            _pause(tp, reason)

    def _pending_len(tp: TopicPartition) -> int:
        q = pending.get(tp)
        return len(q) if q else 0

    def _strict_batch_ready(q: Deque[PendingItem], batch_size: int, timeout_ms: int) -> bool:
        if not q:
            return False
        if len(q) >= batch_size:
            return True
        timeout_sec = max(0.001, float(timeout_ms) / 1000.0)
        oldest = q[0]
        return (time.time() - oldest.ts) >= timeout_sec

    def _drain_pending_for_tp(tp: TopicPartition, spec: WorkerSpec, pool: WorkerPool, st: WorkerState) -> None:
        q = pending.get(tp)
        if not q:
            return

        if spec.circuit_breaker and cb_is_open(st):
            _pause(tp, "circuit_breaker_open")
            return

        if pool.free_slots() <= 0:
            return

        def _take_dispatch_token() -> bool:
            if not spec.rate_limit:
                return True
            rl_init(st, spec.rate_limit)
            if rl_try_take(st, spec.rate_limit, amount=1.0):
                return True
            _pause(tp, "rate_limited", retry_in_sec=0.05)
            return False

        jobs_left = max_jobs_per_tp_per_tick

        # Aggregator
        if spec.kind == "aggregator":
            while q and pool.free_slots() > 0 and jobs_left > 0:
                if not _take_dispatch_token():
                    return

                jobs_left -= 1
                item = q.popleft()
                try:
                    val = json.loads(item.raw)
                    ensure_message_id(val)
                except Exception:
                    coordinator.mark_done(tp, item.offset)
                    coordinator.try_commit()
                    continue

                mid = val.get("id")
                logger.debug(f"🔵 [IN] tp={_tp(tp)} off={item.offset} id={mid} (pending)", "blue")
                logger.debug(f"🟣 [DISPATCH] worker={spec.name} kind=aggregator tp={_tp(tp)} off={item.offset} id={mid}",
                             "magenta")
                coordinator.init_tp(tp, item.offset)

                def _job(tp_=tp, off=item.offset, msgv=val, spec_=spec, st_=st):
                    ok = _run_aggregator(
                        spec_,
                        producer,
                        consumer_name,
                        tp_,
                        off,
                        msgv,
                        redis_client,
                        agg_sha,
                        st_,
                        output_sync=False,
                        output_sync_timeout_sec=output_ack_timeout_sec,
                    )
                    if ok:
                        coordinator.mark_done(tp_, off)
                        coordinator.try_commit()

                pool.submit(_job)
            return

        # Bulk handler
        if spec.bulk_mode:
            bs = max(2, int(spec.batch_size))
            to_ms = max(1, int(spec.batch_timeout_ms))

            if not _strict_batch_ready(q, bs, to_ms):
                return

            while q and pool.free_slots() > 0 and jobs_left > 0:
                if not _strict_batch_ready(q, bs, to_ms):
                    return

                if not _take_dispatch_token():
                    return

                jobs_left -= 1
                take_n = bs if len(q) >= bs else min(len(q), bs)
                chunk: List[PendingItem] = []
                for _ in range(take_n):
                    chunk.append(q.popleft())

                offsets: List[int] = []
                payloads: List[dict] = []
                for it in chunk:
                    try:
                        val = json.loads(it.raw)
                        ensure_message_id(val)
                        offsets.append(it.offset)
                        payloads.append(val)
                    except Exception as _parse_err:
                        logger.debug(f"🟡 [BULK PARSE ERR] tp={_tp(tp)} off={it.offset} err={_parse_err}", "yellow")
                        coordinator.mark_done(tp, it.offset)

                if not payloads:
                    coordinator.try_commit()
                    continue

                ids_preview = [p.get("id") for p in payloads[:5]]
                logger.debug(
                    f"🟣 [DISPATCH] worker={spec.name} kind=bulk tp={_tp(tp)} batch={len(payloads)} ids={ids_preview}",
                    "magenta")
                coordinator.init_tp(tp, offsets[0])

                def _job(tp_=tp, offs=offsets, payloads_=payloads, spec_=spec, st_=st):
                    ok = _run_handler_bulk(
                        spec_,
                        producer,
                        consumer_name,
                        tp_,
                        offs,
                        payloads_,
                        st_,
                        output_sync=bulk_output_sync,
                        output_sync_timeout_sec=output_ack_timeout_sec,
                    )
                    if ok:
                        for off in offs:
                            coordinator.mark_done(tp_, off)
                        coordinator.try_commit()

                pool.submit(_job)
            return

        # Single handler
        while q and pool.free_slots() > 0 and jobs_left > 0:
            if not _take_dispatch_token():
                return

            jobs_left -= 1
            item = q.popleft()
            try:
                val = json.loads(item.raw)
                ensure_message_id(val)
            except Exception as _parse_err:
                logger.debug(f"🟡 [SINGLE PARSE ERR] tp={_tp(tp)} off={item.offset} err={_parse_err}", "yellow")
                coordinator.mark_done(tp, item.offset)
                coordinator.try_commit()
                continue

            mid = val.get("id")
            logger.debug(f"🔵 [IN] tp={_tp(tp)} off={item.offset} id={mid} (pending)", "blue")
            logger.debug(f"🟣 [DISPATCH] worker={spec.name} kind=single tp={_tp(tp)} off={item.offset} id={mid}",
                         "magenta")
            coordinator.init_tp(tp, item.offset)

            def _job(tp_=tp, off=item.offset, msgv=val, spec_=spec, st_=st):
                ok = _run_handler_single(
                    spec_,
                    producer,
                    consumer_name,
                    tp_,
                    off,
                    msgv,
                    st_,
                    output_sync=False,
                    output_sync_timeout_sec=output_ack_timeout_sec,
                )
                if ok:
                    coordinator.mark_done(tp_, off)
                    coordinator.try_commit()

            pool.submit(_job)

    logger.debug(f"🔵 [{consumer_name}] ETL start | topics={list(topics)}", "blue")

    try:
        while True:
            # Added a very small sleep at the top of the loop to ensure
            # we yield control to background threads (worker threads) even during high load.
            time.sleep(0.001)

            now = time.time()

            # 1) Drain pending first + handle resumes
            assigned = consumer.assignment()
            for tp in list(assigned):
                spec = worker_for_topic(tp.topic)
                if not spec:
                    continue
                pool = pools[spec.name]
                st = states[spec.name]

                _drain_pending_for_tp(tp, spec, pool, st)

                if tp in paused:
                    r = pause_reason.get(tp)
                    if r == "circuit_breaker_open":
                        if not (spec.circuit_breaker and cb_is_open(st)):
                            _resume(tp, "circuit_breaker_closed")
                    elif r == "rate_limited":
                        if now >= pause_until.get(tp, 0.0):
                            _resume(tp, "rate_limit_tick")
                    elif r == "pending_full":
                        if _pending_len(tp) < pending_max_per_tp:
                            _resume(tp, "pending_has_room")
                    else:
                        _resume(tp, "resume_fallback")

                if _pending_len(tp) >= pending_max_per_tp and pool.free_slots() <= 0:
                    _pause(tp, "pending_full")

            # 2) Poll
            # CRITICAL: Wrapped in lock to prevent [COMMIT FAIL] collisions with background workers
            with consumer_lock:
                records = consumer.poll(timeout_ms=poll_timeout_ms, max_records=max_records)

            if not records:
                coordinator.try_commit()
                if idle_sleep_sec > 0:
                    time.sleep(idle_sleep_sec)
                continue

            # 3) Enqueue newly polled messages
            for tp, msgs in records.items():
                spec = worker_for_topic(tp.topic)
                if not spec:
                    for m in msgs:
                        coordinator.mark_done(tp, m.offset)
                    coordinator.try_commit()
                    continue

                st = states[spec.name]
                q = pending.setdefault(tp, deque())

                if spec.circuit_breaker and cb_is_open(st):
                    _pause(tp, "circuit_breaker_open")
                    first_unenqueued: Optional[int] = None
                    for m in msgs:
                        if len(q) >= pending_max_per_tp:
                            first_unenqueued = m.offset
                            break
                        q.append(PendingItem(offset=m.offset, raw=_safe_decode(m.value), ts=time.time()))
                    if first_unenqueued is not None:
                        _pause_and_seek_backlog(tp, "pending_full", first_unenqueued)
                    continue

                first_unenqueued: Optional[int] = None
                for m in msgs:
                    if len(q) >= pending_max_per_tp:
                        first_unenqueued = m.offset
                        break
                    raw = _safe_decode(m.value)
                    try:
                        val = json.loads(raw)
                        ensure_message_id(val)
                        q.append(PendingItem(offset=m.offset, raw=json.dumps(val), ts=time.time()))
                    except Exception as _parse_err:
                        coordinator.mark_done(tp, m.offset)
                        continue

                if first_unenqueued is not None:
                    _pause_and_seek_backlog(tp, "pending_full", first_unenqueued)

                _drain_pending_for_tp(tp, spec, pools[spec.name], states[spec.name])

            coordinator.try_commit()

            # Additional safety sleep at end of cycle
            if idle_sleep_sec > 0:
                time.sleep(idle_sleep_sec)

    finally:
        logger.debug("🔵 Shutting down framework...", "blue")
        for pool in pools.values():
            pool.executor.shutdown(wait=False)
        with consumer_lock:
            consumer.close()
        producer.flush(timeout=30)
        producer.close()
        logger.debug("🟢 Shutdown complete", "green")


# def start(*, worker_modules: Iterable[str], bootstrap_servers: str, consumer_name: str = Config.WORKER_NAME) -> None:
#     _validate_config()
#     for mod in worker_modules:
#         importlib.import_module(mod)
#
#     topics = all_topics()
#     if not topics:
#         raise RuntimeError("No workers registered. Did you import the module with @kafka_handler/@kafka_aggregator?")
#
#     workers = list(all_workers())
#     pools: Dict[str, WorkerPool] = {w.name: WorkerPool.create(w.name, w.max_workers) for w in workers}
#     states: Dict[str, WorkerState] = {w.name: WorkerState(name=w.name, lock=threading.Lock()) for w in workers}
#
#     consumer = create_kafka_consumer(list(topics), bootstrap_servers, enable_auto_commit=False)
#     producer = create_kafka_producer(bootstrap_servers)
#     commit_tick_sec = float(getattr(Config, "KAFKA_COMMIT_TICK_SEC", 0.2))
#     coordinator = CommitCoordinator(consumer, commit_tick_sec=commit_tick_sec)
#
#     redis_client = redis_util.redis
#     agg_sha = redis_client.script_load(_AGG_LUA)
#
#     # ==========================================================
#     # Output correctness knobs
#     # ==========================================================
#     bulk_output_sync = bool(getattr(Config, "KAFKA_BULK_OUTPUT_SYNC", True))
#     output_ack_timeout_sec = float(getattr(Config, "KAFKA_OUTPUT_ACK_TIMEOUT_SEC", 30.0))
#
#     # ==========================================================
#     # Tuning knobs
#     # ==========================================================
#     # ==========================================================
#     # Kafka ETL tuning knobs (WITH PRACTICAL COMMENTS)
#     # ==========================================================
#     # These knobs control the trade-off triangle:
#     #   ✅ latency (how fast a message starts processing after arriving)
#     #   ✅ throughput (messages/sec you can drain)
#     #   ✅ CPU usage (how much scheduler overhead you burn)
#     #
#     # Mental model:
#     #   - poll() pulls records from Kafka into your process
#     #   - you enqueue them into `pending[TopicPartition]`
#     #   - you drain pending into worker ThreadPools (per worker)
#     #
#     # If you tune poorly, you get:
#     #   - high CPU (busy scheduler) even when Kafka is quiet
#     #   - high lag (you don’t drain fast enough)
#     #   - bursty latency (bulk batching waits too long)
#     #
#     # ----------------------------------------------------------
#     # 1) poll_timeout_ms
#     # ----------------------------------------------------------
#     poll_timeout_ms = int(getattr(Config, "KAFKA_POLL_TIMEOUT_MS", 20))
#     # What it does:
#     #   - How long KafkaConsumer.poll() blocks waiting for records.
#     #   - If messages are already available, poll() returns immediately anyway.
#     #
#     # Effects:
#     #   - Lower (e.g., 10–50ms): lower "idle latency" but can increase CPU because you wake up frequently.
#     #   - Higher (e.g., 100–500ms): reduces CPU when topics are quiet; may slightly increase latency
#     #     ONLY when the consumer is idle and a message arrives right after poll() starts waiting.
#     #
#     # Key nuance:
#     #   - When topics are busy, poll() won’t wait the full timeout; it returns with records quickly.
#     #   - So in "high traffic" scenarios, increasing poll_timeout_ms does NOT meaningfully hurt throughput.
#     #
#     # Recommended:
#     #   DEV (interactive, low load):     20–100ms
#     #   PROD (steady load):             100–250ms
#     #   PROD (mostly idle topics):      250–1000ms (combined with a small idle sleep)
#     #
#     # Rule of thumb:
#     #   If you see CPU spikes while lag is low → increase poll_timeout_ms.
#
#     # ----------------------------------------------------------
#     # 2) idle_sleep_sec
#     # ----------------------------------------------------------
#     idle_sleep_sec = float(getattr(Config, "KAFKA_IDLE_SLEEP_SEC", 0.03))
#     # What it does:
#     #   - Extra sleep when poll() returns no records (records == {}).
#     #   - This is your "CPU safety valve" for idle periods.
#     #
#     # Effects:
#     #   - Higher (e.g., 50–200ms): very low CPU when idle, but higher "wake-up latency" on new messages.
#     #   - Lower (e.g., 0–10ms): faster reaction to new messages, but more CPU usage when idle.
#     #
#     # Recommended:
#     #   DEV:  0.0–0.02 (fast feedback, CPU not critical)
#     #   PROD: 0.01–0.05 (balanced)
#     #   PROD (mostly idle): 0.05–0.2 (save CPU)
#     #
#     # Practical latency math:
#     #   Worst-case added latency when topic was idle ≈ idle_sleep_sec + poll_timeout_ms/1000
#     #
#     # Example (your defaults):
#     #   0.03s + 0.02s = 0.05s worst-case idle wake-up
#     #
#     # If you want "near real-time":
#     #   - reduce idle_sleep_sec to ~0.005–0.01
#     #   - increase poll_timeout_ms to ~100–250 to avoid CPU spin
#
#     # ----------------------------------------------------------
#     # 3) max_records
#     # ----------------------------------------------------------
#     max_records = int(getattr(Config, "KAFKA_POLL_MAX_RECORDS", 2000))
#     # What it does:
#     #   - Upper bound of how many records poll() returns per call (across all partitions).
#     #   - This is a batching knob for "how big a chunk we ingest into pending per tick".
#     #
#     # Effects:
#     #   - Higher: fewer poll() calls → better throughput; but bigger bursts of JSON decode/enqueue work
#     #     in one tick → can create CPU spikes and increase per-tick latency for other partitions.
#     #   - Lower: smoother scheduling and fairer interleaving, but more poll() calls (more overhead).
#     #
#     # Common failure mode:
#     #   If max_records is huge and you have many topics/partitions,
#     #   a single poll tick may spend a lot of CPU time just decoding/enqueuing,
#     #   delaying actual draining/processing (scheduler becomes the bottleneck).
#     #
#     # Recommended:
#     #   DEV:  200–1000 (more stable behavior)
#     #   PROD: 1000–5000 (depending on message size + CPU budget)
#     #
#     # If your messages are large JSON:
#     #   keep this smaller (500–1500) to avoid "decode bursts".
#
#     # ----------------------------------------------------------
#     # 4) pending_max_per_tp
#     # ----------------------------------------------------------
#     pending_max_per_tp = int(getattr(Config, "KAFKA_PENDING_MAX_PER_TP", 750))
#     # What it does:
#     #   - Hard cap for the in-memory backlog queue PER TopicPartition.
#     #   - When pending[tp] reaches this limit:
#     #       - you pause(tp)
#     #       - you seek back to the first unenqueued offset (correctness)
#     #
#     # Why you need it:
#     #   - Prevents unbounded RAM growth.
#     #   - Applies backpressure when workers / producer can't keep up.
#     #
#     # Effects:
#     #   - Higher: more buffering, fewer pauses/seeks, higher RAM usage.
#     #   - Lower: tighter backpressure, more pause/resume activity, may reduce throughput if too low.
#     #
#     # Interaction with bulk batching:
#     #   - Bulk workers wait for batch_size or batch_timeout.
#     #   - If pending_max_per_tp is too small relative to batch_size, you risk constant pauses
#     #     and less efficient batching.
#     #
#     # Recommended:
#     #   Set based on:
#     #     pending_max_per_tp >= (batch_size * 2)  for bulk topics (to allow filling batches)
#     #   and also based on memory:
#     #     pending_max_per_tp * avg_message_bytes * partitions ~= memory footprint
#     #
#     # Example sizing:
#     #   avg msg 2KB, pending_max_per_tp=750 → ~1.5MB per partition
#     #   20 partitions → ~30MB (just raw queue payloads, plus Python overhead)
#     #
#     # DEV:
#     #   200–1000
#     # PROD:
#     #   750–5000 (depends on memory and message size)
#     #
#     # If you see tons of logs:
#     #   [PAUSE] reason=pending_full and [SEEK BACK]
#     #   → increase pending_max_per_tp OR reduce scheduling burst (max_records/max_jobs)
#
#     # ----------------------------------------------------------
#     # 5) max_jobs_per_tp_per_tick
#     # ----------------------------------------------------------
#     max_jobs_per_tp_per_tick = int(getattr(Config, "KAFKA_MAX_JOBS_PER_TP_PER_TICK", 500))
#     # What it does:
#     #   - Upper bound on how many jobs you dispatch from one TopicPartition "in one scheduler tick".
#     #   - It limits how aggressively you drain pending into a worker pool.
#     #
#     # Why it exists:
#     #   - Fairness: prevents one hot partition from hogging the entire tick.
#     #   - CPU control: dispatching jobs has overhead (JSON decode, token checks, queue ops).
#     #
#     # Effects:
#     #   - Higher: drains faster (lower lag), but can spike CPU and starve other partitions.
#     #   - Lower: smoother CPU, better fairness, but can increase lag on hot partitions.
#     #
#     # Interaction with rate_limit:
#     #   - Your rate_limit now takes a token PER DISPATCHED job (correct).
#     #   - If max_jobs_per_tp_per_tick is huge, you’ll still attempt many dispatch iterations
#     #     (even if they quickly pause due to rate limit), increasing scheduler overhead.
#     #
#     # Recommended:
#     #   DEV:  50–200
#     #   PROD: 200–1000 (depending on cores + number of partitions)
#     #
#     # If you see high CPU with lag still high:
#     #   - you might be hitting downstream bottlenecks (producer acks, Redis, external APIs),
#     #     so increasing this further won't help; it just burns CPU.
#     #
#     # If you see one partition lagging while others are fine:
#     #   - increasing this can help, but only if worker pool has free slots and downstream can keep up.
#
#     # ==========================================================
#     # Practical configuration profiles (copy-paste presets)
#     # ==========================================================
#
#     # --- DEV profile (fast feedback, acceptable CPU) ---
#     # KAFKA_POLL_TIMEOUT_MS = 50
#     # KAFKA_IDLE_SLEEP_SEC = 0.01
#     # KAFKA_POLL_MAX_RECORDS = 1000
#     # KAFKA_PENDING_MAX_PER_TP = 500
#     # KAFKA_MAX_JOBS_PER_TP_PER_TICK = 150
#
#     # --- PROD balanced (good throughput, controlled CPU) ---
#     # KAFKA_POLL_TIMEOUT_MS = 200
#     # KAFKA_IDLE_SLEEP_SEC = 0.02
#     # KAFKA_POLL_MAX_RECORDS = 2000
#     # KAFKA_PENDING_MAX_PER_TP = 1500
#     # KAFKA_MAX_JOBS_PER_TP_PER_TICK = 300
#
#     # --- PROD low-latency (more CPU, quicker reaction when idle) ---
#     # KAFKA_POLL_TIMEOUT_MS = 100
#     # KAFKA_IDLE_SLEEP_SEC = 0.005
#     # KAFKA_POLL_MAX_RECORDS = 2000
#     # KAFKA_PENDING_MAX_PER_TP = 1500
#     # KAFKA_MAX_JOBS_PER_TP_PER_TICK = 500
#
#     # --- PROD mostly idle topics (min CPU, higher wake-up latency) ---
#     # KAFKA_POLL_TIMEOUT_MS = 500
#     # KAFKA_IDLE_SLEEP_SEC = 0.1
#     # KAFKA_POLL_MAX_RECORDS = 500
#     # KAFKA_PENDING_MAX_PER_TP = 500
#     # KAFKA_MAX_JOBS_PER_TP_PER_TICK = 100
#
#     pending: Dict[TopicPartition, Deque[PendingItem]] = {}
#
#     paused: Set[TopicPartition] = set()
#     pause_reason: Dict[TopicPartition, str] = {}
#     pause_until: Dict[TopicPartition, float] = {}
#
#     consumer.poll(timeout_ms=0)  # prime assignment
#
#     def _pause(tp: TopicPartition, reason: str, *, retry_in_sec: float = 0.0) -> None:
#         if tp in paused and pause_reason.get(tp) == reason:
#             return
#         pause_reason[tp] = reason
#         if retry_in_sec > 0:
#             pause_until[tp] = time.time() + retry_in_sec
#         consumer.pause(tp)
#         paused.add(tp)
#         logger.debug(f"🟡 [PAUSE] tp={_tp(tp)} reason={reason}", "yellow")
#
#     def _resume(tp: TopicPartition, reason: str) -> None:
#         if tp not in paused:
#             return
#         prev = pause_reason.pop(tp, None)
#         pause_until.pop(tp, None)
#         consumer.resume(tp)
#         paused.discard(tp)
#         logger.debug(f"🟣 [RESUME] tp={_tp(tp)} reason={reason} prev={prev}", "magenta")
#
#     def _pause_and_seek_backlog(tp: TopicPartition, reason: str, first_unenqueued_offset: int) -> None:
#         """
#         Correctness:
#         If we can't enqueue all polled records (pending full), rewind to the first unenqueued offset.
#         """
#         try:
#             _pause(tp, reason)
#             consumer.seek(tp, first_unenqueued_offset)
#             logger.debug(
#                 f"🟡 [SEEK BACK] tp={_tp(tp)} reason={reason} seek_to_off={first_unenqueued_offset}",
#                 "yellow",
#             )
#         except Exception as e:
#             logger.debug(
#                 f"🟡 [SEEK BACK FAIL] tp={_tp(tp)} reason={reason} off={first_unenqueued_offset} err={e}",
#                 "yellow",
#             )
#             _pause(tp, reason)
#
#     def _pending_len(tp: TopicPartition) -> int:
#         q = pending.get(tp)
#         return len(q) if q else 0
#
#     def _strict_batch_ready(q: Deque[PendingItem], batch_size: int, timeout_ms: int) -> bool:
#         """
#         STRICT batching readiness rule:
#           - if queue >= batch_size -> ready
#           - else if oldest item age >= timeout -> ready
#         Evaluated on every tick (time-driven).
#         """
#         if not q:
#             return False
#         if len(q) >= batch_size:
#             return True
#         timeout_sec = max(0.001, float(timeout_ms) / 1000.0)
#         oldest = q[0]
#         return (time.time() - oldest.ts) >= timeout_sec
#
#     def _drain_pending_for_tp(tp: TopicPartition, spec: WorkerSpec, pool: WorkerPool, st: WorkerState) -> None:
#         q = pending.get(tp)
#         if not q:
#             return
#
#         # Circuit breaker open: keep paused (no drain)
#         if spec.circuit_breaker and cb_is_open(st):
#             _pause(tp, "circuit_breaker_open")
#             return
#
#         # If pool has no room, don't drain
#         if pool.free_slots() <= 0:
#             return
#
#         # ------------------------------------------------------------------
#         # FIX: rate-limit MUST be applied per DISPATCHED JOB (not per tick).
#         # Otherwise a single token lets you submit hundreds of jobs, then
#         # you wedge pools/producer and appear "stuck".
#         # ------------------------------------------------------------------
#         def _take_dispatch_token() -> bool:
#             if not spec.rate_limit:
#                 return True
#             rl_init(st, spec.rate_limit)
#             if rl_try_take(st, spec.rate_limit, amount=1.0):
#                 return True
#             _pause(tp, "rate_limited", retry_in_sec=0.05)
#             return False
#
#         jobs_left = max_jobs_per_tp_per_tick
#
#         # Aggregator
#         if spec.kind == "aggregator":
#             while q and pool.free_slots() > 0 and jobs_left > 0:
#                 if not _take_dispatch_token():
#                     return
#
#                 jobs_left -= 1
#                 item = q.popleft()
#                 try:
#                     val = json.loads(item.raw)
#                     ensure_message_id(val)
#                 except Exception:
#                     coordinator.mark_done(tp, item.offset)
#                     coordinator.try_commit()
#                     continue
#
#                 mid = val.get("id")
#                 logger.debug(f"🔵 [IN] tp={_tp(tp)} off={item.offset} id={mid} (pending)", "blue")
#                 logger.debug(f"🟣 [DISPATCH] worker={spec.name} kind=aggregator tp={_tp(tp)} off={item.offset} id={mid}", "magenta")
#                 coordinator.init_tp(tp, item.offset)
#
#                 def _job(tp_=tp, off=item.offset, msgv=val, spec_=spec, st_=st):
#                     ok = _run_aggregator(
#                         spec_,
#                         producer,
#                         consumer_name,
#                         tp_,
#                         off,
#                         msgv,
#                         redis_client,
#                         agg_sha,
#                         st_,
#                         output_sync=False,  # keep async by default for aggregators
#                         output_sync_timeout_sec=output_ack_timeout_sec,
#                     )
#                     if ok:
#                         coordinator.mark_done(tp_, off)
#                         coordinator.try_commit()
#
#                 pool.submit(_job)
#             return
#
#         # Bulk handler (strict batching)
#         if spec.bulk_mode:
#             bs = max(2, int(spec.batch_size))
#             to_ms = max(1, int(spec.batch_timeout_ms))
#
#             if not _strict_batch_ready(q, bs, to_ms):
#                 return
#
#             while q and pool.free_slots() > 0 and jobs_left > 0:
#                 if not _strict_batch_ready(q, bs, to_ms):
#                     return
#
#                 if not _take_dispatch_token():
#                     return
#
#                 jobs_left -= 1
#
#                 take_n = bs if len(q) >= bs else min(len(q), bs)
#
#                 chunk: List[PendingItem] = []
#                 for _ in range(take_n):
#                     chunk.append(q.popleft())
#
#                 offsets: List[int] = []
#                 payloads: List[dict] = []
#                 for it in chunk:
#                     try:
#                         val = json.loads(it.raw)
#                         ensure_message_id(val)
#                         offsets.append(it.offset)
#                         payloads.append(val)
#                     except Exception as _parse_err:
#                         logger.debug(
#                             f"🟡 [BULK PARSE ERR] tp={_tp(tp)} off={it.offset} err={_parse_err} raw={it.raw[:200]}",
#                             "yellow",
#                         )
#                         coordinator.mark_done(tp, it.offset)
#
#                 if not payloads:
#                     logger.debug(
#                         f"🟡 [BULK SKIP] tp={_tp(tp)} entire batch of {len(chunk)} failed JSON parse",
#                         "yellow",
#                     )
#                     coordinator.try_commit()
#                     continue
#
#                 ids_preview = [p.get("id") for p in payloads[:5]]
#                 logger.debug(
#                     f"🟣 [DISPATCH] worker={spec.name} kind=bulk tp={_tp(tp)} batch={len(payloads)} "
#                     f"ids={ids_preview} (strict bs={bs} timeout_ms={to_ms})",
#                     "magenta",
#                 )
#                 coordinator.init_tp(tp, offsets[0])
#
#                 def _job(tp_=tp, offs=offsets, payloads_=payloads, spec_=spec, st_=st):
#                     ok = _run_handler_bulk(
#                         spec_,
#                         producer,
#                         consumer_name,
#                         tp_,
#                         offs,
#                         payloads_,
#                         st_,
#                         output_sync=bulk_output_sync,
#                         output_sync_timeout_sec=output_ack_timeout_sec,
#                     )
#                     if ok:
#                         for off in offs:
#                             coordinator.mark_done(tp_, off)
#                         coordinator.try_commit()
#                     else:
#                         logger.debug(
#                             f"🟡 [BULK NOT COMMITTED] worker={spec_.name} tp={_tp(tp_)} batch={len(payloads_)}",
#                             "yellow",
#                         )
#
#                 pool.submit(_job)
#             return
#
#         # Single handler
#         while q and pool.free_slots() > 0 and jobs_left > 0:
#             if not _take_dispatch_token():
#                 return
#
#             jobs_left -= 1
#             item = q.popleft()
#             try:
#                 val = json.loads(item.raw)
#                 ensure_message_id(val)
#             except Exception as _parse_err:
#                 logger.debug(
#                     f"🟡 [SINGLE PARSE ERR] tp={_tp(tp)} off={item.offset} err={_parse_err} raw={item.raw[:200]}",
#                     "yellow",
#                 )
#                 coordinator.mark_done(tp, item.offset)
#                 coordinator.try_commit()
#                 continue
#
#             mid = val.get("id")
#             logger.debug(f"🔵 [IN] tp={_tp(tp)} off={item.offset} id={mid} (pending)", "blue")
#             logger.debug(f"🟣 [DISPATCH] worker={spec.name} kind=single tp={_tp(tp)} off={item.offset} id={mid}", "magenta")
#             coordinator.init_tp(tp, item.offset)
#
#             def _job(tp_=tp, off=item.offset, msgv=val, spec_=spec, st_=st):
#                 ok = _run_handler_single(
#                     spec_,
#                     producer,
#                     consumer_name,
#                     tp_,
#                     off,
#                     msgv,
#                     st_,
#                     output_sync=False,  # keep async for throughput
#                     output_sync_timeout_sec=output_ack_timeout_sec,
#                 )
#                 if ok:
#                     coordinator.mark_done(tp_, off)
#                     coordinator.try_commit()
#
#             pool.submit(_job)
#
#     logger.debug(f"🔵 [{consumer_name}] ETL start | topics={list(topics)}", "blue")
#
#     try:
#         while True:
#             now = time.time()
#
#             # 1) Drain pending first + handle resumes
#             assigned = consumer.assignment()
#             for tp in list(assigned):
#                 spec = worker_for_topic(tp.topic)
#                 if not spec:
#                     continue
#                 pool = pools[spec.name]
#                 st = states[spec.name]
#
#                 _drain_pending_for_tp(tp, spec, pool, st)
#
#                 if tp in paused:
#                     r = pause_reason.get(tp)
#
#                     if r == "circuit_breaker_open":
#                         if spec.circuit_breaker and cb_is_open(st):
#                             continue
#                         _resume(tp, "circuit_breaker_closed")
#                         continue
#
#                     if r == "rate_limited":
#                         until = pause_until.get(tp, 0.0)
#                         if now < until:
#                             continue
#                         # We don't "pre-take" here anymore; just allow drain() to take per job
#                         _resume(tp, "rate_limit_tick")
#                         continue
#
#                     if r == "pending_full":
#                         if _pending_len(tp) < pending_max_per_tp:
#                             _resume(tp, "pending_has_room")
#                         continue
#
#                     _resume(tp, "resume_fallback")
#
#                 if _pending_len(tp) >= pending_max_per_tp and pool.free_slots() <= 0:
#                     _pause(tp, "pending_full")
#
#             # 2) Poll
#             records = consumer.poll(timeout_ms=poll_timeout_ms, max_records=max_records)
#             if not records:
#                 coordinator.try_commit()
#                 if idle_sleep_sec > 0:
#                     time.sleep(idle_sleep_sec)
#                 continue
#
#             # 3) Enqueue newly polled messages
#             for tp, msgs in records.items():
#                 spec = worker_for_topic(tp.topic)
#                 if not spec:
#                     for m in msgs:
#                         raw = _safe_decode(m.value)
#                         try:
#                             val = json.loads(raw)
#                             ensure_message_id(val)
#                             _send(producer, Config.ERROR_TOPIC, val, label="no_spec")
#                         except Exception:
#                             pass
#                         coordinator.mark_done(tp, m.offset)
#                     coordinator.try_commit()
#                     continue
#
#                 st = states[spec.name]
#                 q = pending.setdefault(tp, deque())
#
#                 # CB open: pause and enqueue; if queue fills, SEEK BACK the remainder
#                 if spec.circuit_breaker and cb_is_open(st):
#                     _pause(tp, "circuit_breaker_open")
#
#                     first_unenqueued: Optional[int] = None
#                     for m in msgs:
#                         if len(q) >= pending_max_per_tp:
#                             first_unenqueued = m.offset
#                             break
#                         q.append(PendingItem(offset=m.offset, raw=_safe_decode(m.value), ts=time.time()))
#
#                     if first_unenqueued is not None:
#                         _pause_and_seek_backlog(tp, "pending_full", first_unenqueued)
#
#                     continue
#
#                 first_unenqueued: Optional[int] = None
#
#                 for m in msgs:
#                     if len(q) >= pending_max_per_tp:
#                         first_unenqueued = m.offset
#                         break
#
#                     raw = _safe_decode(m.value)
#                     try:
#                         val = json.loads(raw)
#                         ensure_message_id(val)
#                         raw = json.dumps(val)
#                     except Exception as _parse_err:
#                         logger.debug(
#                             f"🟡 [PARSE ERR] tp={_tp(tp)} off={m.offset} err={_parse_err} raw={raw[:200]}",
#                             "yellow",
#                         )
#                         coordinator.mark_done(tp, m.offset)
#                         continue
#
#                     mid = val.get("id")
#                     logger.debug(f"🔵 [IN] tp={_tp(tp)} off={m.offset} id={mid}", "blue")
#
#                     q.append(PendingItem(offset=m.offset, raw=raw, ts=time.time()))
#
#                 if first_unenqueued is not None:
#                     _pause_and_seek_backlog(tp, "pending_full", first_unenqueued)
#
#                 pool = pools[spec.name]
#                 _drain_pending_for_tp(tp, spec, pool, st)
#
#             coordinator.try_commit()
#             if idle_sleep_sec > 0:
#                 time.sleep(idle_sleep_sec)
#
#     finally:
#         try:
#             logger.debug("🔵 Shutting down thread pools...", "blue")
#             for pool in pools.values():
#                 try:
#                     pool.executor.shutdown(wait=False)
#                 except Exception:
#                     pass
#         except Exception:
#             pass
#         try:
#             logger.debug("🔵 Shutting down consumer...", "blue")
#             consumer.close()
#         except Exception:
#             pass
#         try:
#             logger.debug("🔵 Shutting down producer...", "blue")
#             producer.flush(timeout=30)
#             producer.close()
#         except Exception:
#             pass
#         logger.debug("🟢 Shutdown complete", "green")
