# Changelog

All notable changes to the QF Framework and PoC application.

---

## [v6.6.0] — 2026-03-15

### Full PoC expansion — API service layer, Redis caching, Kafka HTTP demo, Jaeger tracing

#### Core framework changes

**`src/framework/tracing/tracing.py`** (rewritten)

- Added `NoOpSpan` — silent stand-in implementing the full OTel Span interface (set_attribute, record_exception, set_status, add_event, is_recording, end, context-manager).
- Added `NoOpTracer` — implements `start_as_current_span()` (contextmanager) and `start_span()`, yields `NoOpSpan`, zero I/O.
- `init_tracing()` now returns immediately with a `NoOpTracer` when `ENABLE_TRACING=false` (default) — no OTel SDK objects are created.
- `get_tracer()` lazily initialises based on `ENABLE_TRACING` env var; safe to call before `init_tracing()`.
- `_tracing_enabled()` helper reads `ENABLE_TRACING` env var (accepts `1/true/yes/y/on`).

**`src/framework/tracing/__init__.py`** — now exports `NoOpTracer` and `NoOpSpan`.

**`src/framework/auth/`** — deleted entirely (keys.py, token.py, utils.py, __init__.py). Security is now the responsibility of the deployment environment (API gateway, mTLS). No remaining imports in framework or PoC.

**`src/framework/api/server.py`**

- Removed `TOKEN_URL` and `JWT_ALGORITHM` auth config references.
- Request/response logging middleware now controlled by `LOG_ENDPOINTS=true` env var (off by default).

#### PoC service layer (new)

**`poc_app/src/service/__init__.py`** — package marker.

**`poc_app/src/service/api_handler.py`** (new)

Service layer between HTTP boundary (`api_endpoints.py`) and workers:

- `cached_enrich(worker_fn, payload, endpoint_name, ...)` — main entry point:
  - Increments `poc:stats:<endpoint>` Redis counter atomically (INCR).
  - Opens OTel span `api.<endpoint_name>` with attributes.
  - Checks `poc:cache:<sha256[:16]>` for a cached result (60s TTL).
  - On miss: calls worker_fn, stores result in Redis.
  - All Redis errors are non-fatal; logs warnings and falls through.
- `get_stats()` — scans `poc:stats:*` keys, returns `{endpoint: count}` dict.
- `health_check()` — PINGs Redis, returns `{"status": "ok|degraded", "redis": "..."}`.
- Redis key conventions: `poc:cache:<16-hex>`, `poc:stats:<endpoint>`.

**`poc_app/src/service/kafka_service.py`** (new)

One-off Kafka produce/consume for HTTP handlers:

- `publish(topic, message, key=None) -> bool` — JSON-encodes and produces, returns False on error (never raises).
- `consume_last(topic, group_id) -> Optional[dict]` — reads the most recent message from a topic, returns None if empty or on error.
- Both functions open OTel child spans (`kafka.publish`, `kafka.consume`) for Jaeger trace correlation.
- Lazy singleton `_get_kafka()` using `KafkaClient.get_instance(security_protocol='NONE')`.

#### PoC API endpoints (updated)

**`poc_app/src/api_endpoints.py`** — updated with inline routing lifecycle comments; all workers now go through `cached_enrich`:

| Endpoint | Method | Description |
|---|---|---|
| `/workers/ner` | POST | echo_single worker, Redis cached |
| `/workers/translate` | POST | rl_single worker, Redis cached, list-aware |
| `/workers/sentiment` | POST | agg_basic_after_merge worker, Redis cached |
| `/workers/health` | GET | Redis liveness probe |
| `/workers/stats` | GET | Per-endpoint call counters from Redis |
| `/workers/echo` | POST | Return payload unchanged (no cache) |
| `/workers/publish` | POST | Produce payload to `poc.echo` Kafka topic |
| `/workers/consume` | GET | Read last message from `poc.echo` Kafka topic |

**`poc_app/maps/endpoint.json`** — added `health`, `stats`, `echo`, `publish`, `consume` entries.

#### Infrastructure

**`poc_app/docker-compose.yml`** — added Jaeger service:

```yaml
jaeger:
  image: jaegertracing/all-in-one:latest
  ports:
    - "16686:16686"   # UI
    - "4317:4317"     # OTLP gRPC
    - "4318:4318"     # OTLP HTTP
  environment:
    COLLECTOR_OTLP_ENABLED: "true"
```

**`poc_app/.env.example`** — added `ENABLE_TRACING=false`, `QSINT_OTLP_ENDPOINT`, `LOG_ENDPOINTS=false`.

**`poc_app/main.py`** — improved startup log line shows tracing/kafka/port; added detailed docstring with tracing quick-start instructions.

#### Tests (new)

**`pytest.ini`** — registered `unit` and `integration` pytest markers.

**`tests/test_tracing.py`** (new, 18 tests `@pytest.mark.unit`):
- `TestNoOpSpan` — all OTel Span methods accepted silently; context manager propagates exceptions.
- `TestNoOpTracer` — yields NoOpSpan; nested spans; exception propagation.
- `TestGetTracer` — returns NoOpTracer when disabled; singleton behaviour; init_tracing noop path.

**`tests/test_api_handler.py`** (new, 13 tests `@pytest.mark.unit`):
- `TestCachedEnrich` — cache hit/miss, worker called/skipped, stats increment, exception propagation, non-fatal Redis errors, cache key collision avoidance.
- `TestGetStats` — populated/empty/error Redis states.
- `TestHealthCheck` — ok and degraded paths.

**`tests/test_kafka_service.py`** (new, 9 tests `@pytest.mark.unit`):
- `TestPublish` — success, JSON encoding, key forwarding, error→False, no-raise guarantee.
- `TestConsumeLast` — parsed dict, empty topic→None, error→None, group_id forwarding.

**Total: 40 new tests (101 passing overall).**

#### OTel auto-instrumentation (new)

**`src/framework/tracing/tracing.py`** — `_instrument_libraries()` called automatically by `init_tracing()` when `ENABLE_TRACING=true`:

| Library | Instrumentor | Spans created |
|---|---|---|
| Flask | `FlaskInstrumentor` | Every HTTP request (server-side) |
| requests | `RequestsInstrumentor` | Every outbound HTTP call |
| kafka-python | `KafkaInstrumentor` | Every produce / consume |
| redis-py | `RedisInstrumentor` | Every Redis command (GET, SET, INCRBY, …) |
| SQLAlchemy | `SQLAlchemyInstrumentor` | Every SQL query |

All instrumentors are wrapped in `try/except` so missing packages are silently skipped. This means the framework works in minimal environments (e.g. Kafka-only with no Redis) without crashing.

**`requirements.txt`** — added `opentelemetry-instrumentation-redis==0.51b0`.

#### Verified end-to-end

- `docker compose up -d` starts Kafka, Redis, Kafka-UI, Postgres, Jaeger.
- `ENABLE_TRACING=true QSINT_OTLP_ENDPOINT=http://localhost:4317 python main.py` starts app.
- Spans `api.ner`, `api.translate`, `api.stats` verified in Jaeger UI at `http://localhost:16686`.
- All 8 endpoints respond correctly; `poc:stats:*` counters increment per call; cache hits skip worker.
- Redis auto-instrumentation verified: `GET`, `INCRBY`, `SET`, `SCRIPT LOAD` spans appear as children of `api.*` spans in Jaeger.

---

## [v6.5.0] — 2026-03-15

### `common.py` decorators applied in POC workers + perf test improvements

#### `poc_app/src/workers/workers.py`

Added three helper functions decorated with `common.py` call-level policies to demonstrate the combined decorator pattern:

- `_enrich_single` — `@call_retry(max_attempts=3)` + `@call_circuit_breaker(fail_max=10, name="enrichment-breaker")` — simulates a retried+protected external enrichment call for single-message workers.
- `_enrich_batch` — `@call_retry(max_attempts=2)` + `@call_circuit_breaker(fail_max=20, name="bulk-enrichment-breaker")` — same for bulk workers.
- `_postprocess_merged` — `@call_rate_limit(per_second=10_000, key="agg-postprocess")` — rate-limited post-processing after aggregation.

All active workers (`echo_single`, `echo_bulk`, `rl_single`, `rl_bulk`, `agg_basic`, `retry_single`) now delegate their enrichment to these helpers, demonstrating the Kafka-level + call-level decorator combination in a real app.

#### Performance tests — default N reduced + comprehensive reports

**`poc_app/tests/perf_kafka.py`**

- Default `N` reduced from `100_000` to `1_000` for faster runs.
- Always sets `fail_prob` in message payload (prevents workers from using their default failure rate during perf tests).
- Final report additions: E2E visual bar chart, column guide, completion summary (total sent/received/missing, wall time), system info (date, Python version, CPU count).

**`poc_app/tests/perf_http.py`**

- Default `N` reduced from `100_000` to `1_000`.
- Progress reporting now scales to `N/5` steps instead of fixed 10,000.
- Final report additions: per-endpoint latency histogram (10 buckets), `min ms` column, status code breakdown on errors, combined req/s summary, system info (date, Python version, CPU count).

#### Test results (N=1,000 on localhost)

**HTTP — 3 endpoints, 1,000 requests each, 50 concurrent threads:**

| Endpoint | req/s | mean ms | p95 ms | p99 ms | Errors |
|---|---|---|---|---|---|
| ner (echo_single) | ~680 | ~72 | ~880 | ~1203 | 0 |
| translate (rl_single) | ~2,540 | ~19 | ~21 | ~21 | 0 |
| sentiment (agg_basic) | ~2,492 | ~19 | ~22 | ~23 | 0 |

All 3,000 requests: 100% success.

**Kafka — 6 workers, 1,000 messages each:**

| Worker | E2E (s) | Out msg/s | Status |
|---|---|---|---|
| echo_single | 0.53 | 1,898 | ✓ |
| echo_bulk | 0.11 | 9,458 | ✓ |
| retry_single | 0.10 | 9,602 | ✓ |
| rl_single | 0.11 | 9,354 | ✓ |
| rl_bulk | 0.10 | 9,576 | ✓ |
| agg_basic | 0.39 | 2,533 | ✓ |

All 6,000 messages received. Wall time: 2.6s.

---

## [v6.4.0] — 2026-03-15

### Two-layer decorator architecture: Kafka-runtime policies vs function-level call policies

#### New: `src/framework/decorators/common.py`

Adds three function-level call policy decorators backed by battle-tested OSS libraries. These work in **any** context — inside Kafka worker functions, HTTP handlers, cron jobs, or standalone scripts. Gevent-compatible after `monkey.patch_all()`.

| Decorator | Library | Description |
|---|---|---|
| `@call_retry` | [tenacity](https://github.com/jd/tenacity) | Retry on exception; fixed or exponential back-off; configurable exception filter |
| `@call_circuit_breaker` | [pybreaker](https://github.com/danielfm/pybreaker) | Trip open after N consecutive failures; auto half-open; excludable exception types |
| `@call_rate_limit` | [limits](https://limits.readthedocs.io/) | Moving-window rate limit; memory or Redis storage; per-key buckets via callable key |

Custom exceptions: `RetryExhaustedError`, `CircuitOpenError`, `RateLimitExceededError` — all exported from `framework.decorators`.

New dependencies added to `requirements.txt`: `tenacity>=8.2`, `pybreaker>=1.2`, `limits>=3.7`.

#### Refactor: `src/framework/decorators/policies.py` → `intern.py`

The existing Kafka-runtime policy decorators (`@retry_to_dlq`, `@rate_limit`, `@circuit_breaker`) moved to `src/framework/decorators/intern.py`. These remain **metadata-only** (no function wrapping) and are interpreted exclusively by the Kafka ETL runtime to pause `TopicPartition`s, re-queue messages to Kafka, and throttle thread-pool dispatch — behaviors impossible with standard call-wrapping libraries.

`policies.py` is kept as a backward-compatibility shim that re-exports all symbols from `intern.py`.

All internal imports updated:
- `src/framework/decorators/kafka_workers.py` — imports configs from `.intern`
- `src/framework/etl/framework_etl.py` — imports configs from `framework.decorators.intern`

#### Updated `src/framework/decorators/__init__.py`

Now exports the complete public API:
- Kafka-runtime policies: `retry_to_dlq`, `circuit_breaker`, `rate_limit` (unchanged names)
- Function-level call policies: `call_retry`, `call_circuit_breaker`, `call_rate_limit`
- Custom exceptions: `RetryExhaustedError`, `CircuitOpenError`, `RateLimitExceededError`

#### Tests

21 new unit tests added to `tests/test_unit.py` (61 total, all pass):
- `TestCallRetry` (6 tests): retry count, exhaustion, reraise, exception filter, no-retry on success, exponential mode
- `TestCallCircuitBreaker` (4 tests): pass-through, open after fail_max, `__circuit_breaker__` attribute, excluded exceptions
- `TestCallRateLimit` (6 tests): within-limit, exceeded, per-minute, invalid window, invalid on_exceeded, callable key
- `TestCommonDecoratorImports` (5 tests): all public names importable from `framework.decorators`

---

## [v6.3.0] — 2026-03-15

### Redis: non-blocking connection pool

**`src/framework/redis/redis_utils.py`**

`RedisSingleton` previously held a single `redis.StrictRedis` socket shared across all threads. Under concurrency this serialized every Redis call through one connection, and any slow or unreachable Redis instance would block all worker threads indefinitely with no timeout.

Replaced with `redis.ConnectionPool`-backed `StrictRedis`:
- Each thread checks out its own socket for the duration of a command, then returns it to the pool — no serialization.
- `socket_timeout` / `socket_connect_timeout` — operations raise `TimeoutError` instead of blocking forever.
- `max_connections` — caps pool size to avoid FD exhaustion under high `max_workers`.
- `socket_keepalive=True` — always on; prevents stale connections on long-idle pools.
- `retry_on_timeout=True` — retries once on timeout; safe for the idempotent Lua aggregation script.

`RedisUtils.__init__` now accepts and forwards all four pool parameters to the singleton.

**`src/framework/etl/framework_etl.py`**

`RedisUtils(...)` call now reads pool config from `Config` at module init:
```python
redis_util = RedisUtils(
    host=Config.REDIS_HOST,
    port=int(Config.REDIS_PORT),
    db=int(Config.REDIS_DB),
    password=None,
    max_connections=int(getattr(Config, "REDIS_MAX_CONNECTIONS", 50)),
    socket_timeout=float(getattr(Config, "REDIS_SOCKET_TIMEOUT", 5.0)),
    socket_connect_timeout=float(getattr(Config, "REDIS_CONNECT_TIMEOUT", 5.0)),
    retry_on_timeout=...,
)
```

**`poc_app/src/config.py`** — four new env vars added:

| Key | Default | Description |
|---|---|---|
| `REDIS_MAX_CONNECTIONS` | `50` | Pool size cap — set ≥ highest `max_workers` |
| `REDIS_SOCKET_TIMEOUT` | `5.0` | Command timeout (seconds) |
| `REDIS_CONNECT_TIMEOUT` | `5.0` | Connect timeout (seconds) |
| `REDIS_RETRY_ON_TIMEOUT` | `true` | Retry once on timeout |

**`poc_app/.env.example`** — same four keys added with comments.

---

## [v6.2.0] — 2026-03-15

### Bug fixes

**`src/framework/etl/framework_etl.py` — ThreadPoolExecutors not shut down on exit**

`WorkerPool` wraps a `ThreadPoolExecutor` per worker, but the `finally` block in `start()` only closed the Kafka consumer and producer. Worker threads were left dangling after the ETL loop exited.

Fixed by calling `pool.executor.shutdown(wait=False)` for every pool in the `finally` block before closing consumer/producer.

---

**`src/framework/etl/framework_etl.py` — Redis NOSCRIPT after Redis restart**

`start()` loads the aggregation Lua script once at startup via `redis_client.script_load(_AGG_LUA)` and stores the SHA. If Redis is restarted the script is evicted; subsequent `evalsha()` calls raise `ResponseError: NOSCRIPT`. The outer `try/except` in `_run_aggregator` was swallowing the error and returning `True` (success), so every aggregated message was committed without being processed — silent data loss.

Fixed by catching `NOSCRIPT` inside `_run_aggregator` and falling back to `redis_client.eval(_AGG_LUA, ...)`, which re-sends the full script body. Other `ResponseError` types are re-raised normally.

---

**`src/framework/etl/framework_etl.py` — Silent JSON parse failures**

Four code paths silently dropped messages when `json.loads()` failed and marked their offsets done:
- Enqueue loop (records received from `consumer.poll()`)
- Bulk drain loop (items dequeued from pending buffer)
- Single drain loop (items dequeued from pending buffer)
- Bulk batch where all items failed to parse (no batch-level log)

All four now emit a `logger.debug()` with the offset, error text, and first 200 chars of the raw payload.

---

**`src/framework/etl/framework_etl.py` — No config validation at startup**

Missing or invalid config values (e.g. empty `ERROR_TOPIC`, negative poll timeout) caused silent misbehavior or exceptions deep inside the poll loop with no clear error.

Added `_validate_config()`, called at the top of `start()`, which:
- Asserts `WORKER_NAME` and `ERROR_TOPIC` are non-empty strings.
- Asserts `KAFKA_POLL_TIMEOUT_MS`, `KAFKA_POLL_MAX_RECORDS`, and `KAFKA_PENDING_MAX_PER_TP` are integers ≥ 1.
- Raises `RuntimeError` listing all failures before the ETL loop starts.

---

**`src/framework/auth/token.py` — Token cache race condition**

`cache = {}` is a module-level dict read and written by multiple HTTP threads concurrently. Concurrent calls to `find_realm_data()` could observe partial writes or a stale empty `realms` list.

Added `_cache_lock = threading.RLock()` and wrapped both the lazy load and the realm lookup inside `with _cache_lock:`.

---

## [v6.1.0] — 2026-03-15

### Bug fixes

**`src/framework/etl/framework_etl.py` — CommitCoordinator: wrong base offset under concurrency**

`CommitCoordinator.mark_done()` was the only place that initialized `next_commit[tp]`. With concurrent worker threads, whichever offset finished first set the window base — not the lowest dispatched offset. All lower offsets accumulated in `done` but could never be reached by the sliding window, so they were never committed.

Fixed by adding `init_tp(tp, first_offset)`, called at dispatch time (before `pool.submit()`). This sets `next_commit[tp]` to the lowest offset in the batch before any thread runs, ensuring the window always starts at the correct base.

---

**`src/framework/etl/framework_etl.py` — CommitCoordinator: per-message commit RPCs**

`try_commit()` was called once per dispatched message, causing up to 100,000 synchronous broker round-trips for 100,000 messages.

Fixed by adding `commit_tick_sec` (from `KAFKA_COMMIT_TICK_SEC`, default 0.2s). `try_commit()` skips the broker call if invoked within the tick window unless `force=True`. A `force=True` commit is issued at ETL shutdown.

---

**`src/framework/etl/framework_etl.py` — CommitCoordinator: committing unchanged partitions**

`try_commit()` added every tracked `TopicPartition` to the commit map regardless of whether the window advanced, issuing redundant broker commits.

Fixed by guarding with `if nxt > original_nxt` — only partitions where the window actually moved are committed.

---

**`src/framework/etl/framework_etl.py` — `_forward_result` sending `None`**

When a worker returned `None` (intentional message filter), `_forward_result` attempted to serialize and produce `None` to the output topic, generating a malformed Kafka message.

Fixed by returning early `if result is None: return True`. `None` items within list results are also skipped.

---

**`src/framework/etl/framework_etl.py` — `_handle_retry_to_dlq` infinite loop**

Workers without a `@retry_to_dlq` decorator that raised exceptions were re-queued to the input topic with `retry=True`. On the second failure the message went to `Config.ERROR_TOPIC`, but the function returned `False` in the no-retry-config path. A `False` return prevented offset commit, causing the same message to be redelivered and processed indefinitely.

Fixed: the no-retry-config path sends to `Config.ERROR_TOPIC` via `_send_sync` and returns `True`, marking the offset as handled.

---

**`src/framework/api/dynamic.py` — URL path doubling**

`endpoint_uri` was constructed as `f"/{operation_name}{api_url}"` where `api_url` already contained the full namespaced path (e.g. `/workers/ner`). Flask-RESTX then prepended the namespace, producing `/workers/ner/workers/ner`.

Fixed by stripping the `/{namespace}` prefix from `api_url` before passing to `ns_api.add_resource()`, so the route resolves correctly to `/workers/ner`.

---

**`src/framework/commons/utils.py` — `eval_file` passing file object to `eval()`**

`eval_file()` called `eval(fin)` on the open file handle rather than on its contents.

Fixed: changed to `eval(fin.read())`.

---

**`poc_app/src/api_endpoints.py` — `ImportError` on startup**

`api_endpoints.py` imported `ner`, `translate_bulk`, and `sentiment_after_merge`, none of which exist in `workers.py`. The app failed to start.

Fixed: updated imports and implementations to use `echo_single`, `rl_single`, and `agg_basic_after_merge`.

---

### New configuration knobs (v6.1)

| Key | Default | Description |
|---|---|---|
| `KAFKA_COMMIT_TICK_SEC` | `0.2` | Min interval between offset commit RPCs |
| `KAFKA_IDLE_SLEEP_SEC` | `0.05` | Sleep duration when poll returns no records |
| `KAFKA_CONNECT_RETRIES` | `10` | Consumer/producer connection retry attempts |
| `KAFKA_CONNECT_RETRY_SLEEP_SEC` | `5` | Seconds between connection retries |
| `KAFKA_PRODUCER_ACKS` | `1` | Producer acks (`0`, `1`, `all`) |
| `KAFKA_PRODUCER_LINGER_MS` | `5` | Producer batching linger (ms) |
| `KAFKA_PRODUCER_BATCH_SIZE` | `65536` | Producer batch size (bytes) |

---

### Infrastructure (v6.1)

**`poc_app/docker-compose.yml`** — created from scratch:
- bitnami/kafka in KRaft mode (no ZooKeeper), external port `9094`
- provectuslabs/kafka-ui on port `8081`
- redis:7-alpine on port `6379`
- postgres:15-alpine on port `5432`

**`poc_app/.env.example`** — sanitized copy of `.env` with placeholder values, safe for version control.

**`.gitignore`** — added at repo root; covers `__pycache__/`, `venv/`, `dist/`, `build/`, `.env`, `.idea/`, `*.log`, `.pytest_cache/`.

---

### Tests (v6.1)

**`tests/test_unit.py`** — 40 unit tests, all pass, no external dependencies:
- Worker registry: registration, deduplication, topic mapping, validation
- Policy decorators: `@retry_to_dlq`, `@rate_limit`, `@circuit_breaker` metadata attachment and decorator order independence
- `CommitCoordinator`: `init_tp` correctness, concurrent `mark_done`, tick batching, force commit, partial window progress
- `_forward_result`: `None` filter, list output, multi-topic routing
- `utils`: `deep_merge`, `load_json`, `getor`
- Dynamic endpoint URL fix: no doubled paths

**`tests/test_integration.py`** — requires Kafka + Redis (`docker compose up -d`):
- Single handler end-to-end
- Bulk handler end-to-end with batching
- Aggregator merging two topics via Redis
- `@retry_to_dlq` exhausting attempts → DLQ routing
- Worker returning `None` → no output produced
- HTTP API: 5 endpoint tests against running server

**`poc_app/tests/perf_http.py`** — 100,000 requests per endpoint, 50 concurrent threads.
Results: ~2,700 req/s, mean 18ms, p99 <50ms, 0 errors.

**`poc_app/tests/perf_kafka.py`** — 100,000 messages per worker (6 workers), E2E throughput.
Results after fixes: `echo_single`, `retry_single`, `rl_single`, `rl_bulk`, `agg_basic` all drained to LAG=0.
Known issue: `echo_bulk` stalls under `KAFKA_BULK_OUTPUT_SYNC=True` with `max_workers=100` — set `KAFKA_BULK_OUTPUT_SYNC=False` or reduce `max_workers` to work around.

---

### Documentation (v6.1)

- `qf_framework_with_poc_v6/README.md` — rewritten with architecture diagram, configuration reference, usage guide, known limitations
- `poc_app/README.md` — rewritten with Mermaid architecture diagram, quickstart, worker reference table, HTTP API table, performance results
- `docs/Documentation.md` — created: deep technical reference (worker registry internals, ETL runtime, CommitCoordinator, Redis Lua aggregation, policy decorators, backpressure, dynamic API, extension patterns, testing guide)
- `docs/Flows.md` — created: 12 Mermaid sequence/flow diagrams covering every execution path
- `CHANGELOG.md` — this file
