# Changelog

All notable changes to the QF Framework and PoC application.

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
