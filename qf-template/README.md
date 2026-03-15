# qf-template

Production-ready hot-start template built on top of **qf-framework**.
Clone, rename, and extend — the scaffolding (Redis, Kafka, Postgres, tracing, HTTP API, workers) is already wired up.

---

## What's included

| Layer | What you get |
|---|---|
| **HTTP API** | Flask-RESTX server with Swagger UI; `GET /app/health`, `GET /app/stats`, `POST /workers/process` |
| **Kafka workers** | Single-message handler, bulk handler, two-topic aggregator — ready to rename and fill in |
| **Redis** | Lazy singleton; result cache (60 s TTL) + per-endpoint call counters |
| **Postgres** | SQLAlchemy engine + session factory; `init_db()` hook called at startup |
| **Tracing** | OTel auto-instrumentation for Flask, requests, kafka-python, redis-py, SQLAlchemy; opt-in via `.env` |
| **Resilience** | `@retry_to_dlq`, `@call_retry`, `@call_circuit_breaker`, `@call_rate_limit` decorators on workers |
| **Gevent** | Monkey-patched in `main.py` line 1; all I/O is non-blocking |
| **Docker** | Two-stage Dockerfile (non-root user, health check); `docker-compose.yml` with all dependencies |

---

## Quick start

### 1. Start infrastructure

```bash
cd qf-template
docker compose up -d
```

Services started:

| Service | URL |
|---|---|
| Kafka | `localhost:9094` |
| Kafka UI | http://localhost:8081 |
| Redis | `localhost:6379` |
| Redis UI (RedisInsight) | http://localhost:5540 |
| Postgres | `localhost:5432` (user/pass/db: `qf`) |
| Jaeger UI | http://localhost:16686 |

### 2. Install dependencies

```bash
# From qf-template/ directory
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
pip install -e ../   # install qf-framework from parent dir
```

### 3. Configure

```bash
cp .env.example .env
# Edit .env if your ports differ
```

### 4. Run

```bash
python main.py
```

Swagger UI → http://localhost:5000
Health check → http://localhost:5000/app/health

---

## Project layout

```
qf-template/
├── main.py                     # Entry point — gevent monkey-patch + FrameworkApp
├── config.py                   # Config class — reads from .env
├── requirements.txt
├── .env.example
├── Dockerfile                  # Two-stage build (builder → runtime, non-root)
├── docker-compose.yml          # All infrastructure services
├── pytest.ini                  # pytest markers: unit, integration
│
├── maps/
│   └── endpoint.json           # Route → handler mapping (no code changes needed for routing)
│
└── src/
    ├── api_endpoints.py        # HTTP handler functions (thin — delegates to service layer)
    ├── instances/
    │   └── instances.py        # Lazy singletons: get_redis(), get_kafka(), get_engine(), get_db()
    ├── models/
    │   ├── base.py             # SQLAlchemy declarative Base
    │   └── models.py           # init_db() hook — add your table creation here
    ├── service/
    │   ├── health_service.py   # check_all() → Redis ping + Kafka list_topics + Postgres SELECT 1
    │   └── api_handler.py      # cached_enrich() + get_stats()
    └── workers/
        └── workers.py          # process_single, process_bulk, merge_parts — replace with your logic
```

---

## Adding a Kafka worker

1. Open `src/workers/workers.py` and add a new function:

```python
@retry_to_dlq(max_attempts=3, dlq_topic="my-app.dlq")
@kafka_handler(
    name="my_worker",
    topics_in=["my-app.input"],
    topics_out=["my-app.output"],
    max_workers=8,
)
def my_worker(message: dict, consumer_name: str, metadatas: dict) -> dict:
    # your logic here
    return message
```

2. No other changes needed — the framework discovers all `@kafka_handler` functions in modules listed under `FrameworkSettings.worker_modules`.

**Bulk mode** — set `bulk_mode=True, batch_size=50` on the decorator to receive a `list[dict]` instead of a single dict.

**Two-topic aggregation** — use `@kafka_aggregator` with two `topics_in`; the framework waits for matching `id` from both topics before calling your function.

---

## Adding an HTTP endpoint

1. Add a handler function in `src/api_endpoints.py`:

```python
def my_endpoint(app, operation, request, **kwargs):
    payload = flask_request.get_json(force=True, silent=False)
    return jsonify(cached_enrich(my_worker, payload, endpoint_name="my_endpoint"))
```

2. Register it in `maps/endpoint.json`:

```json
{
  "namespace": "workers",
  "operation_name": "my_endpoint",
  "model_name": "Empty",
  "request_method": ["POST"],
  "api_url": "/workers/my_endpoint",
  "exec_method": {
    "module_name": "api_endpoints",
    "method_name": "my_endpoint"
  }
}
```

3. Restart — the endpoint appears in Swagger UI automatically.

---

## Using shared instances

All connections are lazy singletons — safe to import anywhere:

```python
from instances import get_redis, get_kafka, get_engine, get_db

# Redis
redis = get_redis()
redis.set_key("my:key", "value", expire=300)
value = redis.get_key("my:key")

# Kafka (for one-off produce outside a worker)
kafka = get_kafka()
kafka.producer.send("my-topic", value=b'{"id":"1"}', key=b"1")

# SQLAlchemy engine
engine = get_engine()
with engine.connect() as conn:
    rows = conn.execute(text("SELECT * FROM my_table")).fetchall()

# SQLAlchemy session (use in FastAPI/Flask route handlers)
def my_route():
    db = next(get_db())
    # ... use db ...
```

---

## Database schema

Add your SQLAlchemy models to `src/models/` and call `Base.metadata.create_all()` in `init_db()`:

```python
# src/models/models.py
from instances import get_engine
from models.base import Base
import models.my_table  # import all model modules so Base sees them

def init_db():
    Base.metadata.create_all(get_engine())
```

`init_db()` is called once at ETL startup before any workers begin polling.

---

## Configuration reference

All settings are read from `.env` (or environment variables).

| Variable | Default | Description |
|---|---|---|
| `APP_NAME` | `qf-template` | Service name (used in logs, traces, Kafka consumer group) |
| `API_PORT` | `5000` | HTTP server port |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9094` | Kafka broker address |
| `WORKER_NAME` | `qf-template` | Kafka consumer group prefix |
| `ERROR_TOPIC` | `qf-template.dlq` | Dead-letter queue topic |
| `REDIS_HOST` | `localhost` | Redis host |
| `REDIS_PORT` | `6379` | Redis port |
| `REDIS_DB` | `0` | Redis database index |
| `DB_HOST` | `localhost` | Postgres host |
| `DB_PORT` | `5432` | Postgres port |
| `DB_NAME` / `DB_USER` / `DB_PASSWORD` | `qf` | Postgres credentials |
| `ENABLE_TRACING` | `false` | Enable OTel tracing |
| `OTLP_ENDPOINT` | — | gRPC OTLP endpoint (e.g. `http://localhost:4317`) |
| `LOG_LEVEL` | `INFO` | Log level (`DEBUG`, `INFO`, `WARNING`, `ERROR`) |
| `LOG_ENDPOINTS` | `false` | Log every HTTP request/response at DEBUG level |

---

## Tracing

OTel auto-instrumentation is built in for Flask, outbound HTTP requests, kafka-python, redis-py, and SQLAlchemy.
Every inbound HTTP request and every Kafka message automatically gets a span.

To enable:

```bash
# .env
ENABLE_TRACING=true
OTLP_ENDPOINT=http://localhost:4317
```

View traces at http://localhost:16686 (Jaeger UI).

---

## Running tests

```bash
# Unit tests — no external services needed
pytest tests/test_unit.py -v -m unit

# Integration tests — requires Docker services running
pytest tests/test_integration.py -v -m integration

# All tests
pytest -v
```

---

## Docker build

```bash
# Build image
docker build -t qf-template:latest .

# Run with .env file
docker run --env-file .env -p 5000:5000 qf-template:latest
```

The image runs as a non-root user (`appuser`, uid 1000).
Docker marks the container healthy once `GET /app/health` responds.

For production, replace the framework install step in the Dockerfile with a pinned wheel:
```dockerfile
RUN pip install --no-cache-dir qf==<version>
```

---

## Renaming the template

1. Replace all occurrences of `qf-template` in `.env.example`, `workers/workers.py`, `docker-compose.yml`, and `maps/endpoint.json` with your app name.
2. Update `APP_NAME` and `WORKER_NAME` in `.env`.
3. Rename Kafka topics in `workers/workers.py` to match your domain.
