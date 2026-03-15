"""
PoC application entry point.

Starts both the ETL (Kafka consumer) and the HTTP API in a single process.
The ETL runs in a background thread managed by FrameworkApp; the Flask
development server blocks the main thread.

Environment variables of interest
----------------------------------
  ENABLE_TRACING=true        — enable OTel span export to Jaeger (default: false)
  QSINT_OTLP_ENDPOINT        — OTLP gRPC endpoint, e.g. http://localhost:4317
  LOG_ENDPOINTS=true         — log every HTTP request/response (default: false)
  KAFKA_BOOTSTRAP_SERVERS    — Kafka broker address (default: localhost:9094)
  REDIS_HOST / REDIS_PORT    — Redis connection (default: localhost:6379)
  API_PORT                   — HTTP listen port (default: 5000)

Tracing quick-start
-------------------
  1. Start docker-compose (includes Jaeger):
       docker-compose up -d
  2. Run the app with tracing enabled:
       ENABLE_TRACING=true QSINT_OTLP_ENDPOINT=http://localhost:4317 python main.py
  3. Send a request:
       curl -X POST http://localhost:5000/workers/ner -H 'Content-Type: application/json' -d '{"id":"1","text":"hello"}'
  4. Open Jaeger UI: http://localhost:16686 — search for service "poc-workers-app"
"""

import os
import sys
from pathlib import Path

# ---- Path setup (PoC only) ----
# Adds poc_app/src/ (workers, config) and the framework src/ to sys.path.
# In a production package these would be installed as proper packages.
BASE_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(BASE_DIR / "src"))          # workers/, config.py, service/
sys.path.insert(0, str(BASE_DIR.parent / "src"))   # framework/

from src.config import Config
from framework.app import FrameworkApp, FrameworkSettings
from framework.commons.logger import logger

# ENABLE_TRACING is read here so the log line below reflects the actual value.
_tracing_on = os.getenv("ENABLE_TRACING", "false").lower() in ("1", "true", "yes")


def main():
    logger.info(
        f"[PoC] Starting — tracing={'enabled' if _tracing_on else 'disabled'} "
        f"kafka={Config.KAFKA_BOOTSTRAP_SERVERS} "
        f"api_port={Config.API_PORT}"
    )

    settings = FrameworkSettings(
        enable_etl=True,
        enable_api=True,
        enable_dynamic_endpoints=True,

        # API server
        api_host="0.0.0.0",
        api_port=Config.API_PORT,
        api_version="1.0",
        api_title="PoC Workers API",
        api_description="Workers exposed via Kafka + HTTP (QF Framework PoC)",

        # Dynamic endpoint registration from JSON map
        endpoint_json_path="maps/endpoint.json",

        # ETL (Kafka consumer) — worker modules are scanned for @kafka_handler
        worker_modules=["workers.workers"],
        kafka_bootstrap_servers=Config.KAFKA_BOOTSTRAP_SERVERS,
        consumer_name=Config.WORKER_NAME,

        # Tracing — init_tracing() is called by FrameworkApp using these values
        enable_tracing=_tracing_on,
        otlp_endpoint=os.getenv("QSINT_OTLP_ENDPOINT"),
        service_name=Config.WORKER_NAME,
    )

    fw = FrameworkApp(settings, app_root=BASE_DIR)
    handles = fw.run()

    # ETL thread is already running; block on the Flask dev server.
    if handles.app:
        logger.info(f"[PoC] API listening on {settings.api_host}:{settings.api_port}")
        handles.app.run(host=settings.api_host, port=settings.api_port, debug=False)


if __name__ == "__main__":
    main()
