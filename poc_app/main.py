import os
import sys
from pathlib import Path

# ---- Path setup (PoC only) ----
BASE_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(BASE_DIR / "src"))          # workers/, config.py
sys.path.insert(0, str(BASE_DIR.parent / "src"))   # framework/

from src.config import Config
from framework.app import FrameworkApp, FrameworkSettings
from framework.commons.logger import logger


def main():
    settings = FrameworkSettings(
        enable_etl=True,
        enable_api=True,
        enable_dynamic_endpoints=True,

        # API
        api_host="0.0.0.0",
        api_port=Config.API_PORT,
        api_version="1.0",
        api_title="PoC Workers API",
        api_description="Workers exposed via Kafka + HTTP",

        # Endpoints
        endpoint_json_path="maps/endpoint.json",

        # ETL
        worker_modules=["workers.workers"],
        kafka_bootstrap_servers=Config.KAFKA_BOOTSTRAP_SERVERS,
        consumer_name=Config.WORKER_NAME,

        # Tracing
        enable_tracing=True,
        otlp_endpoint=os.getenv("QSINT_OTLP_ENDPOINT"),
        service_name=Config.WORKER_NAME,
    )

    fw = FrameworkApp(settings, app_root=BASE_DIR)
    handles = fw.run()

    # Start API server (ETL already running in background thread)
    if handles.app:
        logger.info(f"[PoC] API listening on {settings.api_host}:{settings.api_port}")
        handles.app.run(host=settings.api_host, port=settings.api_port, debug=False)


if __name__ == "__main__":
    main()
