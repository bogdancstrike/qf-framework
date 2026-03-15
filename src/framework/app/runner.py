from __future__ import annotations

import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

from flask import Flask

from ..commons.logger import logger
from ..config.settings import FrameworkSettings
from ..tracing import init_tracing
from ..api.server import create_app, create_api
from ..api.dynamic import generate_endpoints_from_config
from ..etl.framework_etl import start as etl_start


@dataclass
class FrameworkHandles:
    app: Optional[Flask] = None
    api: Any = None
    etl_thread: Optional[threading.Thread] = None


class FrameworkApp:
    """High-level integration wrapper.

    Intended usage in a real app:
        from framework.app import FrameworkApp, FrameworkSettings
        settings = FrameworkSettings(enable_etl=True, enable_api=True, ...)
        FrameworkApp(settings, app_root=Path(__file__).parent).run()

    The goal: *one call* from main.py.
    """

    def __init__(self, settings: FrameworkSettings, *, app_root: Optional[Path] = None):
        self.settings = settings
        self.app_root = app_root or Path.cwd()
        self.handles = FrameworkHandles()

    def build_flask_app(self) -> Flask:
        app = Flask(
            __name__,
            root_path=str(self.app_root),
            static_url_path="",
            instance_relative_config=True,
        )
        create_app(app)

        if self.settings.init_app:
            try:
                self.settings.init_app(app)
            except Exception:
                logger.exception("init_app hook failed")

        self.handles.app = app
        return app

    def start_api(self) -> None:
        app = self.handles.app or self.build_flask_app()
        api = create_api(
            app,
            version=self.settings.api_version,
            title=self.settings.api_title,
            description=self.settings.api_description,
        )
        self.handles.api = api

        if self.settings.enable_dynamic_endpoints:
            # endpoint_json_path is relative to app root
            cfg_path = (self.app_root / self.settings.endpoint_json_path).as_posix()
            generate_endpoints_from_config(api, cfg_path)

    def start_etl(self) -> None:
        """Start ETL runtime in a background thread.

        worker_modules and bootstrap_servers can be provided via FrameworkSettings.
        If missing, we try to read them from the application's config.Config.
        """
        worker_modules = list(self.settings.worker_modules or [])
        bootstrap = self.settings.kafka_bootstrap_servers
        consumer_name = self.settings.consumer_name

        if (not worker_modules) or (not bootstrap) or (not consumer_name):
            try:
                from config import Config  # type: ignore

                bootstrap = bootstrap or getattr(Config, "KAFKA_BOOTSTRAP_SERVERS", None)
                consumer_name = consumer_name or getattr(Config, "WORKER_NAME", None)

                # optional default
                if not worker_modules:
                    worker_modules = [getattr(Config, "WORKER_MODULE", "workers.workers")]
            except Exception:
                pass

        if not worker_modules or not bootstrap or not consumer_name:
            raise RuntimeError(
                "Missing ETL configuration. Provide FrameworkSettings.worker_modules, "
                "kafka_bootstrap_servers and consumer_name (or define them in config.Config)."
            )

        def _run() -> None:
            etl_start(worker_modules=worker_modules, bootstrap_servers=bootstrap, consumer_name=consumer_name)

        t = threading.Thread(target=_run, name="qsint-etl", daemon=True)
        t.start()
        self.handles.etl_thread = t

    def run(self) -> FrameworkHandles:
        # tracing
        if self.settings.enable_tracing:
            service = self.settings.service_name or "qsint-worker"
            try:
                from config import Config  # type: ignore

                service = self.settings.service_name or getattr(Config, "WORKER_NAME", service)
            except Exception:
                pass

            init_tracing(service_name=service, otlp_endpoint=self.settings.otlp_endpoint)

        if self.settings.enable_api:
            self.start_api()

        if self.settings.enable_etl:
            self.start_etl()

        return self.handles

    def shutdown(self) -> None:
        logger.warning("ETL runtime stop not implemented; exiting relies on process termination")
