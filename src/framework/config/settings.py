from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Optional, Callable, Any, Dict

try:
    from dotenv import load_dotenv  # type: ignore
except Exception:  # pragma: no cover
    load_dotenv = None

def _bool(v: Optional[str], default: bool=False) -> bool:
    if v is None:
        return default
    return v.strip().lower() in ("1", "true", "yes", "y", "on")

@dataclass
class FrameworkSettings:
    # Feature toggles
    enable_etl: bool = True
    enable_api: bool = True
    enable_dynamic_endpoints: bool = True

    # API config
    api_host: str = "0.0.0.0"
    api_port: int = 4000
    api_version: str = "1.0"
    api_title: str = "QSINT Worker API"
    api_description: str = "Unified Worker API (Kafka + HTTP)"

    # Endpoint mapping
    endpoint_json_path: str = "maps/endpoint.json"

    # Kafka ETL
    worker_modules: list[str] = field(default_factory=list)
    kafka_bootstrap_servers: Optional[str] = None
    consumer_name: Optional[str] = None

    # Tracing
    enable_tracing: bool = True
    otlp_endpoint: Optional[str] = None  # e.g. http://jaeger:4317
    service_name: Optional[str] = None   # defaults to WORKER_NAME if available

    # Hooks
    init_app: Optional[Callable[[Any], None]] = None  # called with Flask app

    # Extra knobs: pass-through
    extra: Dict[str, Any] = field(default_factory=dict)

def load_framework_settings(*, dotenv_path: Optional[str]=None, overrides: Optional[dict]=None) -> FrameworkSettings:
    """Load settings from environment and optional overrides.
    This is intentionally minimal, so real apps can pass explicit params.
    """
    if load_dotenv:
        load_dotenv(dotenv_path=dotenv_path)

    s = FrameworkSettings(
        enable_etl=_bool(os.getenv("QSINT_ENABLE_ETL"), True),
        enable_api=_bool(os.getenv("QSINT_ENABLE_API"), True),
        enable_dynamic_endpoints=_bool(os.getenv("QSINT_ENABLE_DYNAMIC_ENDPOINTS"), True),
        api_host=os.getenv("QSINT_API_HOST", "0.0.0.0"),
        api_port=int(os.getenv("QSINT_API_PORT", "4000")),
        api_version=os.getenv("QSINT_API_VERSION", "1.0"),
        api_title=os.getenv("QSINT_API_TITLE", "QSINT Worker API"),
        api_description=os.getenv("QSINT_API_DESCRIPTION", "Unified Worker API (Kafka + HTTP)"),
        endpoint_json_path=os.getenv("QSINT_ENDPOINT_JSON", "maps/endpoint.json"),
        kafka_bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS"),
        consumer_name=os.getenv("WORKER_NAME"),
        enable_tracing=_bool(os.getenv("QSINT_ENABLE_TRACING"), True),
        otlp_endpoint=os.getenv("QSINT_OTLP_ENDPOINT"),
        service_name=os.getenv("QSINT_SERVICE_NAME"),
    )

    if overrides:
        for k, v in overrides.items():
            if hasattr(s, k):
                setattr(s, k, v)
            else:
                s.extra[k] = v
    return s
