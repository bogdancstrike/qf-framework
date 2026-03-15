"""
Application configuration — all values read from environment variables.

Copy .env.example → .env and edit before running.
All settings have sane local-dev defaults so the app starts with minimal config.
"""
import os

from framework.commons.logger import logger


class Config:
    # ------------------------------------------------------------------ #
    # Identity
    # ------------------------------------------------------------------ #
    APP_NAME: str = os.getenv("APP_NAME", "qf-template")
    SECRET_KEY: str = os.getenv("SECRET_KEY", "change-me-in-production")

    # ------------------------------------------------------------------ #
    # API
    # ------------------------------------------------------------------ #
    API_PORT: int = int(os.getenv("API_PORT", "5000"))

    # ------------------------------------------------------------------ #
    # Kafka
    # ------------------------------------------------------------------ #
    KAFKA_BOOTSTRAP_SERVERS: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9094")
    WORKER_NAME: str = os.getenv("WORKER_NAME", APP_NAME)
    ERROR_TOPIC: str = os.getenv("ERROR_TOPIC", f"{APP_NAME}.dlq")

    # Commit semantics: "before" (at-most-once) | "after_success" (at-least-once)
    KAFKA_COMMIT_STRATEGY: str = os.getenv("KAFKA_COMMIT_STRATEGY", "before")

    # Poll tuning (DEV defaults — see docs for PROD presets)
    KAFKA_POLL_TIMEOUT_MS: int = int(os.getenv("KAFKA_POLL_TIMEOUT_MS", "1"))
    KAFKA_POLL_MAX_RECORDS: int = int(os.getenv("KAFKA_POLL_MAX_RECORDS", "200"))
    KAFKA_IDLE_SLEEP_SEC: float = float(os.getenv("KAFKA_IDLE_SLEEP_SEC", "0"))
    KAFKA_COMMIT_TICK_SEC: float = float(os.getenv("KAFKA_COMMIT_TICK_SEC", "0.2"))
    KAFKA_MAX_JOBS_PER_TP_PER_TICK: int = int(os.getenv("KAFKA_MAX_JOBS_PER_TP_PER_TICK", "20"))

    # ------------------------------------------------------------------ #
    # Redis
    # ------------------------------------------------------------------ #
    REDIS_HOST: str = os.getenv("REDIS_HOST", "localhost")
    REDIS_PORT: int = int(os.getenv("REDIS_PORT", "6379"))
    REDIS_DB: int = int(os.getenv("REDIS_DB", "0"))
    REDIS_PASSWORD: str | None = os.getenv("REDIS_PASSWORD") or None
    REDIS_MAX_CONNECTIONS: int = int(os.getenv("REDIS_MAX_CONNECTIONS", "50"))
    REDIS_SOCKET_TIMEOUT: float = float(os.getenv("REDIS_SOCKET_TIMEOUT", "5.0"))
    REDIS_CONNECT_TIMEOUT: float = float(os.getenv("REDIS_CONNECT_TIMEOUT", "5.0"))
    REDIS_RETRY_ON_TIMEOUT: bool = os.getenv("REDIS_RETRY_ON_TIMEOUT", "true").lower() == "true"

    # ------------------------------------------------------------------ #
    # Postgres
    # ------------------------------------------------------------------ #
    DB_HOST: str = os.getenv("DB_HOST", "localhost")
    DB_PORT: int = int(os.getenv("DB_PORT", "5432"))
    DB_NAME: str = os.getenv("DB_NAME", "qf")
    DB_USER: str = os.getenv("DB_USER", "qf")
    DB_PASSWORD: str = os.getenv("DB_PASSWORD", "qf")

    @classmethod
    def postgres_url(cls) -> str:
        return (
            f"postgresql+psycopg2://{cls.DB_USER}:{cls.DB_PASSWORD}"
            f"@{cls.DB_HOST}:{cls.DB_PORT}/{cls.DB_NAME}"
        )

    # ------------------------------------------------------------------ #
    # Tracing
    # ------------------------------------------------------------------ #
    ENABLE_TRACING: bool = os.getenv("ENABLE_TRACING", "false").lower() in ("1", "true", "yes")
    OTLP_ENDPOINT: str | None = os.getenv("OTLP_ENDPOINT") or None

    # ------------------------------------------------------------------ #
    # Logging
    # ------------------------------------------------------------------ #
    LOG_ENDPOINTS: bool = os.getenv("LOG_ENDPOINTS", "false").lower() in ("1", "true", "yes")


# Apply log level immediately so workers imported later inherit it.
logger.setLevel(os.getenv("LOG_LEVEL", "INFO"))
