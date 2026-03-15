import os
from framework.commons.logger import logger

class Config:
    # Kafka
    WORKER_NAME = os.getenv("WORKER_NAME", "poc-workers-app")
    KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9094")
    ERROR_TOPIC = os.getenv("ERROR_TOPIC", "poc.dlq")

    # Commit semantics
    KAFKA_COMMIT_STRATEGY = os.getenv("KAFKA_COMMIT_STRATEGY", "before")  # before | after_success

    # Poll tuning
    KAFKA_POLL_TIMEOUT_MS = int(os.getenv("KAFKA_POLL_TIMEOUT_MS", "1")) # PROD: 200
    KAFKA_POLL_MAX_RECORDS = int(os.getenv("KAFKA_POLL_MAX_RECORDS", "200")) # PROD: 100
    KAFKA_IDLE_SLEEP_SEC = float(os.getenv("KAFKA_IDLE_SLEEP_SEC", "0")) # PROD: 0.05
    KAFKA_COMMIT_TICK_SEC = float(os.getenv("KAFKA_COMMIT_TICK_SEC", "0.2")) # PROD: 2.0
    KAFKA_MAX_JOBS_PER_TP_PER_TICK = int(os.getenv("KAFKA_MAX_JOBS_PER_TP_PER_TICK", "20")) # PROD: 20

    # Redis (for aggregators)
    REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
    REDIS_PORT = os.getenv("REDIS_PORT", "6379")
    REDIS_DB = os.getenv("REDIS_DB", "0")
    REDIS_MAX_CONNECTIONS = os.getenv("REDIS_MAX_CONNECTIONS", "50")
    REDIS_SOCKET_TIMEOUT = os.getenv("REDIS_SOCKET_TIMEOUT", "5.0")
    REDIS_CONNECT_TIMEOUT = os.getenv("REDIS_CONNECT_TIMEOUT", "5.0")
    REDIS_RETRY_ON_TIMEOUT = os.getenv("REDIS_RETRY_ON_TIMEOUT", "true")

    # API
    API_PORT = int(os.getenv("API_PORT", "5000"))

    SECRET_KEY = os.getenv("SECRET_KEY", "secret")

    # LOGS
    logger.setLevel("INFO")
