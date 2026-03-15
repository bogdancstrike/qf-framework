"""
Lazy singleton accessors for all third-party connections.

Design principles
-----------------
- Every accessor is lazy: the connection is created on first call, not at
  module import time.  This means tests and scripts can import the module
  without requiring live services.
- All singletons are module-level variables (not class attributes).  This
  is the simplest pattern that is also gevent-safe after monkey.patch_all().
- Configuration is read from config.Config so env-var overrides work at
  runtime.

Usage
-----
    from instances import get_redis, get_kafka, get_engine, get_db

    # Redis
    r = get_redis()
    r.set_key("greeting", "hello", expire=60)

    # Kafka (one-off produce/consume)
    k = get_kafka()
    k.put_message("my.topic", '{"id": "1"}')

    # SQLAlchemy engine (raw queries / Alembic)
    engine = get_engine()
    with engine.connect() as conn:
        result = conn.execute(text("SELECT 1"))

    # SQLAlchemy session (ORM, dependency-injection style)
    for db in get_db():
        db.query(MyModel).filter_by(id=1).first()

Extending
---------
Add more singletons here following the same lazy-init pattern:
    _elasticsearch: Optional[Elasticsearch] = None
    def get_elasticsearch() -> Elasticsearch: ...
"""

from __future__ import annotations

from threading import Lock
from typing import Generator, Optional

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, Session

from framework.commons.logger import logger
from framework.redis.redis_utils import RedisUtils
from framework.streams.kafka_client import KafkaClient

# ---------------------------------------------------------------------------
# Redis
# ---------------------------------------------------------------------------

_redis: Optional[RedisUtils] = None
_redis_lock = Lock()


def get_redis() -> RedisUtils:
    """Return the shared RedisUtils instance (thread-safe lazy singleton).

    Configuration read from config.Config.  Call this from worker functions,
    HTTP handlers, or anywhere you need Redis — never instantiate RedisUtils
    directly in application code.
    """
    global _redis
    if _redis is None:
        with _redis_lock:
            if _redis is None:
                from config import Config  # type: ignore[import]
                _redis = RedisUtils(
                    host=Config.REDIS_HOST,
                    port=Config.REDIS_PORT,
                    db=Config.REDIS_DB,
                    max_connections=Config.REDIS_MAX_CONNECTIONS,
                    socket_timeout=Config.REDIS_SOCKET_TIMEOUT,
                    socket_connect_timeout=Config.REDIS_CONNECT_TIMEOUT,
                    retry_on_timeout=Config.REDIS_RETRY_ON_TIMEOUT,
                )
                logger.debug(
                    f"[instances] Redis initialised — "
                    f"{Config.REDIS_HOST}:{Config.REDIS_PORT}/{Config.REDIS_DB}"
                )
    return _redis


# ---------------------------------------------------------------------------
# Kafka
# ---------------------------------------------------------------------------

_kafka: Optional[KafkaClient] = None
_kafka_lock = Lock()


def get_kafka() -> KafkaClient:
    """Return the shared KafkaClient instance (thread-safe lazy singleton).

    Uses security_protocol='NONE' (plain TCP) by default, suitable for a
    local docker-compose Kafka.  For production update the security params
    or replace this factory with your own.
    """
    global _kafka
    if _kafka is None:
        with _kafka_lock:
            if _kafka is None:
                from config import Config  # type: ignore[import]
                _kafka = KafkaClient.get_instance(
                    security_protocol="NONE",
                    bootstrap_servers=Config.KAFKA_BOOTSTRAP_SERVERS,
                    auto_offset_reset="earliest",
                    group_id=f"{Config.WORKER_NAME}-client",
                )
                logger.debug(
                    f"[instances] Kafka initialised — {Config.KAFKA_BOOTSTRAP_SERVERS}"
                )
    return _kafka


# ---------------------------------------------------------------------------
# SQLAlchemy — engine
# ---------------------------------------------------------------------------

_engine = None
_engine_lock = Lock()


def get_engine():
    """Return the shared SQLAlchemy engine (thread-safe lazy singleton).

    The engine manages a connection pool internally.  Reuse the same engine
    across all requests — never create a new engine per request.

    For Alembic migrations, import get_engine() in env.py:
        from instances import get_engine
        connectable = get_engine()
    """
    global _engine
    if _engine is None:
        with _engine_lock:
            if _engine is None:
                from config import Config  # type: ignore[import]
                _engine = create_engine(
                    Config.postgres_url(),
                    pool_size=10,
                    max_overflow=20,
                    pool_pre_ping=True,   # detect stale connections before use
                    pool_recycle=1800,    # recycle connections older than 30 min
                )
                logger.debug(
                    f"[instances] Postgres engine initialised — "
                    f"{Config.DB_HOST}:{Config.DB_PORT}/{Config.DB_NAME}"
                )
    return _engine


# ---------------------------------------------------------------------------
# SQLAlchemy — session factory
# ---------------------------------------------------------------------------

_SessionLocal = None
_session_lock = Lock()


def _get_session_factory():
    global _SessionLocal
    if _SessionLocal is None:
        with _session_lock:
            if _SessionLocal is None:
                _SessionLocal = sessionmaker(
                    bind=get_engine(),
                    autocommit=False,
                    autoflush=False,
                )
    return _SessionLocal


def get_db() -> Generator[Session, None, None]:
    """Yield a SQLAlchemy session and ensure it is closed afterwards.

    Use as a context manager or in a dependency-injection loop:

        # Direct use
        for db in get_db():
            db.query(User).all()

        # Or with next()
        db = next(get_db())
        try:
            db.query(User).all()
            db.commit()
        finally:
            db.close()
    """
    SessionLocal = _get_session_factory()
    db: Session = SessionLocal()
    try:
        yield db
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()
