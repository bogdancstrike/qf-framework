# redis_utils.py

import redis
from threading import Lock


class RedisSingleton:
    """
    Singleton managing a Redis ConnectionPool.

    Uses a connection pool instead of a single blocking connection so that:
    - Multiple threads each get their own socket from the pool (no serialization).
    - `socket_timeout` / `socket_connect_timeout` prevent threads from blocking
      indefinitely when Redis is slow or unreachable.
    - `max_connections` caps memory/FD usage under high concurrency.

    The singleton is keyed on the first set of parameters passed.
    Re-initializing with different parameters has no effect (singleton contract).
    """

    _instance = None
    _lock = Lock()

    def __new__(
        cls,
        host: str = "localhost",
        port: int = 6379,
        db: int = 0,
        password: str | None = None,
        max_connections: int = 50,
        socket_timeout: float = 5.0,
        socket_connect_timeout: float = 5.0,
        retry_on_timeout: bool = True,
    ):
        if not cls._instance:
            with cls._lock:
                if not cls._instance:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialize(
                        host=host,
                        port=port,
                        db=db,
                        password=password,
                        max_connections=max_connections,
                        socket_timeout=socket_timeout,
                        socket_connect_timeout=socket_connect_timeout,
                        retry_on_timeout=retry_on_timeout,
                    )
        return cls._instance

    def _initialize(
        self,
        host: str,
        port: int,
        db: int,
        password: str | None,
        max_connections: int,
        socket_timeout: float,
        socket_connect_timeout: float,
        retry_on_timeout: bool,
    ) -> None:
        pool = redis.ConnectionPool(
            host=host,
            port=port,
            db=db,
            password=password or None,
            decode_responses=True,
            max_connections=max_connections,
            socket_timeout=socket_timeout,
            socket_connect_timeout=socket_connect_timeout,
            socket_keepalive=True,
            retry_on_timeout=retry_on_timeout,
        )
        self._client = redis.StrictRedis(connection_pool=pool)

    @property
    def client(self) -> redis.StrictRedis:
        return self._client


class RedisUtils:
    """
    Thin wrapper around the Redis singleton providing named helpers.

    All timeout / pool settings are forwarded to RedisSingleton on first
    instantiation.  Subsequent instantiations reuse the existing pool.
    """

    def __init__(
        self,
        host: str = "localhost",
        port: int = 6379,
        db: int = 0,
        password: str | None = None,
        max_connections: int = 50,
        socket_timeout: float = 5.0,
        socket_connect_timeout: float = 5.0,
        retry_on_timeout: bool = True,
    ):
        self.redis = RedisSingleton(
            host=host,
            port=port,
            db=db,
            password=password,
            max_connections=max_connections,
            socket_timeout=socket_timeout,
            socket_connect_timeout=socket_connect_timeout,
            retry_on_timeout=retry_on_timeout,
        ).client

    def set_key(self, key, value, expire=None):
        return self.redis.set(key, value, ex=expire)

    def get_key(self, key):
        return self.redis.get(key)

    def delete_key(self, key):
        return self.redis.delete(key)

    def exists_key(self, key):
        return self.redis.exists(key) == 1

    def increment_key(self, key, amount=1):
        return self.redis.incr(key, amount)

    def set_hash(self, name, key, value):
        return self.redis.hset(name, key, value)

    def get_hash(self, name, key):
        return self.redis.hget(name, key)

    def delete_hash_field(self, name, key):
        return self.redis.hdel(name, key) == 1

    def get_all_hash(self, name):
        return self.redis.hgetall(name)

    def list_all_keys(self, pattern="*"):
        return self.redis.keys(pattern)

    def get_list_length(self, key):
        return self.redis.llen(key)
