# single_execute.py
from __future__ import annotations

import functools
import json
import os
import random
import time
import uuid
from dataclasses import dataclass
from typing import Any, Callable, Optional, TypeVar, Literal, Protocol, cast

T = TypeVar("T")


# ============================================================
# Runtime abstraction: thread / gevent / auto
# ============================================================

class _Runtime(Protocol):
    def sleep(self, seconds: float) -> None: ...
    def event(self): ...
    def spawn(self, fn, *args): ...
    def join(self, handle, timeout: Optional[float] = None) -> None: ...


class _ThreadRuntime:
    def __init__(self):
        import threading
        self._threading = threading

    class _Event:
        def __init__(self, threading_mod):
            self._evt = threading_mod.Event()

        def set(self) -> None:
            self._evt.set()

        def wait(self, timeout: Optional[float] = None) -> bool:
            return self._evt.wait(timeout)

    def event(self):
        return self._Event(self._threading)

    def spawn(self, fn, *args):
        t = self._threading.Thread(target=fn, args=args, daemon=True)
        t.start()
        return t

    def join(self, handle, timeout: Optional[float] = None) -> None:
        try:
            handle.join(timeout=timeout)
        except Exception:
            pass

    @staticmethod
    def sleep(seconds: float) -> None:
        time.sleep(seconds)


class _GeventRuntime:
    def __init__(self):
        import gevent
        from gevent import event as gevent_event
        self._gevent = gevent
        self._gevent_event = gevent_event

    class _Event:
        def __init__(self, gevent_event_mod):
            self._evt = gevent_event_mod.Event()

        def set(self) -> None:
            self._evt.set()

        def wait(self, timeout: Optional[float] = None) -> bool:
            return bool(self._evt.wait(timeout=timeout))

    def event(self):
        return self._Event(self._gevent_event)

    def spawn(self, fn, *args):
        return self._gevent.spawn(fn, *args)

    def join(self, handle, timeout: Optional[float] = None) -> None:
        try:
            handle.join(timeout=timeout)
        except Exception:
            pass

    def sleep(self, seconds: float) -> None:
        self._gevent.sleep(seconds)


def _select_runtime(mode: Literal["thread", "gevent", "auto"]) -> _Runtime:
    if mode == "thread":
        return _ThreadRuntime()
    if mode == "gevent":
        return _GeventRuntime()

    # auto: prefer gevent if importable; else thread
    try:
        return _GeventRuntime()
    except Exception:
        return _ThreadRuntime()


# ============================================================
# Lock backend interface
# ============================================================

@dataclass(frozen=True)
class LockHandle:
    token: str


class LockBackend(Protocol):
    def acquire(self, token: str, ttl_sec: int, block: bool, block_timeout_sec: Optional[float], sleep_fn) -> bool: ...
    def renew_if_owner(self, token: str, ttl_sec: int) -> bool: ...
    def cooldown_if_owner(self, token: str, cooldown_sec: int) -> bool: ...
    def release_if_owner(self, token: str) -> bool: ...


# ============================================================
# Redis backend (recommended)
# ============================================================

class RedisLockBackend:
    _RENEW_LUA = """
    if redis.call("GET", KEYS[1]) == ARGV[1] then
      return redis.call("EXPIRE", KEYS[1], ARGV[2])
    else
      return 0
    end
    """

    _UNLOCK_LUA = """
    if redis.call("GET", KEYS[1]) == ARGV[1] then
      return redis.call("DEL", KEYS[1])
    else
      return 0
    end
    """

    def __init__(self, redis_client, key: str):
        self.r = redis_client
        self.key = key

    def acquire(self, token: str, ttl_sec: int, block: bool, block_timeout_sec: Optional[float], sleep_fn) -> bool:
        start = time.time()
        while True:
            ok = self.r.set(self.key, token, nx=True, ex=int(ttl_sec))
            if ok:
                return True

            if not block:
                return False

            if block_timeout_sec is not None and (time.time() - start) >= block_timeout_sec:
                return False

            sleep_fn(0.2)

    def renew_if_owner(self, token: str, ttl_sec: int) -> bool:
        ok = self.r.eval(self._RENEW_LUA, 1, self.key, token, str(int(ttl_sec)))
        return ok == 1

    def cooldown_if_owner(self, token: str, cooldown_sec: int) -> bool:
        # keep the lock key as a cooldown barrier
        ok = self.r.eval(self._RENEW_LUA, 1, self.key, token, str(int(cooldown_sec)))
        return ok == 1

    def release_if_owner(self, token: str) -> bool:
        ok = self.r.eval(self._UNLOCK_LUA, 1, self.key, token)
        return ok == 1


# ============================================================
# File backend (shared PVC: CephFS / NFS)
# ============================================================

class FileLockBackend:
    """
    Shared-PVC lock using:
      - Atomic acquire via os.open(..., O_CREAT|O_EXCL)
      - JSON payload: {"token": "...", "expires_at": epoch_seconds}
      - Renew/cooldown: atomic write + os.replace
      - Stale-lock stealing w/ skew margin

    Notes:
      - Works best on CephFS.
      - On NFS, correctness depends on server+mount options; jitter+skew help a lot.
    """

    def __init__(self, file_path: str, *, skew_sec: float = 2.0):
        self.file_path = file_path
        self.skew_sec = float(skew_sec)

    def _ensure_dir(self) -> None:
        d = os.path.dirname(self.file_path)
        if d:
            os.makedirs(d, exist_ok=True)

    def _now(self) -> float:
        return time.time()

    def _read(self) -> Optional[dict]:
        try:
            with open(self.file_path, "r", encoding="utf-8") as f:
                return json.load(f)
        except FileNotFoundError:
            return None
        except Exception:
            # Corrupt/partial read: treat as locked briefly (safe default)
            return {"token": None, "expires_at": self._now() + 5}

    def _write_atomic(self, data: dict) -> None:
        tmp = f"{self.file_path}.tmp.{uuid.uuid4()}"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, separators=(",", ":"))
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, self.file_path)

    def acquire(self, token: str, ttl_sec: int, block: bool, block_timeout_sec: Optional[float], sleep_fn) -> bool:
        self._ensure_dir()
        start = self._now()
        ttl = float(ttl_sec)

        while True:
            expires_at = self._now() + ttl
            payload = {"token": token, "expires_at": expires_at}

            try:
                fd = os.open(self.file_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o644)
                try:
                    os.write(fd, json.dumps(payload, separators=(",", ":")).encode("utf-8"))
                    os.fsync(fd)
                finally:
                    os.close(fd)
                return True

            except FileExistsError:
                current = self._read()
                cur_exp = None
                if current and isinstance(current.get("expires_at"), (int, float)):
                    cur_exp = float(current["expires_at"])
                if cur_exp is None:
                    cur_exp = self._now() + 5  # conservative

                # stale iff now > expires_at + skew_sec
                if self._now() > (cur_exp + self.skew_sec):
                    try:
                        os.remove(self.file_path)
                    except FileNotFoundError:
                        pass
                    except Exception:
                        pass
                    continue

                if not block:
                    return False

                if block_timeout_sec is not None and (self._now() - start) >= block_timeout_sec:
                    return False

                sleep_fn(0.2)

    def renew_if_owner(self, token: str, ttl_sec: int) -> bool:
        current = self._read()
        if not current or current.get("token") != token:
            return False

        cur_exp = float(current.get("expires_at", 0))
        if self._now() > (cur_exp + self.skew_sec):
            return False  # lock considered lost

        current["expires_at"] = self._now() + float(ttl_sec)
        try:
            self._write_atomic(current)
            return True
        except Exception:
            return False

    def cooldown_if_owner(self, token: str, cooldown_sec: int) -> bool:
        current = self._read()
        if not current or current.get("token") != token:
            return False

        current["expires_at"] = self._now() + float(cooldown_sec)
        try:
            self._write_atomic(current)
            return True
        except Exception:
            return False

    def release_if_owner(self, token: str) -> bool:
        current = self._read()
        if not current or current.get("token") != token:
            return False
        try:
            os.remove(self.file_path)
            return True
        except FileNotFoundError:
            return True
        except Exception:
            return False


# ============================================================
# Decorator: SingleExecute
# ============================================================

class SingleExecute:
    """
    Execute a function in at most ONE pod/process at a time across a distributed system.

    This decorator provides a distributed mutual-exclusion mechanism with:
      - Redis or shared-filesystem (PVC) locking
      - Heartbeat-based lock renewal while the function is running
      - Optional cooldown ("break") period after execution
      - Safe failover if the running pod crashes or is killed

    It is ideal for:
      - Background sync jobs (Keycloak, LDAP, external APIs)
      - Periodic cache rebuilds
      - Reconciliation / cleanup jobs
      - Leader-only tasks in horizontally scaled services

    ---------------------------------------------------------------------
    BASIC USAGE
    ---------------------------------------------------------------------

    Redis (recommended):

        @SingleExecute(
            backend="redis",
            redis_client=redis_client,
            lock_id="iam:kc:sync",
        )
        def sync():
            ...

    Shared PVC / filesystem (CephFS / NFS):

        @SingleExecute(
            backend="file",
            file_path="/maps/lock.txt",
            lock_id="iam:kc:sync",
        )
        def sync():
            ...

    ---------------------------------------------------------------------
    PARAMETERS
    ---------------------------------------------------------------------

    backend : Literal["redis", "file"], default="redis"
        Lock backend to use.

        - "redis"  → distributed lock using Redis (recommended, most reliable)
        - "file"   → lock file on a shared filesystem (CephFS/NFS)

    lock_id : str (REQUIRED)
        Logical identifier for the lock.
        Must be unique per job across all pods.

        Examples:
            "iam:kc:background_users_sync"
            "billing:monthly_reconciliation"

    ---------------------------------------------------------------------
    Redis-specific options
    ---------------------------------------------------------------------

    redis_client : redis.Redis, default=None
        Redis client instance used for locking.
        Required when backend="redis".

    key_prefix : str, default="singleexec:"
        Prefix prepended to lock_id when generating Redis keys.

        Final Redis key example:
            singleexec:iam:kc:background_users_sync

    ---------------------------------------------------------------------
    File-lock (PVC) specific options
    ---------------------------------------------------------------------

    file_path : Optional[str], default=None
        Path to a lock file on a shared filesystem.
        Required when backend="file".

        Example:
            /maps/locks/iam_kc_sync.lock

    file_skew_sec : float, default=2.0
        Clock-skew / filesystem delay tolerance (seconds).

        Prevents multiple pods from stealing the lock due to:
          - NFS latency
          - Clock drift
          - Slow metadata propagation

    ---------------------------------------------------------------------
    Runtime / concurrency model
    ---------------------------------------------------------------------

    mode : Literal["thread", "gevent", "auto"], default="auto"
        Execution runtime for internal heartbeat thread.

        - "thread" → use standard threading (Gunicorn threads, Flask dev)
        - "gevent" → use gevent greenlets
        - "auto"   → prefer gevent if available, else thread

    ---------------------------------------------------------------------
    Timing & lifecycle
    ---------------------------------------------------------------------

    run_ttl_sec : int, default=240
        Maximum lifetime of the lock *while the function is running*.

        This is a SAFETY TTL:
          - If the pod crashes, is OOM-killed, or deadlocks
          - The lock automatically expires after this time
          - Another pod can safely take over

        Rule of thumb:
            run_ttl_sec ≥ (max expected runtime) × 2

    renew_every_sec : int, default=30
        Heartbeat interval (seconds) while the function is running.

        At each interval:
            - The lock TTL is refreshed back to run_ttl_sec

    cooldown_sec : int, default=300
        Cooldown ("break") period after the function finishes successfully.

        During cooldown:
          - The lock remains held
          - Other pods cannot execute the function

        Common patterns:
          - Periodic jobs → cooldown_sec = interval
          - Leader loop   → cooldown_sec = 0

    cooldown_on_error_sec : Optional[int], default=None
        Alternative cooldown to apply if the function raises an exception.

        If None:
            cooldown_sec is used for both success and failure.

        Example:
            cooldown_sec = 3600
            cooldown_on_error_sec = 300
            → retry sooner on failure

    ---------------------------------------------------------------------
    Execution behavior
    ---------------------------------------------------------------------

    jitter_sec : float, default=0.0
        Random delay (0…jitter_sec seconds) before attempting lock acquisition.

        Reduces thundering-herd effects when many pods start simultaneously.

    block : bool, default=False
        If False:
            - If the lock is already held, the function returns immediately.

        If True:
            - Wait until the lock becomes available (up to block_timeout_sec).

    block_timeout_sec : Optional[float], default=None
        Maximum time (seconds) to wait when block=True.
        None means wait indefinitely.

    raise_if_not_acquired : bool, default=False
        If True:
            - Raise RuntimeError when lock cannot be acquired.

        If False:
            - Return on_skip_return instead.

    on_skip_return : Optional[Any], default=None
        Value returned when execution is skipped because another pod holds the lock.

        Useful for:
          - Silent background jobs
          - Idempotent API handlers

    release_on_cooldown_zero : bool, default=True
        If cooldown_sec == 0:
            - True  → release the lock immediately after execution
            - False → keep lock until run_ttl_sec expires

        Usually leave this as True.

    ---------------------------------------------------------------------
    IMPORTANT DESIGN NOTES
    ---------------------------------------------------------------------

    • The decorated function SHOULD run once and return.
      Do NOT place long sleep loops inside it if you want cooldown semantics.

    • Scheduling (how often the function is attempted) should be handled
      outside the function (e.g. a simple timer thread or cron-like loop).

    • Redis backend is strongly recommended for correctness and failover.
      File backend is a fallback for environments without Redis.

    ---------------------------------------------------------------------
    """

    def __init__(
        self,
        *,
        backend: Literal["redis", "file"] = "redis",
        lock_id: str,
        # Redis
        redis_client=None,
        key_prefix: str = "singleexec:",
        # File
        file_path: Optional[str] = None,
        file_skew_sec: float = 2.0,
        # Runtime
        mode: Literal["thread", "gevent", "auto"] = "auto",
        # Timing
        run_ttl_sec: int = 240,
        renew_every_sec: int = 30,
        cooldown_sec: int = 300,
        cooldown_on_error_sec: Optional[int] = None,
        # Behavior
        jitter_sec: float = 0.0,
        block: bool = False,
        block_timeout_sec: Optional[float] = None,
        raise_if_not_acquired: bool = False,
        on_skip_return: Optional[Any] = None,
        # If cooldown==0, should we release immediately?
        release_on_cooldown_zero: bool = True,
    ):
        self.runtime = _select_runtime(mode)

        self.run_ttl_sec = int(run_ttl_sec)
        self.renew_every_sec = int(renew_every_sec)
        self.cooldown_sec = int(cooldown_sec)
        self.cooldown_on_error_sec = int(cooldown_on_error_sec) if cooldown_on_error_sec is not None else None

        self.jitter_sec = float(jitter_sec)
        self.block = bool(block)
        self.block_timeout_sec = block_timeout_sec
        self.raise_if_not_acquired = bool(raise_if_not_acquired)
        self.on_skip_return = on_skip_return
        self.release_on_cooldown_zero = bool(release_on_cooldown_zero)

        if self.run_ttl_sec <= 0:
            raise ValueError("run_ttl_sec must be > 0")
        if self.renew_every_sec <= 0:
            raise ValueError("renew_every_sec must be > 0")
        if self.cooldown_sec < 0:
            raise ValueError("cooldown_sec must be >= 0")

        if backend == "redis":
            if redis_client is None:
                raise ValueError("redis_client is required for backend='redis'")
            key = f"{key_prefix}{lock_id}"
            self.backend: LockBackend = RedisLockBackend(redis_client, key)

        elif backend == "file":
            if not file_path:
                raise ValueError("file_path is required for backend='file'")
            self.backend = FileLockBackend(file_path=file_path, skew_sec=file_skew_sec)

        else:
            raise ValueError("backend must be 'redis' or 'file'")

    def __call__(self, fn: Callable[..., T]) -> Callable[..., T]:
        @functools.wraps(fn)
        def wrapper(*args, **kwargs) -> T:
            if self.jitter_sec > 0:
                self.runtime.sleep(random.uniform(0, self.jitter_sec))

            token = str(uuid.uuid4())

            acquired = self.backend.acquire(
                token=token,
                ttl_sec=self.run_ttl_sec,
                block=self.block,
                block_timeout_sec=self.block_timeout_sec,
                sleep_fn=self.runtime.sleep,
            )

            if not acquired:
                if self.raise_if_not_acquired:
                    raise RuntimeError("SingleExecute: lock not acquired")
                return cast(T, self.on_skip_return)

            stop_evt = self.runtime.event()
            heartbeat = self.runtime.spawn(self._renew_loop, token, stop_evt)

            success = False
            try:
                result = fn(*args, **kwargs)
                success = True
                return result
            finally:
                stop_evt.set()
                self.runtime.join(heartbeat, timeout=2.0)

                cooldown = self.cooldown_sec if success else (self.cooldown_on_error_sec or self.cooldown_sec)

                if cooldown > 0:
                    # IMPORTANT: enforce "break after finish"
                    self.backend.cooldown_if_owner(token, cooldown)
                else:
                    if self.release_on_cooldown_zero:
                        self.backend.release_if_owner(token)

        return wrapper

    def _renew_loop(self, token: str, stop_evt) -> None:
        while True:
            if stop_evt.wait(self.renew_every_sec):
                break
            ok = self.backend.renew_if_owner(token, self.run_ttl_sec)
            if not ok:
                break
