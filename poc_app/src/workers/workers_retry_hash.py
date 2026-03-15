# workers_policy_tests.py
import hashlib
import time
from framework.commons.logger import logger
from framework.decorators import (
    kafka_handler,
    kafka_aggregator,
    rate_limit,
    circuit_breaker,
    retry_to_dlq,
)

# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------

FAIL_BUCKET = 10
# Meaning:
#   - bucket is computed as sha1(id)[0] % 100  -> range 0..99
#   - if bucket < FAIL_BUCKET => ALWAYS FAIL
# Examples:
#   FAIL_BUCKET=5  -> ~5% DLQ
#   FAIL_BUCKET=10 -> ~10% DLQ
#   FAIL_BUCKET=20 -> ~20% DLQ

def _deterministic_should_fail(msg: dict, *, default_bucket: int = FAIL_BUCKET) -> bool:
    """
    Deterministic failure control (Option A):
      - force_fail=True -> always fail
      - fail_bucket=<0..100> -> deterministic failure rate override per message
      - else uses default_bucket

    Decision is stable across retries / restarts for the same "id".
    """
    if msg.get("force_fail") is True:
        return True

    mid = msg.get("id")
    if mid is None:
        # If you want "missing id" to be a poison-pill -> return True
        return False

    # allow per-message override
    b = msg.get("fail_bucket", default_bucket)
    try:
        b = int(b)
    except Exception:
        b = default_bucket
    b = max(0, min(100, b))

    # stable hash -> stable bucket 0..99
    mid_s = str(mid)
    digest = hashlib.sha1(mid_s.encode("utf-8")).digest()
    bucket = digest[0] % 100
    return bucket < b


def _touch_enrichment(msg: dict, key: str, value):
    msg.setdefault("enrichment", {})[key] = value
    return msg


# ============================================================
# C) kafka_handler + retry_to_dlq (single)
#     - deterministic fails -> retries -> eventually DLQ
# ============================================================

@kafka_handler(
    name="retry_single",
    topics_in=["poc.retry.single.in"],
    topics_out=["poc.retry.single.out"],
    max_workers=50,
    bulk_mode=False,
    metadatas={"worker": "retry_single"},
)
@retry_to_dlq(max_attempts=3, dlq_topic="poc.dlq.retry.single", retry_count_field="retry_count")
def retry_single(message: dict, consumer_name: str, metadatas: dict) -> dict:
    mid = message.get("id")

    # Deterministic failure
    if _deterministic_should_fail(message, default_bucket=FAIL_BUCKET):
        logger.info(
            f"[retry_single] FAIL(det) consumer={consumer_name} id={mid} "
            f"retry_count={message.get('retry_count')} fail_bucket={message.get('fail_bucket', FAIL_BUCKET)}"
        )
        raise RuntimeError("deterministic failure (retry_single)")

    logger.info(
        f"[retry_single] OK(det) consumer={consumer_name} id={mid} "
        f"retry_count={message.get('retry_count')} fail_bucket={message.get('fail_bucket', FAIL_BUCKET)}"
    )
    return _touch_enrichment(message, "retry_single", {"ok": True, "ts": time.time()})
