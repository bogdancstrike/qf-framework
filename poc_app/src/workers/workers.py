# workers_policy_tests.py
import random
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
def _should_fail(msg: dict, *, default_prob: float) -> bool:
    """
    Failure control:
      - force_fail=True -> always fail
      - fail_prob=<0..1> -> probabilistic
      - else uses default_prob
    """
    if msg.get("force_fail") is True:
        return True
    p = msg.get("fail_prob")
    if p is None:
        p = default_prob
    try:
        p = float(p)
    except Exception:
        p = default_prob
    return random.random() < max(0.0, min(1.0, p))


def _touch_enrichment(msg: dict, key: str, value):
    msg.setdefault("enrichment", {})[key] = value
    return msg


# ============================================================
# A) ONLY kafka_handler (single + bulk) - no policies
# ============================================================

@kafka_handler(
    name="echo_single",
    topics_in=["poc.echo.single.in"],
    topics_out=["poc.echo.single.out"],
    max_workers=100,
    bulk_mode=False,
    metadatas={"worker": "echo_single"},
)
def echo_single(message: dict, consumer_name: str, metadatas: dict) -> dict:
    mid = message.get("id")
    # logger.info(f"[echo_single] consumer={consumer_name} id={mid} meta={metadatas}")
    return _touch_enrichment(message, "echo_single", {"ok": True, "ts": time.time()})


@kafka_handler(
    name="echo_bulk",
    topics_in=["poc.echo.bulk.in"],
    topics_out=["poc.echo.bulk.out"],
    max_workers=100,
    bulk_mode=True,
    batch_size=100,
    batch_timeout_ms=1000,
    metadatas={"worker": "echo_bulk", "mode": "bulk"},
)
def echo_bulk(messages: list[dict], consumer_name: str, metadatas: dict):
    # logger.info(f"[echo_bulk] consumer={consumer_name} batch={len(messages)} meta={metadatas}")
    out = []
    for m in messages:
        out.append(_touch_enrichment(m, "echo_bulk", {"ok": True, "ts": time.time()}))
    return out


# # ============================================================
# # B) ONLY kafka_aggregator - no policies
# # ============================================================

@kafka_aggregator(
    name="agg_basic",
    topics_in=["poc.agg.basic.a", "poc.agg.basic.b"],
    topics_out=["poc.agg.basic.out"],
    aggregate_by="id",
    aggregator_timeout_sec=3600 * 24,
    max_workers=100,
    metadatas={"worker": "agg_basic", "mode": "aggregator"},
)
def agg_basic_after_merge(merged: dict, consumer_name: str, metadatas: dict) -> dict:
    mid = merged.get("id")
    # logger.info(f"[agg_basic] consumer={consumer_name} id={mid} merged_keys={list(merged.keys())} meta={metadatas}")
    return _touch_enrichment(merged, "agg_basic", {"merged": True, "ts": time.time()})


# ============================================================
# C) kafka_handler + retry_to_dlq (single + bulk)
#    - random fails -> retries -> eventually DLQ
# ============================================================

@kafka_handler(
    name="retry_single",
    topics_in=["poc.retry.single.in"],
    topics_out=["poc.retry.single.out"],
    max_workers=100,
    bulk_mode=False,
    metadatas={"worker": "retry_single"},
)
@retry_to_dlq(max_attempts=2, dlq_topic="poc.dlq.retry.single", retry_count_field="retry_count")
def retry_single(message: dict, consumer_name: str, metadatas: dict) -> dict:
    mid = message.get("id")
    if _should_fail(message, default_prob=0.10):
        # logger.info(f"[retry_single] FAIL consumer={consumer_name} id={mid} retry_count={message.get('retry_count')}")
        raise RuntimeError("random failure (retry_single)")
    # logger.info(f"[retry_single] OK consumer={consumer_name} id={mid} retry_count={message.get('retry_count')}")
    return _touch_enrichment(message, "retry_single", {"ok": True, "ts": time.time()})


# # ============================================================
# # D) kafka_handler + rate_limit (single + bulk)
# # ============================================================

@kafka_handler(
    name="rl_single",
    topics_in=["poc.rl.single.in"],
    topics_out=["poc.rl.single.out"],
    max_workers=100,
    bulk_mode=False,
    metadatas={"worker": "rl_single"},
)
@rate_limit(rps=5000, burst=5000)  # interpret as "dispatches per second" in your current runtime
def rl_single(message: dict, consumer_name: str, metadatas: dict) -> dict:
    mid = message.get("id")
    # logger.info(f"[rl_single] consumer={consumer_name} id={mid}")
    return _touch_enrichment(message, "rl_single", {"ok": True, "ts": time.time()})


@kafka_handler(
    name="rl_bulk",
    topics_in=["poc.rl.bulk.in"],
    topics_out=["poc.rl.bulk.out"],
    max_workers=100,
    bulk_mode=True,
    batch_size=25,
    batch_timeout_ms=1000,
    metadatas={"worker": "rl_bulk", "mode": "bulk"},
)
@rate_limit(rps=5000, burst=5000)  # with current runtime, this is ~10 BATCHES/sec, not messages/sec
def rl_bulk(messages: list[dict], consumer_name: str, metadatas: dict):
    # logger.info(f"[rl_bulk] consumer={consumer_name} batch={len(messages)}")
    out = []
    for m in messages:
        out.append(_touch_enrichment(m, "rl_bulk", {"ok": True, "ts": time.time()}))
    return out


# # ============================================================
# # E) kafka_handler + circuit_breaker (single + bulk)
# # CIRCUIT BREAKER: ARE BUGS
# # ============================================================

# @kafka_handler(
#     name="cb_single",
#     topics_in=["poc.cb.single.in"],
#     topics_out=["poc.cb.single.out"],
#     max_workers=200,
#     bulk_mode=False,
#     metadatas={"worker": "cb_single"},
# )
# @circuit_breaker(failures=20, reset_sec=10)
# def cb_single(message: dict, consumer_name: str, metadatas: dict) -> dict:
#     mid = message.get("id")
#     if _should_fail(message, default_prob=0.05):
#         logger.info(f"[cb_single] FAIL id={mid}")
#         raise RuntimeError("random failure (cb_single)")
#     logger.info(f"[cb_single] OK id={mid}")
#     return _touch_enrichment(message, "cb_single", {"ok": True, "ts": time.time()})
#
#
# @kafka_handler(
#     name="cb_bulk",
#     topics_in=["poc.cb.bulk.in"],
#     topics_out=["poc.cb.bulk.out"],
#     max_workers=150,
#     bulk_mode=True,
#     batch_size=2,
#     batch_timeout_ms=1500,
#     metadatas={"worker": "cb_bulk", "mode": "bulk"},
# )
# @circuit_breaker(failures=20, reset_sec=10)
# def cb_bulk(messages: list[dict], consumer_name: str, metadatas: dict):
#     if any(_should_fail(m, default_prob=0.05) for m in messages):
#         logger.info(f"[cb_bulk] FAIL batch={len(messages)}")
#         raise RuntimeError("random failure (cb_bulk batch)")
#     logger.info(f"[cb_bulk] OK batch={len(messages)}")
#     out = []
#     for m in messages:
#         out.append(_touch_enrichment(m, "cb_bulk", {"ok": True, "ts": time.time()}))
#     return out


# # ============================================================
# # F) OPTIONAL: combo (rate_limit + circuit_breaker + retry) single + bulk
# # INCA NU AM TESTAT
# # ============================================================
#
# @kafka_handler(
#     name="combo_single",
#     topics_in=["poc.combo.single.in"],
#     topics_out=["poc.combo.single.out"],
#     max_workers=200,
#     bulk_mode=False,
#     metadatas={"worker": "combo_single"},
# )
# @retry_to_dlq(max_attempts=3, dlq_topic="poc.dlq.combo.single", retry_count_field="retry_count")
# @circuit_breaker(failures=5, reset_sec=5)
# @rate_limit(rps=300, burst=300)
# def combo_single(message: dict, consumer_name: str, metadatas: dict) -> dict:
#     mid = message.get("id")
#     if _should_fail(message, default_prob=0.10):
#         logger.info(f"[combo_single] FAIL id={mid} retry_count={message.get('retry_count')}")
#         raise RuntimeError("random failure (combo_single)")
#     logger.info(f"[combo_single] OK id={mid}")
#     return _touch_enrichment(message, "combo_single", {"ok": True, "ts": time.time()})
#
#
# @kafka_handler(
#     name="combo_bulk",
#     topics_in=["poc.combo.bulk.in"],
#     topics_out=["poc.combo.bulk.out"],
#     max_workers=100,
#     bulk_mode=True,
#     batch_size=20,
#     batch_timeout_ms=1500,
#     metadatas={"worker": "combo_bulk", "mode": "bulk"},
# )
# @retry_to_dlq(max_attempts=3, dlq_topic="poc.dlq.combo.bulk", retry_count_field="retry_count")
# @circuit_breaker(failures=3, reset_sec=3)
# @rate_limit(rps=8, burst=8)
# def combo_bulk(messages: list[dict], consumer_name: str, metadatas: dict):
#     if any(_should_fail(m, default_prob=0.08) for m in messages):
#         logger.info(f"[combo_bulk] FAIL batch={len(messages)}")
#         raise RuntimeError("random failure (combo_bulk batch)")
#     logger.info(f"[combo_bulk] OK batch={len(messages)}")
#     out = []
#     for m in messages:
#         out.append(_touch_enrichment(m, "combo_bulk", {"ok": True, "ts": time.time()}))
#     return out
