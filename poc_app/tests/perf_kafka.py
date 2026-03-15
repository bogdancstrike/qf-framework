#!/usr/bin/env python3
"""
Kafka workers performance test.

For each worker: produces N messages to input topic(s), consumes from
output topic until all expected messages arrive, and reports throughput
and latency.

Usage:
  python tests/perf_kafka.py [N]   # N messages per worker (default 1_000)
"""
import json
import os
import platform
import sys
import threading
import time
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

from kafka import KafkaConsumer, KafkaProducer, TopicPartition

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
BOOTSTRAP = "localhost:9094"
N = int(sys.argv[1]) if len(sys.argv) > 1 else 1_000
FLUSH_EVERY = max(N // 10, 100)
CONSUME_TIMEOUT_SEC = 120   # max wait after produce finishes

WORKERS = [
    {
        "name":      "echo_single",
        "in":        ["poc.echo.single.in"],
        "out":       "poc.echo.single.out",
        "kind":      "single",
        "fail_prob": 0,
    },
    {
        "name":      "echo_bulk",
        "in":        ["poc.echo.bulk.in"],
        "out":       "poc.echo.bulk.out",
        "kind":      "bulk",
        "fail_prob": 0,
    },
    {
        "name":      "retry_single",
        "in":        ["poc.retry.single.in"],
        "out":       "poc.retry.single.out",
        "dlq":       "poc.dlq.retry.single",
        "kind":      "single",
        "fail_prob": 0,   # force success so all land in .out
    },
    {
        "name":      "rl_single",
        "in":        ["poc.rl.single.in"],
        "out":       "poc.rl.single.out",
        "kind":      "single",
        "fail_prob": 0,
    },
    {
        "name":      "rl_bulk",
        "in":        ["poc.rl.bulk.in"],
        "out":       "poc.rl.bulk.out",
        "kind":      "bulk",
        "fail_prob": 0,
    },
    {
        "name":      "agg_basic",
        "in":        ["poc.agg.basic.a", "poc.agg.basic.b"],   # pairs with same id
        "out":       "poc.agg.basic.out",
        "kind":      "aggregator",
        "fail_prob": 0,
    },
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        linger_ms=5,
        batch_size=64 * 1024,
        acks=1,
    )


def _end_offset(topic: str) -> int:
    """Return the current end offset for partition 0 of a topic."""
    c = KafkaConsumer(bootstrap_servers=BOOTSTRAP, consumer_timeout_ms=2000)
    tp = TopicPartition(topic, 0)
    c.assign([tp])
    c.seek_to_end(tp)
    off = c.position(tp)
    c.close()
    return off


def _consume_n(
    topic: str, start_offset: int, expected: int, timeout_sec: float
) -> tuple[int, float, float]:
    """
    Consume from `topic` starting at `start_offset` until `expected` messages
    or timeout.

    Returns (count_received, first_msg_latency_sec, consume_window_sec).
    """
    consumer = KafkaConsumer(
        bootstrap_servers=BOOTSTRAP,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        consumer_timeout_ms=1000,
    )
    tp = TopicPartition(topic, 0)
    consumer.assign([tp])
    consumer.seek(tp, start_offset)

    count = 0
    first_ts: Optional[float] = None
    last_ts: Optional[float] = None
    deadline = time.time() + timeout_sec

    while count < expected and time.time() < deadline:
        batch = consumer.poll(timeout_ms=500, max_records=5000)
        for _, records in batch.items():
            for _ in records:
                now = time.time()
                if first_ts is None:
                    first_ts = now
                count += 1
                last_ts = now

    consumer.close()
    window = (last_ts - first_ts) if (first_ts and last_ts and last_ts > first_ts) else 0.001
    first_lag = (first_ts - time.time()) if first_ts else 0.0   # not meaningful here; use e2e
    return count, window, first_ts or time.time()


# ---------------------------------------------------------------------------
# Per-worker test
# ---------------------------------------------------------------------------

@dataclass
class WorkerResult:
    name: str
    kind: str
    n_sent: int = 0
    n_received: int = 0
    n_expected: int = 0
    produce_sec: float = 0.0
    e2e_sec: float = 0.0        # produce_start → last output
    consume_window_sec: float = 0.0   # first output → last output
    status: str = "OK"
    notes: str = ""


def test_worker(w: dict, producer: KafkaProducer) -> WorkerResult:
    name = w["name"]
    in_topics = w["in"]
    out_topic = w["out"]
    kind = w["kind"]
    fail_prob = w.get("fail_prob", 0)
    expected_out = N   # 1 output per input (aggregator: 1 per pair)

    print(f"\n{'='*64}")
    print(f"  Worker: {name}  ({kind})  N={N:,}")
    print(f"{'='*64}")

    # 1. Snapshot end offset of output topic
    try:
        out_start = _end_offset(out_topic)
    except Exception:
        out_start = 0
    print(f"  Output topic offset before test : {out_start}")

    # 2. Start consumer thread BEFORE producing
    consume_results: dict = {}
    consume_event = threading.Event()

    def _consumer_thread():
        cnt, window, first_ts = _consume_n(out_topic, out_start, expected_out, CONSUME_TIMEOUT_SEC)
        consume_results["count"] = cnt
        consume_results["window"] = window
        consume_results["first_ts"] = first_ts
        consume_event.set()

    ct = threading.Thread(target=_consumer_thread, daemon=True)
    ct.start()

    # 3. Produce
    produce_start = time.time()
    n_sent = 0

    if kind == "aggregator":
        for i in range(N):
            mid = f"{name}-{i}"
            producer.send(in_topics[0], {"id": mid, "a": f"A{i}"})
            producer.send(in_topics[1], {"id": mid, "b": f"B{i}"})
            n_sent += 1
            if i % FLUSH_EVERY == 0 and i > 0:
                producer.flush()
                print(f"    sent {i:,} pairs ...")
    else:
        topic = in_topics[0]
        for i in range(N):
            # Always include fail_prob so workers don't fall back to their default failure rate.
            # fail_prob=0 means "force success" in this perf test.
            msg: dict = {"id": f"{name}-{i}", "v": i, "fail_prob": fail_prob}
            producer.send(topic, msg)
            n_sent += 1
            if i % FLUSH_EVERY == 0 and i > 0:
                producer.flush()
                print(f"    sent {i:,} ...")

    producer.flush()
    produce_end = time.time()
    produce_sec = produce_end - produce_start
    produce_rps = N / max(produce_sec, 0.001)

    print(f"  Produced {n_sent:,} msgs in {produce_sec:.2f}s  ({produce_rps:,.0f} msg/s)")
    print(f"  Waiting for {expected_out:,} outputs (timeout {CONSUME_TIMEOUT_SEC}s)...")

    # 4. Wait for consumer
    consume_event.wait(timeout=CONSUME_TIMEOUT_SEC + 5)
    e2e_sec = time.time() - produce_start
    n_received = consume_results.get("count", 0)
    consume_window = consume_results.get("window", 0.0)

    status = "OK" if n_received >= expected_out else "PARTIAL"
    notes = ""
    if n_received < expected_out:
        notes = f"missing {expected_out - n_received:,}"
    elif n_received > expected_out:
        notes = f"+{n_received - expected_out:,} extra (pre-existing?)"

    out_rps = n_received / max(e2e_sec, 0.001)
    consume_rps = n_received / max(consume_window, 0.001)
    print(f"  Received {n_received:,}/{expected_out:,} outputs")
    print(f"  E2E: {e2e_sec:.2f}s  |  E2E throughput: {out_rps:,.0f} msg/s")
    print(f"  Consume window: {consume_window:.2f}s  |  Output rate: {consume_rps:,.0f} msg/s")
    if notes:
        print(f"  NOTE: {notes}")

    return WorkerResult(
        name=name,
        kind=kind,
        n_sent=n_sent,
        n_received=n_received,
        n_expected=expected_out,
        produce_sec=produce_sec,
        e2e_sec=e2e_sec,
        consume_window_sec=consume_window,
        status=status,
        notes=notes,
    )


# ---------------------------------------------------------------------------
# Helpers for report
# ---------------------------------------------------------------------------

def _bar(value: float, max_value: float, width: int = 16) -> str:
    filled = int(round(value / max_value * width)) if max_value > 0 else 0
    return "[" + "#" * filled + "." * (width - filled) + "]"


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    run_ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    try:
        cpu_count = os.cpu_count() or "?"
    except Exception:
        cpu_count = "?"

    print(f"\n{'#'*64}")
    print(f"  QF Framework — Kafka Workers Performance Test")
    print(f"{'#'*64}")
    print(f"  Date           : {run_ts}")
    print(f"  Python         : {platform.python_version()}")
    print(f"  CPUs           : {cpu_count}")
    print(f"  Bootstrap      : {BOOTSTRAP}")
    print(f"  Msgs / worker  : {N:,}")
    print(f"  Workers        : {len(WORKERS)}")
    print(f"{'#'*64}\n")

    producer = _make_producer()
    results: list[WorkerResult] = []

    overall_start = time.time()
    for w in WORKERS:
        r = test_worker(w, producer)
        results.append(r)

    producer.close()
    overall_sec = time.time() - overall_start

    # -----------------------------------------------------------------------
    # Final Report
    # -----------------------------------------------------------------------
    print(f"\n\n{'#'*64}")
    print(f"  FINAL REPORT — QF Kafka Workers Performance Test")
    print(f"{'#'*64}")
    print(f"  Date             : {run_ts}")
    print(f"  Bootstrap        : {BOOTSTRAP}")
    print(f"  Total wall time  : {overall_sec:.1f}s")
    print(f"  Messages/worker  : {N:,}")
    print(f"  Workers tested   : {len(results)}\n")

    # ---- main table ----
    col = [14, 11, 8, 9, 10, 10, 10, 10, 16]
    hdr = (
        f"{'Worker':<{col[0]}} {'Kind':<{col[1]}} {'Status':<{col[2]}} "
        f"{'Sent':>{col[3]}} {'Received':>{col[4]}} "
        f"{'Prod msg/s':>{col[5]}} {'E2E s':>{col[6]}} {'Out msg/s':>{col[7]}} "
        f"{'Notes':<{col[8]}}"
    )
    sep = "-" * sum(col)
    print(hdr)
    print(sep)

    all_ok = True
    max_e2e = max((r.e2e_sec for r in results), default=1.0)
    for r in results:
        prod_rps = r.n_sent / max(r.produce_sec, 0.001)
        out_rps = r.n_received / max(r.e2e_sec, 0.001)
        if r.status != "OK":
            all_ok = False
        flag = "" if r.status == "OK" else "✗"
        print(
            f"{r.name:<{col[0]}} {r.kind:<{col[1]}} {(r.status + flag):<{col[2]}} "
            f"{r.n_sent:>{col[3]},} {r.n_received:>{col[4]},} "
            f"{prod_rps:>{col[5]},.0f} {r.e2e_sec:>{col[6]}.2f} {out_rps:>{col[7]},.0f} "
            f"{r.notes:<{col[8]}}"
        )
    print(sep)

    # ---- E2E time visual comparison ----
    print(f"\n  E2E time comparison (lower = faster):\n")
    for r in results:
        bar = _bar(r.e2e_sec, max_e2e, 28)
        status_flag = "✓" if r.status == "OK" else "✗"
        out_rps = r.n_received / max(r.e2e_sec, 0.001)
        print(f"  {r.name:<14} {bar} {r.e2e_sec:5.2f}s  {out_rps:>8,.0f} msg/s  {status_flag}")

    # ---- completion summary ----
    total_sent = sum(r.n_sent for r in results)
    total_recv = sum(r.n_received for r in results)
    total_expected = sum(r.n_expected for r in results)
    n_ok = sum(1 for r in results if r.status == "OK")
    n_fail = len(results) - n_ok

    print(f"\n  Completion summary:")
    print(f"    Workers tested  : {len(results)}")
    print(f"    Passed          : {n_ok}  |  Failed : {n_fail}")
    print(f"    Total sent      : {total_sent:,}")
    print(f"    Total received  : {total_recv:,}  /  {total_expected:,} expected")
    lag = total_expected - total_recv
    if lag > 0:
        print(f"    Total missing   : {lag:,}")
    print(f"    Wall time       : {overall_sec:.1f}s")
    print(f"\n  Column guide:")
    print(f"    Prod msg/s = input produce rate  |  E2E s = produce_start → last output")
    print(f"    Out msg/s  = output receive rate (E2E throughput)")
    print(f"\n  Overall: {'ALL PASSED ✓' if all_ok else 'SOME FAILURES ✗'}\n")


if __name__ == "__main__":
    main()
