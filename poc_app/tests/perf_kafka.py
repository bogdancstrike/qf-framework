#!/usr/bin/env python3
"""
Kafka workers performance test.

For each worker: produces N messages to input topic(s), consumes from
output topic until all expected messages arrive, and reports throughput.

Usage:
  python tests/perf_kafka.py [N]   # N messages per worker (default 100_000)
"""
import json
import sys
import threading
import time
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Optional

from kafka import KafkaConsumer, KafkaProducer, TopicPartition

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
BOOTSTRAP = "localhost:9094"
N = int(sys.argv[1]) if len(sys.argv) > 1 else 100_000
FLUSH_EVERY = 5_000
CONSUME_TIMEOUT_SEC = 300   # max wait after produce finishes

WORKERS = [
    {
        "name":   "echo_single",
        "in":     ["poc.echo.single.in"],
        "out":    "poc.echo.single.out",
        "kind":   "single",
        "fail_prob": 0,
    },
    {
        "name":   "echo_bulk",
        "in":     ["poc.echo.bulk.in"],
        "out":    "poc.echo.bulk.out",
        "kind":   "bulk",
        "fail_prob": 0,
    },
    {
        "name":   "retry_single",
        "in":     ["poc.retry.single.in"],
        "out":    "poc.retry.single.out",
        "dlq":    "poc.dlq.retry.single",
        "kind":   "single",
        "fail_prob": 0,   # force success so all land in .out
    },
    {
        "name":   "rl_single",
        "in":     ["poc.rl.single.in"],
        "out":    "poc.rl.single.out",
        "kind":   "single",
        "fail_prob": 0,
    },
    {
        "name":   "rl_bulk",
        "in":     ["poc.rl.bulk.in"],
        "out":    "poc.rl.bulk.out",
        "kind":   "bulk",
        "fail_prob": 0,
    },
    {
        "name":   "agg_basic",
        "in":     ["poc.agg.basic.a", "poc.agg.basic.b"],   # pairs with same id
        "out":    "poc.agg.basic.out",
        "kind":   "aggregator",
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


def _consume_n(topic: str, start_offset: int, expected: int, timeout_sec: float) -> tuple[int, float]:
    """
    Consume from `topic` starting at `start_offset` until `expected` messages
    or timeout. Returns (count_received, elapsed_sec_from_first_msg).
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
                if first_ts is None:
                    first_ts = time.time()
                count += 1
                last_ts = time.time()

    consumer.close()
    elapsed = (last_ts - first_ts) if (first_ts and last_ts and last_ts > first_ts) else 0.001
    return count, elapsed


# ---------------------------------------------------------------------------
# Per-worker test
# ---------------------------------------------------------------------------

@dataclass
class WorkerResult:
    name: str
    n_sent: int = 0
    n_received: int = 0
    produce_sec: float = 0.0
    e2e_sec: float = 0.0        # from first produce to last output
    consume_sec: float = 0.0    # from first output to last output
    status: str = "OK"
    notes: str = ""


def test_worker(w: dict, producer: KafkaProducer) -> WorkerResult:
    name = w["name"]
    in_topics = w["in"]
    out_topic = w["out"]
    kind = w["kind"]
    fail_prob = w.get("fail_prob", 0)
    expected_out = N   # 1 output per input (aggregator: 1 per pair)

    print(f"\n{'='*60}")
    print(f"  Worker: {name}  ({kind})  N={N:,}")
    print(f"{'='*60}")

    # 1. Snapshot end offset of output topic (create it first if needed)
    try:
        out_start = _end_offset(out_topic)
    except Exception:
        out_start = 0
    print(f"  Output topic offset before test: {out_start}")

    # 2. Start consumer thread BEFORE producing
    consume_results = {}
    consume_event = threading.Event()

    def _consumer_thread():
        cnt, elapsed = _consume_n(out_topic, out_start, expected_out, CONSUME_TIMEOUT_SEC)
        consume_results["count"] = cnt
        consume_results["elapsed"] = elapsed
        consume_event.set()

    ct = threading.Thread(target=_consumer_thread, daemon=True)
    ct.start()

    # 3. Produce
    produce_start = time.time()
    n_sent = 0

    if kind == "aggregator":
        # Both parts must arrive; same id ensures aggregation
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
            msg: dict = {"id": f"{name}-{i}", "v": i}
            if fail_prob:
                msg["fail_prob"] = fail_prob
            producer.send(topic, msg)
            n_sent += 1
            if i % FLUSH_EVERY == 0 and i > 0:
                producer.flush()
                print(f"    sent {i:,} ...")

    producer.flush()
    produce_end = time.time()
    produce_sec = produce_end - produce_start
    produce_rps = N / produce_sec

    print(f"  Produced {n_sent:,} msgs in {produce_sec:.2f}s  ({produce_rps:,.0f} msg/s)")
    print(f"  Waiting for {expected_out:,} outputs (timeout {CONSUME_TIMEOUT_SEC}s)...")

    # 4. Wait for consumer
    consume_event.wait(timeout=CONSUME_TIMEOUT_SEC + 5)
    e2e_sec = time.time() - produce_start
    n_received = consume_results.get("count", 0)
    consume_sec = consume_results.get("elapsed", 0.0)

    status = "OK" if n_received >= expected_out else "PARTIAL"
    notes = ""
    if n_received < expected_out:
        notes = f"missing {expected_out - n_received:,} messages"
    if n_received > expected_out:
        notes = f"extra {n_received - expected_out:,} (pre-existing?)"

    out_rps = n_received / max(e2e_sec, 0.001)
    print(f"  Received {n_received:,}/{expected_out:,} outputs")
    print(f"  E2E time: {e2e_sec:.2f}s  |  E2E throughput: {out_rps:,.0f} msg/s")
    if notes:
        print(f"  NOTE: {notes}")

    return WorkerResult(
        name=name,
        n_sent=n_sent,
        n_received=n_received,
        produce_sec=produce_sec,
        e2e_sec=e2e_sec,
        consume_sec=consume_sec,
        status=status,
        notes=notes,
    )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    print(f"\n{'#'*60}")
    print(f"  QF Kafka Workers Performance Test")
    print(f"  Messages per worker: {N:,}")
    print(f"  Bootstrap: {BOOTSTRAP}")
    print(f"{'#'*60}\n")

    producer = _make_producer()
    results: list[WorkerResult] = []

    overall_start = time.time()
    for w in WORKERS:
        r = test_worker(w, producer)
        results.append(r)

    producer.close()
    overall_sec = time.time() - overall_start

    # ---------------------------------------------------------------------------
    # Final Report
    # ---------------------------------------------------------------------------
    print(f"\n\n{'#'*60}")
    print(f"  FINAL REPORT")
    print(f"{'#'*60}")
    print(f"  Total wall time: {overall_sec:.1f}s")
    print(f"  Messages per worker: {N:,}\n")

    col_w = [14, 8, 10, 10, 10, 10, 8, 20]
    header = (
        f"{'Worker':<{col_w[0]}} {'Status':<{col_w[1]}} "
        f"{'Sent':>{col_w[2]}} {'Received':>{col_w[3]}} "
        f"{'Produce/s':>{col_w[4]}} {'E2E/s':>{col_w[5]}} "
        f"{'E2E_rps':>{col_w[6]}} {'Notes':<{col_w[7]}}"
    )
    sep = "-" * sum(col_w)
    print(header)
    print(sep)

    all_ok = True
    for r in results:
        e2e_rps = r.n_received / max(r.e2e_sec, 0.001)
        prod_rps = r.n_sent / max(r.produce_sec, 0.001)
        if r.status != "OK":
            all_ok = False
        print(
            f"{r.name:<{col_w[0]}} {r.status:<{col_w[1]}} "
            f"{r.n_sent:>{col_w[2]},} {r.n_received:>{col_w[3]},} "
            f"{prod_rps:>{col_w[4]},.0f} {r.e2e_sec:>{col_w[5]}.1f} "
            f"{e2e_rps:>{col_w[6]},.0f} {r.notes:<{col_w[7]}}"
        )

    print(sep)
    print(f"\n  Overall: {'ALL PASSED' if all_ok else 'SOME FAILURES'}")
    print(f"\nColumns: Produce/s = input produce rate, E2E/s = seconds, E2E_rps = end-to-end output rate\n")


if __name__ == "__main__":
    main()
