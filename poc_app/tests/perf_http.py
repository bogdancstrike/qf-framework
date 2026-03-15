#!/usr/bin/env python3
"""
HTTP endpoints performance test.

Sends N concurrent requests to each exposed worker endpoint and reports
throughput, latency percentiles, error breakdown, and system context.

Usage:
  python tests/perf_http.py [N] [concurrency]
    N           total requests per endpoint (default 1_000)
    concurrency worker threads (default 50)
"""
import json
import os
import platform
import statistics
import sys
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
BASE_URL = "http://localhost:5000"
N = int(sys.argv[1]) if len(sys.argv) > 1 else 1_000
CONCURRENCY = int(sys.argv[2]) if len(sys.argv) > 2 else 50

ENDPOINTS = [
    {
        "name":    "ner",
        "url":     f"{BASE_URL}/workers/ner",
        "payload": {"id": "x", "text": "John Smith visited Paris"},
    },
    {
        "name":    "translate",
        "url":     f"{BASE_URL}/workers/translate",
        "payload": {"id": "x", "text": "Hello world"},
    },
    {
        "name":    "sentiment",
        "url":     f"{BASE_URL}/workers/sentiment",
        "payload": {"id": "x", "part_a": {"score": 0.9}, "part_b": {"label": "positive"}},
    },
]


# ---------------------------------------------------------------------------
# Single request
# ---------------------------------------------------------------------------

def _post(url: str, payload: dict) -> tuple[int, float]:
    """Returns (http_status, latency_sec). Returns (0, elapsed) on network error."""
    body = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        url, data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    t0 = time.perf_counter()
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            resp.read()
            status = resp.status
    except urllib.error.HTTPError as e:
        status = e.code
    except Exception:
        status = 0
    return status, time.perf_counter() - t0


# ---------------------------------------------------------------------------
# Per-endpoint test
# ---------------------------------------------------------------------------

@dataclass
class EndpointResult:
    name: str
    url: str
    n_total: int = 0
    n_ok: int = 0
    n_error: int = 0
    total_sec: float = 0.0
    latencies: list = field(default_factory=list)
    status_counts: dict = field(default_factory=dict)
    status: str = "OK"


def test_endpoint(ep: dict) -> EndpointResult:
    name = ep["name"]
    url = ep["url"]
    payload = ep["payload"]

    print(f"\n{'='*64}")
    print(f"  Endpoint: POST {url}")
    print(f"  Requests: {N:,}  |  Concurrency: {CONCURRENCY} threads")
    print(f"{'='*64}")

    result = EndpointResult(name=name, url=url, n_total=N)
    latencies: list[float] = []
    status_counts: dict[int, int] = {}
    n_ok = 0
    n_error = 0
    progress_lock = __import__("threading").Lock()
    progress = [0]
    report_every = max(N // 5, 100)

    def _task(i: int) -> tuple[int, float]:
        msg = dict(payload)
        msg["id"] = f"{name}-{i}"
        return _post(url, msg)

    start_ts = time.time()

    with ThreadPoolExecutor(max_workers=CONCURRENCY) as pool:
        futures = {pool.submit(_task, i): i for i in range(N)}
        for fut in as_completed(futures):
            status, lat = fut.result()
            latencies.append(lat)
            status_counts[status] = status_counts.get(status, 0) + 1
            if 200 <= status < 300:
                n_ok += 1
            else:
                n_error += 1
            with progress_lock:
                progress[0] += 1
                if progress[0] % report_every == 0:
                    elapsed = time.time() - start_ts
                    rps = progress[0] / max(elapsed, 0.001)
                    print(f"    {progress[0]:>{len(str(N))},} / {N:,}  ({rps:,.0f} req/s) ...")

    total_sec = time.time() - start_ts

    result.n_ok = n_ok
    result.n_error = n_error
    result.total_sec = total_sec
    result.latencies = sorted(latencies)
    result.status_counts = status_counts
    result.status = "OK" if n_error == 0 else "ERRORS"

    rps = N / max(total_sec, 0.001)
    lats_ms = [l * 1000 for l in latencies]
    p50 = statistics.median(lats_ms)
    p95 = lats_ms[int(len(lats_ms) * 0.95)]
    p99 = lats_ms[int(len(lats_ms) * 0.99)]
    p_mean = statistics.mean(lats_ms)
    p_min = min(lats_ms)
    p_max = max(lats_ms)

    print(f"  Done in {total_sec:.2f}s: {n_ok:,} OK / {n_error:,} errors")
    print(f"  Throughput : {rps:,.1f} req/s")
    print(f"  Latency ms : min={p_min:.1f}  mean={p_mean:.1f}  p50={p50:.1f}  p95={p95:.1f}  p99={p99:.1f}  max={p_max:.1f}")
    if status_counts:
        counts_str = "  ".join(f"HTTP {k}: {v:,}" for k, v in sorted(status_counts.items()))
        print(f"  Status codes: {counts_str}")

    return result


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _pct(lats_ms: list[float], p: float) -> float:
    if not lats_ms:
        return 0.0
    idx = int(len(lats_ms) * p / 100)
    return lats_ms[min(idx, len(lats_ms) - 1)]


def _bar(value: float, max_value: float, width: int = 20) -> str:
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
    print(f"  QF Framework — HTTP Endpoints Performance Test")
    print(f"{'#'*64}")
    print(f"  Date        : {run_ts}")
    print(f"  Python      : {platform.python_version()}")
    print(f"  CPUs        : {cpu_count}")
    print(f"  Base URL    : {BASE_URL}")
    print(f"  N / endpoint: {N:,}")
    print(f"  Concurrency : {CONCURRENCY} threads")
    print(f"  Endpoints   : {len(ENDPOINTS)}")
    print(f"{'#'*64}\n")

    results: list[EndpointResult] = []
    overall_start = time.time()

    for ep in ENDPOINTS:
        r = test_endpoint(ep)
        results.append(r)

    overall_sec = time.time() - overall_start

    # -----------------------------------------------------------------------
    # Final Report
    # -----------------------------------------------------------------------
    print(f"\n\n{'#'*64}")
    print(f"  FINAL REPORT — QF HTTP Performance Test")
    print(f"{'#'*64}")
    print(f"  Date             : {run_ts}")
    print(f"  Total wall time  : {overall_sec:.1f}s")
    print(f"  Requests/endpoint: {N:,}  |  Concurrency: {CONCURRENCY}")
    print(f"  Total requests   : {N * len(results):,}\n")

    # ---- throughput table ----
    col = [13, 7, 9, 7, 9, 8, 8, 8, 8, 8]
    hdr = (
        f"{'Endpoint':<{col[0]}} {'Status':<{col[1]}} "
        f"{'Requests':>{col[2]}} {'Errors':>{col[3]}} "
        f"{'req/s':>{col[4]}} {'min ms':>{col[5]}} {'mean ms':>{col[6]}} "
        f"{'p50 ms':>{col[7]}} {'p95 ms':>{col[8]}} {'p99 ms':>{col[9]}}"
    )
    sep = "-" * sum(col)
    print(hdr)
    print(sep)

    all_ok = True
    for r in results:
        if not r.latencies:
            continue
        lats_ms = [l * 1000 for l in r.latencies]
        rps = r.n_total / max(r.total_sec, 0.001)
        p_mean = statistics.mean(lats_ms)
        p50 = statistics.median(lats_ms)
        p95 = _pct(lats_ms, 95)
        p99 = _pct(lats_ms, 99)
        p_min = min(lats_ms)
        if r.status != "OK":
            all_ok = False
        print(
            f"{r.name:<{col[0]}} {r.status:<{col[1]}} "
            f"{r.n_total:>{col[2]},} {r.n_error:>{col[3]},} "
            f"{rps:>{col[4]},.1f} {p_min:>{col[5]}.1f} {p_mean:>{col[6]}.1f} "
            f"{p50:>{col[7]}.1f} {p95:>{col[8]}.1f} {p99:>{col[9]}.1f}"
        )
    print(sep)

    # ---- latency distribution (histogram buckets) ----
    print(f"\n  Latency distribution (ms):\n")
    buckets_ms = [1, 2, 5, 10, 20, 50, 100, 200, 500, float("inf")]
    bucket_labels = ["<1", "<2", "<5", "<10", "<20", "<50", "<100", "<200", "<500", "≥500"]

    for r in results:
        if not r.latencies:
            continue
        lats_ms = [l * 1000 for l in r.latencies]
        total = len(lats_ms)
        print(f"  [{r.name}]")
        prev = 0.0
        for label, cap in zip(bucket_labels, buckets_ms):
            count = sum(1 for l in lats_ms if prev <= l < cap)
            pct = count / total * 100
            bar = _bar(count, total * 0.5, 24)
            print(f"    {label:>5} ms  {bar} {count:>{len(str(total))},}  ({pct:5.1f}%)")
            prev = cap

    # ---- status code breakdown ----
    has_errors = any(r.n_error > 0 for r in results)
    if has_errors:
        print(f"\n  Status code breakdown:")
        for r in results:
            if r.status_counts:
                counts_str = "  ".join(f"HTTP {k}: {v:,}" for k, v in sorted(r.status_counts.items()))
                print(f"    {r.name}: {counts_str}")

    # ---- summary ----
    total_reqs = sum(r.n_total for r in results)
    total_ok = sum(r.n_ok for r in results)
    total_errors = sum(r.n_error for r in results)
    combined_rps = total_reqs / max(overall_sec, 0.001)
    print(f"\n  Summary:")
    print(f"    Total requests : {total_reqs:,}")
    print(f"    Successful     : {total_ok:,}  ({total_ok/max(total_reqs,1)*100:.1f}%)")
    print(f"    Errors         : {total_errors:,}  ({total_errors/max(total_reqs,1)*100:.1f}%)")
    print(f"    Combined req/s : {combined_rps:,.1f}")
    print(f"\n  Overall: {'ALL PASSED ✓' if all_ok else 'SOME FAILURES ✗'}\n")


if __name__ == "__main__":
    main()
