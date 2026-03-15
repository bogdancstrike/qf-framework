#!/usr/bin/env python3
"""
HTTP endpoints performance test.

Sends N concurrent requests to each exposed worker endpoint and reports
throughput, latency percentiles, and success rate.

Usage:
  python tests/perf_http.py [N] [concurrency]
    N           total requests per endpoint (default 100_000)
    concurrency worker threads (default 50)
"""
import json
import statistics
import sys
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from typing import Optional

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
BASE_URL = "http://localhost:5000"
N = int(sys.argv[1]) if len(sys.argv) > 1 else 100_000
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
    """Returns (http_status, latency_sec). Returns (0, elapsed) on error."""
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
    n_total: int = 0
    n_ok: int = 0
    n_error: int = 0
    total_sec: float = 0.0
    latencies: list = field(default_factory=list)
    status: str = "OK"


def test_endpoint(ep: dict) -> EndpointResult:
    name = ep["name"]
    url = ep["url"]
    payload = ep["payload"]

    print(f"\n{'='*60}")
    print(f"  Endpoint: POST {url}")
    print(f"  Requests: {N:,}  |  Concurrency: {CONCURRENCY}")
    print(f"{'='*60}")

    result = EndpointResult(name=name, n_total=N)
    latencies: list[float] = []
    n_ok = 0
    n_error = 0
    progress_lock = __import__("threading").Lock()
    progress = [0]

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
            if 200 <= status < 300:
                n_ok += 1
            else:
                n_error += 1
            with progress_lock:
                progress[0] += 1
                if progress[0] % 10_000 == 0:
                    elapsed = time.time() - start_ts
                    rps = progress[0] / max(elapsed, 0.001)
                    print(f"    {progress[0]:,} / {N:,}  ({rps:,.0f} req/s) ...")

    total_sec = time.time() - start_ts

    result.n_ok = n_ok
    result.n_error = n_error
    result.total_sec = total_sec
    result.latencies = sorted(latencies)
    result.status = "OK" if n_error == 0 else "ERRORS"

    rps = N / max(total_sec, 0.001)
    p50 = statistics.median(latencies) * 1000
    p95 = latencies[int(len(latencies) * 0.95)] * 1000
    p99 = latencies[int(len(latencies) * 0.99)] * 1000
    p_mean = statistics.mean(latencies) * 1000

    print(f"  Done: {n_ok:,} OK / {n_error:,} errors  in {total_sec:.2f}s")
    print(f"  Throughput: {rps:,.0f} req/s")
    print(f"  Latency (ms): mean={p_mean:.1f}  p50={p50:.1f}  p95={p95:.1f}  p99={p99:.1f}")

    return result


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    print(f"\n{'#'*60}")
    print(f"  QF HTTP Endpoints Performance Test")
    print(f"  Requests per endpoint: {N:,}")
    print(f"  Concurrency: {CONCURRENCY} threads")
    print(f"  Base URL: {BASE_URL}")
    print(f"{'#'*60}\n")

    results: list[EndpointResult] = []
    overall_start = time.time()

    for ep in ENDPOINTS:
        r = test_endpoint(ep)
        results.append(r)

    overall_sec = time.time() - overall_start

    # ---------------------------------------------------------------------------
    # Final Report
    # ---------------------------------------------------------------------------
    print(f"\n\n{'#'*60}")
    print(f"  FINAL REPORT")
    print(f"{'#'*60}")
    print(f"  Total wall time: {overall_sec:.1f}s")
    print(f"  Requests per endpoint: {N:,}  |  Concurrency: {CONCURRENCY}\n")

    col_w = [12, 8, 10, 8, 10, 10, 10, 10]
    header = (
        f"{'Endpoint':<{col_w[0]}} {'Status':<{col_w[1]}} "
        f"{'Requests':>{col_w[2]}} {'Errors':>{col_w[3]}} "
        f"{'req/s':>{col_w[4]}} {'mean ms':>{col_w[5]}} "
        f"{'p95 ms':>{col_w[6]}} {'p99 ms':>{col_w[7]}}"
    )
    sep = "-" * sum(col_w)
    print(header)
    print(sep)

    all_ok = True
    for r in results:
        if not r.latencies:
            continue
        rps = r.n_total / max(r.total_sec, 0.001)
        p_mean = statistics.mean(r.latencies) * 1000
        p95 = r.latencies[int(len(r.latencies) * 0.95)] * 1000
        p99 = r.latencies[int(len(r.latencies) * 0.99)] * 1000
        if r.status != "OK":
            all_ok = False
        print(
            f"{r.name:<{col_w[0]}} {r.status:<{col_w[1]}} "
            f"{r.n_total:>{col_w[2]},} {r.n_error:>{col_w[3]},} "
            f"{rps:>{col_w[4]},.0f} {p_mean:>{col_w[5]}.1f} "
            f"{p95:>{col_w[6]}.1f} {p99:>{col_w[7]}.1f}"
        )

    print(sep)
    print(f"\n  Overall: {'ALL PASSED' if all_ok else 'SOME FAILURES'}\n")


if __name__ == "__main__":
    main()
