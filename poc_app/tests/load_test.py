# load_test.py
import json
import time
from kafka import KafkaProducer

BOOTSTRAP = "localhost:9094"

TOPICS = {
    # ONLY kafka_handler
    "echo_single": "poc.echo.single.in",
    "echo_bulk": "poc.echo.bulk.in",

    # ONLY kafka_aggregator
    "agg_basic_a": "poc.agg.basic.a",
    "agg_basic_b": "poc.agg.basic.b",

    # retry_to_dlq
    "retry_single": "poc.retry.single.in",

    # rate limit
    "rl_single": "poc.rl.single.in",
    "rl_bulk": "poc.rl.bulk.in"
}

TOTAL_MESSAGES = 250_000
FLUSH_EVERY = 500
SLEEP_BETWEEN = 0.0

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

def send(topic: str, payload: dict):
    producer.send(topic, payload)

print(f"🚀 Starting load test: {TOTAL_MESSAGES} messages / stream")

start_ts = time.time()

# ------------------------------------------------------------
# Helper: periodic flush + progress
# ------------------------------------------------------------
def maybe_flush(i: int, label: str):
    if i % FLUSH_EVERY == 0:
        producer.flush()
        print(f"  {label} sent: {i}")
    if SLEEP_BETWEEN:
        time.sleep(SLEEP_BETWEEN)

# ------------------------------------------------------------
# 1) ONLY kafka_handler tests
# ------------------------------------------------------------
print("➡️ Producing echo_single...")
for i in range(TOTAL_MESSAGES):
    send(TOPICS["echo_single"], {"id": f"echo-single-{i}", "payload": f"x{i}"})
    maybe_flush(i, "echo_single")
producer.flush()
print("✅ echo_single done")

print("➡️ Producing echo_bulk...")
for i in range(TOTAL_MESSAGES):
    send(TOPICS["echo_bulk"], {"id": f"echo-bulk-{i}", "payload": f"x{i}"})
    maybe_flush(i, "echo_bulk")
producer.flush()
print("✅ echo_bulk done")

# ------------------------------------------------------------
# 2) ONLY kafka_aggregator tests
# ------------------------------------------------------------
print("➡️ Producing agg_basic (A+B pairs)...")
for i in range(TOTAL_MESSAGES):
    mid = f"agg-basic-{i}"
    send(TOPICS["agg_basic_a"], {"id": mid, "a": f"A{i}"})
    send(TOPICS["agg_basic_b"], {"id": mid, "b": f"B{i}"})
    maybe_flush(i, "agg_basic pairs")
producer.flush()
print("✅ agg_basic done")

# ------------------------------------------------------------
# 3) retry_to_dlq tests
#    - deterministic failures: every 50th message force_fail=True
# ------------------------------------------------------------
print("➡️ Producing retry_single...")
for i in range(TOTAL_MESSAGES):
    send(
        TOPICS["retry_single"],
        {
            "id": f"retry-single-{i}",
            "text": f"retry me {i}",
            "force_fail": (i % 50 == 0),
            # optional: probabilistic too
            "fail_prob": 0.10,
        },
    )
    maybe_flush(i, "retry_single")
producer.flush()
print("✅ retry_single done")

# ------------------------------------------------------------
# 4) rate_limit tests
# ------------------------------------------------------------
print("➡️ Producing rl_single...")
for i in range(TOTAL_MESSAGES):
    send(TOPICS["rl_single"], {"id": f"rl-single-{i}", "text": f"rl {i}"})
    maybe_flush(i, "rl_single")
producer.flush()
print("✅ rl_single done")

print("➡️ Producing rl_bulk...")
for i in range(TOTAL_MESSAGES):
    send(TOPICS["rl_bulk"], {"id": f"rl-bulk-{i}", "text": f"rl bulk {i}"})
    maybe_flush(i, "rl_bulk")
producer.flush()
print("✅ rl_bulk done")

# ------------------------------------------------------------
# 5) finished
# ------------------------------------------------------------

end_ts = time.time()
print("🎉 Load test finished")
print(f"⏱️ Total time: {end_ts - start_ts:.2f}s")
print(f"📊 Per stream messages: {TOTAL_MESSAGES}")
print("📌 NOTE: aggregator streams send 2 messages per id (A+B)")

producer.close()
