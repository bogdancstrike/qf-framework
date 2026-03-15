# QF Framework — Operational Flows

All major execution paths illustrated with Mermaid diagrams.

---

## 1. Application Startup

```mermaid
sequenceDiagram
    participant App as app.py
    participant FW as FrameworkApp
    participant Reg as Worker Registry
    participant ETL as ETL Runtime
    participant Flask as Flask/RESTX

    App->>FW: FrameworkApp(settings).run()
    FW->>FW: Import worker_modules
    Note over FW,Reg: @kafka_handler / @kafka_aggregator decorators fire
    FW->>Reg: Register WorkerSpec per decorated function
    FW->>ETL: start(worker_modules, bootstrap_servers) in daemon thread
    ETL->>ETL: create_kafka_consumer(all_topics)
    ETL->>ETL: create_kafka_producer()
    ETL->>ETL: Create ThreadPoolExecutor per worker
    ETL->>ETL: Enter main poll loop
    FW->>Flask: create_app() with dynamic endpoints
    Flask->>Flask: Load endpoint.json → generate Namespaces/Resources
    FW-->>App: FrameworkHandles(app, thread)
    App->>Flask: app.run(host, port)
```

---

## 2. Kafka Message — Single Worker (commit_strategy="before")

```mermaid
sequenceDiagram
    participant Kafka as Kafka Broker
    participant Poll as Poll Loop
    participant CC as CommitCoordinator
    participant Pool as ThreadPoolExecutor
    participant W as Worker Function
    participant Out as Output Topic

    Kafka-->>Poll: consumer.poll() → records batch
    Poll->>Poll: For each record in TP
    Poll->>CC: (strategy=before) commit offset+1 immediately
    Poll->>Pool: pool.submit(_job, record)
    Pool->>W: worker_fn(message, consumer_name, metadatas)
    W-->>Pool: return enriched message
    Pool->>Out: _send_async(producer, topic, result)
```

---

## 3. Kafka Message — Single Worker (commit_strategy="after_success")

```mermaid
sequenceDiagram
    participant Kafka as Kafka Broker
    participant Poll as Poll Loop
    participant CC as CommitCoordinator
    participant Pool as ThreadPoolExecutor
    participant W as Worker Function
    participant Out as Output Topic

    Kafka-->>Poll: consumer.poll() → records batch
    Poll->>CC: init_tp(tp, first_offset) [sets sliding window base]
    Poll->>Pool: pool.submit(_job, record)
    Pool->>W: worker_fn(message, consumer_name, metadatas)
    W-->>Pool: return enriched message
    Pool->>Out: _send_async(producer, topic, result)
    Pool->>CC: mark_done(tp, offset)
    Note over CC: Sliding window advances only when contiguous offsets done
    CC->>CC: try_commit() — batch commits within tick window
    CC->>Kafka: commit_offsets({tp: next_contiguous_offset})
```

---

## 4. Kafka Message — Bulk Worker

```mermaid
sequenceDiagram
    participant Kafka as Kafka Broker
    participant Poll as Poll Loop
    participant Buf as Batch Buffer (deque per TP)
    participant Pool as ThreadPoolExecutor
    participant W as Worker Function (batch)
    participant Out as Output Topic

    Kafka-->>Poll: consumer.poll() → records
    Poll->>Buf: buffer.append(record) per TP
    Poll->>Poll: Check: buffer.size >= batch_size OR timeout elapsed?
    alt Batch ready
        Poll->>Pool: pool.submit(_job, batch_list)
        Pool->>W: worker_fn(messages: list, consumer_name, metadatas)
        W-->>Pool: return list of enriched messages
        Pool->>Out: _send_async per output message
        alt KAFKA_BULK_OUTPUT_SYNC=True
            Pool->>Pool: _wait_futures(futures) — ACK-gate all outputs
        end
    end
```

---

## 5. Aggregator Flow (Redis + Lua)

```mermaid
sequenceDiagram
    participant KA as Topic A
    participant KB as Topic B
    participant Poll as Poll Loop
    participant Redis as Redis (Lua script)
    participant Pool as ThreadPoolExecutor
    participant W as Aggregator Function
    participant Out as Output Topic

    KA-->>Poll: record {id: "x", part: "a", val_a: 1}
    Poll->>Redis: HSET agg_basic:x part_a={...}; check all parts present?
    Redis-->>Poll: incomplete (missing part_b) → None

    KB-->>Poll: record {id: "x", part: "b", val_b: 10}
    Poll->>Redis: HSET agg_basic:x part_b={...}; check all parts present?
    Redis-->>Poll: COMPLETE → merged = deep_merge(part_a, part_b); DEL key
    Poll->>Pool: pool.submit(_job, merged_record)
    Pool->>W: worker_fn(merged, consumer_name, metadatas)
    W-->>Pool: return enriched merged record
    Pool->>Out: _send_async(producer, out_topic, result)
```

---

## 6. Retry to DLQ Flow

```mermaid
sequenceDiagram
    participant Kafka as Input Topic
    participant Poll as Poll Loop
    participant Pool as ThreadPoolExecutor
    participant W as Worker Function
    participant RQ as Requeue (same topic)
    participant DLQ as DLQ Topic

    Kafka-->>Poll: record {id: "x", retry_count: 0}
    Poll->>Pool: pool.submit(_job, record)
    Pool->>W: worker_fn(message) → raises RuntimeError
    Pool->>Pool: _handle_retry_to_dlq(record, error, retry_config)
    alt retry_count < max_attempts
        Pool->>RQ: produce {id: "x", retry_count: 1}
    else retry_count >= max_attempts
        Pool->>DLQ: _send_sync(producer, dlq_topic, record)
        Note over DLQ: Message with all retry history stored
    end
```

---

## 7. Rate Limit Flow

```mermaid
flowchart TD
    A[Poll Loop tick] --> B{Worker has @rate_limit?}
    B -- No --> E[Normal dispatch]
    B -- Yes --> C{Token bucket has tokens?}
    C -- Yes --> D[Consume 1 token\nDispatch job to pool]
    C -- No --> F[Pause worker TPs\nSkip dispatch this tick]
    F --> G[Next tick: refill tokens at rps/s]
    G --> C
    D --> H[Worker executes normally]
```

---

## 8. Circuit Breaker Flow

```mermaid
flowchart TD
    A[Job completes] --> B{Success?}
    B -- Yes --> C[Reset failure counter to 0]
    B -- No --> D[Increment consecutive_failures]
    D --> E{consecutive_failures >= threshold?}
    E -- No --> F[Normal operation continues]
    E -- Yes --> G[OPEN: pause all worker TPs]
    G --> H[Wait reset_sec]
    H --> I[HALF-OPEN: resume TPs]
    I --> J[Next job attempt]
    J --> B
```

---

## 9. HTTP API Request Flow

```mermaid
sequenceDiagram
    participant Client as HTTP Client
    participant Flask as Flask/RESTX
    participant NS as Namespace Resource
    participant EF as api_endpoints.py handler
    participant WF as Worker Function (direct call)
    participant Resp as JSON Response

    Client->>Flask: POST /workers/ner {id: "1", text: "..."}
    Flask->>NS: Route to NerResource.post()
    NS->>EF: worker_ner(app, operation, request)
    EF->>WF: echo_single(payload, consumer_name="api", metadatas={"via": "http"})
    WF-->>EF: enriched_message
    EF-->>NS: jsonify(enriched_message)
    NS-->>Client: 200 OK {id: "1", enrichment: {...}}
```

---

## 10. Backpressure Mechanism

```mermaid
flowchart LR
    subgraph "Per-worker backpressure"
        A[Poll records] --> B{pending_jobs[tp] >= MAX?}
        B -- Yes --> C[consumer.pause(tp)\nSkip dispatching]
        B -- No --> D[Dispatch to pool]
        D --> E[Job completes]
        E --> F{pending_jobs[tp] < MAX/2?}
        F -- Yes --> G[consumer.resume(tp)]
        F -- No --> H[Keep paused]
    end
```

---

## 11. Worker Chaining (Pipeline Pattern)

```mermaid
flowchart LR
    subgraph "Multi-stage pipeline"
        T1[raw.events] --> W1["Worker A\n(normalize)"]
        W1 --> T2[normalized.events]
        T2 --> W2["Worker B\n(enrich)"]
        W2 --> T3[enriched.events]
        T3 --> W3["Worker C\n(aggregate)"]
        W3 --> T4[final.output]
    end
    Note["Single ETL process\nhosts all workers\nKafka provides buffering\nbetween stages"]
```

---

## 12. Full System Overview

```mermaid
flowchart TB
    subgraph "External"
        P[Producers / upstream services]
        C[Consumers / downstream services]
        HC[HTTP Clients]
    end

    subgraph "Docker Infrastructure"
        K[Kafka:9094]
        KUI[kafka-ui:8081]
        R[Redis:6379]
        PG[Postgres:5432]
    end

    subgraph "QF App Process"
        subgraph "ETL Thread (daemon)"
            EL[Poll Loop]
            REG[Worker Registry]
            CC[CommitCoordinator]
            subgraph "Workers"
                WE[echo_single pool×100]
                WEB[echo_bulk pool×100]
                WA[agg_basic pool×100]
                WR[retry_single pool×100]
                WRL[rl_single pool×100]
                WRB[rl_bulk pool×100]
            end
        end
        subgraph "HTTP Server (main thread)"
            FL[Flask:5000]
            SW[/swagger-ui]
        end
    end

    P --> K
    K --> EL
    EL --> REG
    REG --> WE & WEB & WA & WR & WRL & WRB
    WA <--> R
    WE & WEB & WA & WR & WRL & WRB --> K
    K --> C
    EL --> CC
    CC --> K

    HC --> FL
    FL --> WE & WRL & WA

    KUI -.-> K
```
