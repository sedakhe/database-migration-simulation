# database-migration-simulation
Simulating a database migration scenario where a source system emits Change Data Capture (CDC) events to Kafka. Apache Flink consumes the events, **normalizes, deduplicates, and enriches** them, and finally sinks them into a target Postgres database.
Stack used: Apache Flink + Kafka + Postgres
 
---

## Quickstart

1. **Start infrastructure (Kafka, Flink, Postgres, Adminer):**
   ```bash
   cd docker
   docker compose up -d
   ```
   - Flink UI → http://localhost:8081  
   - Adminer UI (Postgres GUI) → http://localhost:8080

---

2. **Initialize Postgres schema (create table + function):**
   ```bash
   docker exec -i docker-postgres-1      psql -U app -d appdb < /Users/s0e04ij/database-migration-simulation/src/schemas/postgres.sql
   ```

---

3. **Run Flink setup and launch job (register tables, views, sink):**
   ```bash
   docker exec -it docker-jobmanager-1 /opt/flink/bin/sql-client.sh -f /opt/flink/usql/01_set_checkpoint.sql
   docker exec -it docker-jobmanager-1 /opt/flink/bin/sql-client.sh -f /opt/flink/usql/02_create_users_cdc_raw.sql
   docker exec -it docker-jobmanager-1 /opt/flink/bin/sql-client.sh -f /opt/flink/usql/03_create_users_changes.sql
   docker exec -it docker-jobmanager-1 /opt/flink/bin/sql-client.sh -f /opt/flink/usql/04_create_users_latest.sql
   docker exec -it docker-jobmanager-1 /opt/flink/bin/sql-client.sh -f /opt/flink/usql/05_create_country_dim.sql
   docker exec -it docker-jobmanager-1 /opt/flink/bin/sql-client.sh -f /opt/flink/usql/06_create_users_enriched_view.sql
   docker exec -it docker-jobmanager-1 /opt/flink/bin/sql-client.sh -f /opt/flink/usql/07_create_users_enriched_sink.sql
   docker exec -it docker-jobmanager-1 /opt/flink/bin/sql-client.sh -f /opt/flink/usql/insert.sql

   ```

   This starts the SQL client with all sources, views, sink registered, and launches the continuous job.

---

4. **Publish CDC events to Kafka:**
   ```bash
   python3 src/producers/cdc_producer.py      --file data/sample_events.json      --topic users_cdc      --bootstrap localhost:9092      --delay 0.5
   ```

---

5. **Verify results in Postgres:**
   ```bash
   docker exec -it docker-postgres-1      psql -U app -d appdb -c "SELECT * FROM users_enriched ORDER BY user_id;"
   ```

---

## Architecture

### High-level overview

```
Kafka (CDC JSON) → Flink Normalize → Dedup (event-time) → Enrich → Postgres
```

### Detailed pipeline

```
               ┌─────────────────────────────┐
               │        Kafka Topic          │
               │       (users_cdc JSON)      │
               └──────────────┬──────────────┘
                              │
                              ▼
               ┌─────────────────────────────┐
               │ Normalize CDC Events        │
               │ (before/after → flat row,   │
               │ add is_deleted flag)        │
               └──────────────┬──────────────┘
                              │
                              ▼
               ┌─────────────────────────────┐
               │ Deduplicate by Event-Time   │
               │ (ROW_NUMBER → keep latest   │
               │ per user_id, watermark 10s) │
               └──────────────┬──────────────┘
                              │
                              ▼
               ┌─────────────────────────────┐
               │ Enrichment                  │
               │ - Join country_dim          │
               │ - Extract email_domain      │
               │ - Flag corporate domains    │
               └──────────────┬──────────────┘
                              │
                              ▼
               ┌─────────────────────────────┐
               │ Postgres Sink (users_enriched) │
               │ - PK upserts (user_id)       │
               │ - Guarded by updated_at      │
               └─────────────────────────────┘
```

---

## Project Structure

```
cdc-flink-migration/
├── src/
│   ├── flink_job/          # Flink SQL job
│   │   └── 0_*.sql         # Individual setup SQL scripts
│   │   └── insert.sql      # Launch insert SQL script
│   │   └── setup_all.sql   # Setup SQL script consolidated from all above individual scripts
│   ├── producers/          # Kafka CDC producer
│   │   └── cdc_producer.py
│   └── schemas/            # Postgres schema
│       └── postgres.sql
├── data/
│   └── sample_events.json  # CDC events (with out-of-order delete)
├── docker/
│   └── docker-compose.yml  # Kafka + Flink + Postgres + Adminer
├── tools/
│   └── convert_timestamp.py # Utility: decode timestamp_ms
├── README.md
└── requirements.txt
```

---

## Setup Instructions

### 1. Start infrastructure
```bash
cd docker
docker compose up -d
```

- **Flink UI:** http://localhost:8081
- **Postgres Adminer (optional):** http://localhost:8080

### 2. Initialize Postgres schema
```bash
psql "postgresql://app:app@localhost:5432/appdb" -f src/schemas/postgres.sql
```

### 3. Start Flink SQL client & load job
```bash
docker cp src/flink_job/setup_all.sql docker-jobmanager-1:/opt/flink/usql/setup_all.sql
docker exec -it docker-jobmanager-1 /opt/flink/bin/sql-client.sh

-- inside Flink SQL client:
SOURCE /opt/flink/usql/setup_all.sql;
```

### 4. Publish CDC events
```bash
python3 src/producers/cdc_producer.py   --file data/sample_events.json   --topic users_cdc   --bootstrap localhost:9092   --delay 0.5
```

### 5. Verify results

- **Option A: Postgres CLI**
  ```bash
  psql "postgresql://app:app@localhost:5432/appdb" -c "SELECT * FROM users_enriched ORDER BY user_id;"
  ```

- **Option B (Optional): Adminer GUI**
  - Open [http://localhost:8080](http://localhost:8080).
  - Login:
    - **System:** PostgreSQL
    - **Server:** postgres
    - **Username:** app
    - **Password:** app
    - **Database:** appdb
  - Inspect table `users_enriched`.

 ### 6. Testing locally
   ```bash
   pytest -v 
   ```
---

## Architectural Decisions and Considerations

- **Kafka as CDC transport:** Reliable, replayable event bus to decouple source system and Flink 
- **Flink SQL job:** Handles ordering, deduplication, and enrichment in a streaming-first way  
- **Postgres sink:** Chosen as a simple, well-known OLTP target to demonstrate idempotent upserts  
- **JSON format:** Easy to inspect for demo; could evolve to Avro/Protobuf with Schema Registry  
- **Enrichment strategy:** Inline static dimension for simplicity; scalable alternatives include broadcast state or async lookups 
- **Fault tolerance:** Relies on Flink checkpoints and idempotent upserts for recovery, rather than XA transactions  

---

## Handling Out-of-Order CDC Events

- **Event-time (`timestamp_ms`)** -> ensures ordering is based on when the change happened, not arrival time.
- **Watermarks (10s)** -> allow bounded lateness.
- **ROW_NUMBER() deduplication** -> keep only the latest row per user_id.
- **DB guard (updated_at check)** -> ensures late stale events cannot overwrite newer truth.

Example from sample data:
- Update at 10:05 arrives before delete at 10:04 (late).
- Flink dedup + DB guard ensure the **update wins**.

---

## Enrichment Logic

- **Country Join** -> `country_code` -> `country_name`
- **Email Domain Extraction** -> `SUBSTRING(email FROM ...)`
- **Corporate Flag** -> mark domains like `contoso.com`, `autodesk.com`

In production, we could expand the scope with additions like:
- Disposable email detection
- Currency/timezone enrichments
- Quality flags (missing/null values)

---

## Error Handling

- **Bad Events:** Malformed JSON ignored; invalid operations or missing fields routed to a Dead-Letter Queue (DLQ) Kafka topic (`users_cdc_dlq`) for later inspection.  
- **DB Failures:** JDBC sink retries writes; Flink applies backpressure until the database recovers. Idempotent upserts with `updated_at` ensure retries never create duplicates.  
- **Batching:** Mini-batches (`200 rows / 2s`) improve throughput without sacrificing correctness.  
- **Trade-off:** We use upserts + DLQ instead of XA transactions, balancing resilience with simplicity and performance.  

---

## Exactly-Once Processing

- **Source side:** Kafka + Flink checkpoints -> exactly-once replay
- **Sink side (JDBC):** At-least-once by default (no XA/2PC)
- **Our strategy:** Make sink **idempotent** via:
  - Primary-key upserts
  - Guarded `updated_at` check

Result: **effectively-once** outcomes.

---

## Testing

- **Producer Unit Tests (pytest):** validate argument parsing, JSON loading, and event validation in CDC producer.  
- **Logic Tests (pandas):** simulate deduplication (latest event wins) and enrichment (joining country codes to names) to confirm business rules outside Flink  
- **End-to-End:** e2e pipeline test by publishing sample CDC events and asserting results in Postgres  

---

## Monitoring

- Start with **Flink UI** and **structured logging from producer**. Backpressure column in Flink UI to detect bottlenecks.

- Could enable PrometheusReporter, Kafka JMX exporter; show a Grafana dashboard.

---

## Performance Optimization

- **Sink batching:** JDBC sink flushes every 200 rows / 2s, instead of row-by-row -> higher throughput 
- **Parallelism:** Increasing parallelism to match Kafka partitions  
- **State TTL:** 7-day TTL to bound memory usage in long-running jobs 
- **Producer batching:** Uses linger/batching, batch_size, compression_type can be enabled later
- **Enrichment:** Inline lookup for small dims, broadcast state or async I/O for large datasets
- **Monitoring:** Backpressure metrics in Flink UI

---

## Trade-offs Made

- **Sink Guarantees:** Idempotent upserts + retries (at-least-once) over XA transactions -> resilience & simplicity 
- **Schema Format:** JSON for demo speed/readability. Avro/Protobuf + Schema Registry for long-term  
- **Enrichment:** Inlined small dim tables; broadcast state or async lookups for larger dims  
- **Event Ordering:** 10s watermark, longer windows improve correctness but increase latency  
- **Performance vs Correctness:** Batched sink writes (200 rows / 2s) boosting throughput; per-row flushes guarantee lower latency but waste resources.  

---


## Useful Tools

### Convert timestamp_ms
To check event times in human-readable form:
```bash
python3 tools/convert_timestamp.py
```
Output:
```
1726531200000 ms -> 2024-09-17 10:00:00 UTC
...
```

---

## Extensions (Future Work)

- Adding Dead Letter Queue (DLQ) Kafka topic for bad events
- Using broadcast state or async lookups for large enrichment tables
- Switch from JSON to Avro/Protobuf - row-oriented, compact, support schema evolution
- Monitoring enhancements via other tools like Prometheus and Grafana
