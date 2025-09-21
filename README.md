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
docker cp src/flink_job/job.sql docker-jobmanager-1:/opt/flink/usql/job.sql
docker exec -it docker-jobmanager-1 /opt/flink/bin/sql-client.sh

-- inside Flink SQL client:
SOURCE /opt/flink/usql/job.sql;
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

## Handling Out-of-Order Events

- **Event-time (`timestamp_ms`)** -> ensures ordering is based on when the change happened, not arrival time.
- **Watermarks (10s)** -> allow bounded lateness.
- **ROW_NUMBER() deduplication** -> keep only the latest row per user_id.
- **DB guard (updated_at check)** -> ensures late stale events cannot overwrite newer truth.

Example from sample data:
- Update at 10:05 arrives before delete at 10:04 (late).
- Flink dedup + DB guard ensure the **update wins**.

---

## Exactly-Once Processing

- **Source side:** Kafka + Flink checkpoints -> exactly-once replay.
- **Sink side (JDBC):** At-least-once by default (no XA/2PC).
- **Our strategy:** Make sink **idempotent** via:
  - Primary-key upserts
  - Guarded `updated_at` check

Result: **effectively-once** outcomes.

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
