# Design — real Kafka source for the wearable stream

_Date: 2026-07-02 · Status: DRAFT — approved design, not yet implemented · Phase: streaming ingestion (exercises the "Kafka" stack claim)_

> **One-liner:** the wearable stream is Spark Structured Streaming but **file-source**, with a docstring
> promising "in production the only change is the source: `.format('kafka')`." This unit makes that real:
> a local Kafka broker + a producer + the Spark job reading `format("kafka")`, sharing the **same**
> cleaning transform, and **parity-proven** to produce identical output to the file path — turning a
> *claim* into an *exercised fact*.

## Goal

`src/vitals/streaming.py` is a genuine Spark Structured Streaming job (schema, checkpointing,
`availableNow` trigger, outlier cleaning), but it reads micro-batch JSON files from a landing directory
— the broker-free demo. Kafka is named in the stack and the docstring literally says only the source
would change. Leaving it there is exactly the "compatible ≠ exercised" trap (teach record 0011). This
unit exercises it: a real Kafka broker (Docker) + a producer publishing wearable events + the Spark job
consuming `format("kafka")`, and a **parity check** that the Kafka path's cleaned output equals the
file path's — the concrete proof of "only the source changed, everything downstream identical."

## Non-negotiable principles this serves / preserves

- **Compatible ≠ exercised** — the point is to *run* the Kafka path, not assert it would work.
- **DRY / single transform** — the cleaning logic is extracted once and shared by both source paths, so
  the parity is meaningful (same transform, different source) and there's one implementation.
- **Clone-and-run / hermetic CI unaffected** — streaming needs JDK + Spark + Docker; it was never in the
  hermetic CI gate and stays out. The Kafka path is an optional `stream` extra + `make` targets.
- **Reproducible from code** — the broker is `docker compose`; producer + consumer + parity run from `make`.

## Scope decisions (locked with the user)

- **Keep both sources.** The file-source path stays as the no-broker default; the Kafka path is added.
  Both call the shared `clean_wearables`; parity between them is the acceptance proof.
- **`kafka-python` producer** (pure-Python, pip-installable) — not a Spark-based producer.
- **Live acceptance** — a real broker + produce → Spark-from-Kafka → parity vs file-source is "done."

## Current state

`streaming.py`: `produce_stream()` splits `data/bronze/wearables.ndjson` into N micro-batch JSON files
in `data/stream/landing`; `run_stream()` does `spark.readStream.schema(SCHEMA).json(landing)` →
`withColumn` chain (`patient_key` = md5 of the patient ref, `event_date`, null-out steps outside
[0, 50000], select) → `writeStream.format("parquet")` to `data/stream/cleaned` with a checkpoint,
`trigger(availableNow=True)`. Returns `{events_streamed, outliers_nulled, sink, sample}`. The cleaning
chain is currently inline inside `run_stream`.

## Components

### 1. `streaming.py` — extract the shared `clean_wearables(df)` (DRY)

Pull the `withColumn`/`select` cleaning chain out of `run_stream` into `clean_wearables(df) -> df`
(pure Spark DataFrame → DataFrame). `run_stream` (file path) calls it unchanged; the new Kafka path
calls the identical function. `SCHEMA` (the `StructType`) is module-level, shared by both paths.

### 2. `docker-compose.yml` — a Kafka broker (KRaft, single node)

Add a `kafka` service (Apache Kafka in **KRaft** mode — no Zookeeper): single broker, advertised
listener `localhost:9092`, a healthcheck (broker API reachable). `make stream-up` starts it + waits
healthy; `make stream-down` stops it (mirrors `rag-up`/`rag-down`). The `wearables` topic is
auto-created on first publish (or created by the producer).

### 3. `streaming.py` — `produce_to_kafka()` (kafka-python)

Read `data/bronze/wearables.ndjson`; for each line, publish the JSON bytes to the `wearables` topic via
`kafka-python`'s `KafkaProducer(bootstrap_servers="localhost:9092")`; flush; return the count. This is
the "events arrive on the topic" half — the real message bus.

### 4. `streaming.py` — `run_stream_kafka()` (the Kafka consumer)

`spark.readStream.format("kafka").option("kafka.bootstrap.servers","localhost:9092")
.option("subscribe","wearables").option("startingOffsets","earliest").load()` → the Kafka `value`
(bytes) is `CAST(value AS STRING)` then `from_json(value, SCHEMA)` → `clean_wearables(...)` → the same
`writeStream.format("parquet")` sink (own output dir `data/stream/cleaned_kafka` + own checkpoint),
`trigger(availableNow=True)`. The SparkSession is built with
`spark.jars.packages = org.apache.spark:spark-sql-kafka-0-10_2.12:<pyspark version>` (pinned to the
installed pyspark — the connector jar downloads on first run). Returns the same result shape as
`run_stream`.

### 5. `pyproject.toml` extra + Makefile

- `stream = ["kafka-python>=2.0", "pyspark>=3.5"]` (self-contained for the streaming demo).
- `make stream-up` / `stream-down` (broker); `make stream-produce` (`produce_to_kafka`); `make
  stream-kafka` (produce → `run_stream_kafka`); `make stream-parity` (run BOTH paths, assert identical).
  The existing `make stream` (file path) stays.

### 6. Parity — `run_parity() -> dict` (the acceptance proof)

Run the file path (`produce_stream` + `run_stream`) and the Kafka path (`produce_to_kafka` +
`run_stream_kafka`); read both cleaned parquet outputs; assert **identical**: same `events_streamed`,
same `outliers_nulled`, and the same set of cleaned rows (sorted by `patient_key, event_date`). Returns
`{file: {...}, kafka: {...}, identical: bool}`. `make stream-parity` runs it and exits non-zero if not
identical.

## Data flow

```
data/bronze/wearables.ndjson
   ├─ produce_stream()  → landing/*.json  → readStream.json  ─┐
   └─ produce_to_kafka() → Kafka `wearables` → readStream.format("kafka") → from_json ─┐
                                                                                        ▼
                                                        clean_wearables(df)  (ONE shared transform)
                                                                                        ▼
                          file path → data/stream/cleaned     kafka path → data/stream/cleaned_kafka
                                                     └────────── parity: identical ──────────┘
```

## Error handling / gates

- **Broker down / not healthy** → `produce_to_kafka` / the Kafka read fails loudly; `make stream-up`
  waits for the healthcheck before producing.
- **Connector jar missing** → the SparkSession `spark.jars.packages` downloads it on first run (needs
  network once); a wrong version vs pyspark fails fast — the plan pins it to `pyspark.__version__`.
- **Parity mismatch** → `run_parity` returns `identical: False` and `make stream-parity` exits non-zero
  — surfaces a real "downstream differs" bug rather than passing silently.

## Testing

- **Hermetic (CI-safe):** a pure unit test of the non-Spark surface — the producer's per-event
  serialization (`wearables.ndjson` line → the bytes published) and the connector-package string derived
  from the pyspark version. No broker/Spark needed; runs in CI.
- **Not in CI:** the Spark + Kafka paths need JDK/Spark/Docker — excluded from the hermetic gate (as
  streaming always has been). A Spark/Docker-gated test may wrap `run_parity` (skips without JDK+broker).
- **Live acceptance (the bar):** `make stream-up` → `make stream-parity` → both paths run, output is
  **identical**; spot-check `events_streamed`/`outliers_nulled` match the file path.

## Docs

- New ADR `docs/adr/0010-kafka-streaming-source.md` — real Kafka source (local Docker KRaft broker);
  the file-source path kept as the no-broker default; the shared `clean_wearables` transform; parity as
  the "only the source changed" proof; the Spark-Kafka connector pinned to pyspark; production/managed
  Kafka noted-not-exercised. Explicitly frames this as retiring a "compatible ≠ exercised" gap.
- `README.md` — the **Kafka** stack item made true (real broker + `make stream-kafka`/`stream-parity`).
- `streaming.py` docstring — drop "in production the only change is the source"; now done + exercised.

## Non-goals (YAGNI)

- No production/managed Kafka (MSK / Confluent Cloud) — local Docker broker only.
- No schema registry / Avro — JSON message values (the connector deserializes with the existing SCHEMA).
- No windowing / watermarks / exactly-once beyond the current cleaning + append sink.
- **Not** wiring the streamed output into silver — it stays a standalone streaming-ingestion demo (as
  today); the medallion still ingests wearables in batch.
- No replacement of the file-source path (it's the no-broker default + the parity reference).

## Risks to pin in the plan

- **Connector version.** `spark-sql-kafka-0-10_2.12:<version>` must exactly match the installed pyspark
  (e.g. pyspark 3.5.x → `:3.5.x`); the plan derives it from `pyspark.__version__`, not a hardcode.
- **KRaft docker image config.** Single-node KRaft needs the right env (node id, listeners, advertised
  listeners, `CLUSTER_ID`); the plan pins a known-good `apache/kafka` (or `bitnami/kafka`) config and
  verifies `docker compose up` reaches healthy + a produce/consume round-trips.
- **JDK.** Spark needs JDK 17/21; `streaming.py` already prefers `openjdk@17`/`@21` via `JAVA_HOME`
  (present locally). The plan keeps that.

## Files touched

| File | Change |
|---|---|
| `src/vitals/streaming.py` | extract `clean_wearables`; add `produce_to_kafka`, `run_stream_kafka`, `run_parity`; module `SCHEMA`; docstring |
| `docker-compose.yml` | add the `kafka` (KRaft) service + healthcheck |
| `pyproject.toml` | `stream` extra (`kafka-python`, `pyspark`) |
| `Makefile` | `stream-up`/`stream-down`/`stream-produce`/`stream-kafka`/`stream-parity` |
| `tests/test_streaming.py` | new — hermetic pure test (serialization + connector-version string) |
| `docs/adr/0010-kafka-streaming-source.md` | new ADR |
| `README.md` | Kafka stack item made true |
