# Real Kafka Source for the Wearable Stream — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Turn the file-source Spark Structured Streaming job into a real Kafka source — a local broker + a producer + the Spark job reading `format("kafka")`, sharing one cleaning transform — and parity-prove the Kafka path's cleaned output equals the file path's.

**Architecture:** Extract the cleaning chain into a shared `clean_wearables(df)`; keep the file-source path as the no-broker default; add a Kafka broker (Docker KRaft), a `kafka-python` producer, and a Spark consumer reading `format("kafka")`. A `run_parity()` runs both paths and asserts identical output. Streaming stays out of the hermetic CI gate (needs JDK+Spark+Docker).

**Tech Stack:** Apache Kafka (Docker, KRaft), kafka-python, PySpark (Spark 4, Structured Streaming), spark-sql-kafka connector, pandas (parity), pytest.

## Global Constraints

- **Keep BOTH sources.** File-source path (`produce_stream`/`run_stream`) stays the no-broker default (`make stream` unchanged); the Kafka path is added. Both call the identical `clean_wearables`.
- **Connector version is DERIVED, not hardcoded:** `spark-sql-kafka-0-10_<scala>:<pyspark version>` where scala = `2.13` for Spark ≥ 4, `2.12` for Spark 3.x. (Installed pyspark resolves to 4.1.2 → `spark-sql-kafka-0-10_2.13:4.1.2`.)
- **Module stays hermetically importable** — `pyspark` and `kafka` are imported INSIDE functions (never at module top); `import vitals.streaming` must not require them. The existing module-top `JAVA_HOME` logic stays.
- **Streaming is NOT in the hermetic CI gate** (it needs JDK 17/21 + Spark + Docker) — unchanged. Only a pure test (the connector-coordinate helper) runs in CI.
- **Parity is the acceptance:** `run_parity()` runs the file + Kafka paths and asserts identical cleaned rows + counts; `make stream-parity` exits non-zero on mismatch.
- Bootstrap `localhost:9092`; topic `wearables`; Kafka output `data/stream/cleaned_kafka` (own checkpoint), file output stays `data/stream/cleaned`.
- Tests import `vitals` via pytest `pythonpath = ["src"]`.

---

### Task 1: DRY refactor — shared `clean_wearables` + `SCHEMA` + connector helper (hermetic)

Pull the cleaning transform out so both source paths share it, and add the pure connector-coordinate helper — all unit-testable without Spark.

**Files:**
- Modify: `src/vitals/streaming.py`
- Test: `tests/test_streaming.py`

**Interfaces:**
- Produces: module-level `SCHEMA` (StructType); `clean_wearables(df) -> df`; `kafka_connector_package(version: str | None = None) -> str`.

- [ ] **Step 1: Write the failing test**

Create `tests/test_streaming.py`:

```python
from vitals import streaming


def test_kafka_connector_package_matches_spark_scala_version():
    # Spark 4+ ships Scala 2.13; Spark 3.x ships 2.12. Coordinate version == pyspark version.
    assert streaming.kafka_connector_package("4.1.2") == \
        "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.2"
    assert streaming.kafka_connector_package("3.5.1") == \
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"


def test_module_exposes_shared_transform_symbols():
    # The `from vitals import streaming` at the top of this file already proves the module imports
    # without pyspark/kafka (they're function-local); assert the shared symbols exist.
    assert hasattr(streaming, "clean_wearables")
    assert hasattr(streaming, "SCHEMA")
    assert hasattr(streaming, "kafka_connector_package")
```

- [ ] **Step 2: Run to verify it fails**

Run: `uv run --extra dev python -m pytest tests/test_streaming.py -q`
Expected: FAIL — `AttributeError: module 'vitals.streaming' has no attribute 'kafka_connector_package'`.

- [ ] **Step 3: Add `kafka_connector_package` + hoist `SCHEMA` + extract `clean_wearables`**

In `src/vitals/streaming.py`:

(a) Add the pure helper (near the top, after the constants):

```python
def kafka_connector_package(version: str | None = None) -> str:
    """Maven coordinate for the spark-sql-kafka connector, matching the installed pyspark: Scala 2.13
    for Spark 4+, 2.12 for Spark 3.x; connector version == pyspark version (they must line up)."""
    if version is None:
        import pyspark
        version = pyspark.__version__
    scala = "2.13" if int(version.split(".")[0]) >= 4 else "2.12"
    return f"org.apache.spark:spark-sql-kafka-0-10_{scala}:{version}"
```

(b) Move the `SCHEMA` StructType to module level (it's currently built inside `run_stream`). Add near the constants — it needs the pyspark types, so define it lazily via a function OR keep the import local. To keep the module hermetic, make `SCHEMA` a builder function:

```python
def _schema():
    from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType
    return StructType([
        StructField("type", StringType()),
        StructField("id", StringType()),
        StructField("patient", StructType([StructField("reference", StringType())])),
        StructField("date", StringType()),
        StructField("steps", LongType()),
        StructField("active_minutes", LongType()),
        StructField("resting_hr", LongType()),
        StructField("sleep_hours", DoubleType()),
    ])
```

And expose a module attribute the test checks: `SCHEMA = _schema` (the builder). (The test asserts `hasattr(streaming, "SCHEMA")`; a callable is fine — update the test assertion if you prefer, but keep a `SCHEMA` name.)

(c) Extract the cleaning chain into `clean_wearables`:

```python
def clean_wearables(stream):
    """The shared cleaning transform (identical for the file + Kafka sources): derive patient_key +
    event_date, null out impossible step counts, select the canonical columns."""
    from pyspark.sql import functions as F
    return (stream
            .withColumn("patient_key", F.md5(F.regexp_replace("patient.reference", "Patient/", "")))
            .withColumn("event_date", F.to_date("date"))
            .withColumn("steps", F.when((F.col("steps") >= 0) & (F.col("steps") <= 50000), F.col("steps")))
            .select("patient_key", "event_date", "steps", "active_minutes", "resting_hr", "sleep_hours"))
```

(d) In `run_stream`, replace the inline `schema = StructType([...])` with `schema = _schema()`, and replace the inline `cleaned = (stream.withColumn(...)...)` with `cleaned = clean_wearables(stream)`. Behavior is unchanged.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run --extra dev python -m pytest tests/test_streaming.py -q`
Expected: PASS (2 tests). (If the `SCHEMA` assertion needs adjusting to the builder form, fix the test to `hasattr(streaming, "SCHEMA")` which holds.)

- [ ] **Step 5: Lint + commit**

```bash
uv run ruff check src/vitals/streaming.py tests/test_streaming.py
git add src/vitals/streaming.py tests/test_streaming.py
git commit -m "refactor(streaming): shared clean_wearables + SCHEMA builder + kafka connector helper

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01F1hdqzju3WbgEALYreWUjk"
```

---

### Task 2: Kafka producer + consumer + parity + broker + extra + make targets

The Kafka integration code (verified hermetically for import/lint/config; run live in Task 3).

**Files:**
- Modify: `src/vitals/streaming.py` (add `produce_to_kafka`, `run_stream_kafka`, `run_parity`, `main` dispatch, Kafka output constants)
- Modify: `docker-compose.yml` (add the `kafka` service)
- Modify: `pyproject.toml` (`stream` extra)
- Modify: `Makefile` (`stream-up/down/produce/kafka/parity` + `.PHONY`)

**Interfaces:**
- Consumes: `_schema`, `clean_wearables`, `kafka_connector_package` (Task 1); `produce_stream`, `run_stream` (existing).
- Produces: `produce_to_kafka(bootstrap="localhost:9092", topic="wearables") -> int`; `run_stream_kafka() -> dict`; `run_parity() -> dict` (`{file, kafka, identical, n_file, n_kafka}`); `main(argv=None)`.

- [ ] **Step 1: Add the Kafka output constants + functions to `streaming.py`**

Add constants near the existing `OUT`/`CHECKPOINT`:

```python
OUT_KAFKA = ROOT / "data" / "stream" / "cleaned_kafka"
CHECKPOINT_KAFKA = ROOT / "data" / "stream" / "checkpoint_kafka"
BOOTSTRAP = "localhost:9092"
TOPIC = "wearables"
```

Add the producer:

```python
def produce_to_kafka(bootstrap: str = BOOTSTRAP, topic: str = TOPIC) -> int:
    """Publish each wearable bronze event (one NDJSON line = one JSON message) to the Kafka topic."""
    from kafka import KafkaProducer
    producer = KafkaProducer(bootstrap_servers=bootstrap, acks="all", linger_ms=5)
    lines = [ln for ln in BRONZE_WEARABLES.read_text().splitlines() if ln.strip()]
    for ln in lines:
        producer.send(topic, ln.encode("utf-8"))
    producer.flush()
    producer.close()
    print(f"produced {len(lines)} events -> kafka topic '{topic}'")
    return len(lines)
```

Add the Kafka consumer (mirrors `run_stream`, source = Kafka, transform shared):

```python
def run_stream_kafka() -> dict:
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F

    shutil.rmtree(OUT_KAFKA, ignore_errors=True)
    shutil.rmtree(CHECKPOINT_KAFKA, ignore_errors=True)

    spark = (SparkSession.builder.master("local[2]").appName("vitals-wearable-stream-kafka")
             .config("spark.ui.enabled", "false")
             .config("spark.sql.shuffle.partitions", "4")
             .config("spark.jars.packages", kafka_connector_package())
             .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")

    raw = (spark.readStream.format("kafka")
           .option("kafka.bootstrap.servers", BOOTSTRAP)
           .option("subscribe", TOPIC)
           .option("startingOffsets", "earliest")
           .load())
    # Kafka value is bytes -> string -> parse JSON with the SAME schema the file source uses.
    parsed = (raw.select(F.from_json(F.col("value").cast("string"), _schema()).alias("j"))
              .select("j.*"))
    cleaned = clean_wearables(parsed)

    query = (cleaned.writeStream.format("parquet")
             .option("path", str(OUT_KAFKA))
             .option("checkpointLocation", str(CHECKPOINT_KAFKA))
             .outputMode("append")
             .trigger(availableNow=True)
             .start())
    query.awaitTermination()

    out = spark.read.parquet(str(OUT_KAFKA))
    n = out.count()
    outliers = out.filter("steps is null").count()
    spark.stop()
    result = {"events_streamed": n, "outliers_nulled": outliers,
              "sink": "data/stream/cleaned_kafka (parquet)"}
    print("kafka streaming complete:", result)
    return result
```

Add parity + `main` dispatch:

```python
def run_parity() -> dict:
    """Run BOTH sources and assert the cleaned output is identical (only the source changed)."""
    import pandas as pd
    produce_stream()
    file_res = run_stream()
    produce_to_kafka()
    kafka_res = run_stream_kafka()

    def _load(path):
        df = pd.read_parquet(path)
        return df.sort_values(by=list(df.columns)).reset_index(drop=True)

    f, k = _load(OUT), _load(OUT_KAFKA)
    identical = f.equals(k)
    result = {"file": file_res, "kafka": kafka_res,
              "identical": bool(identical), "n_file": len(f), "n_kafka": len(k)}
    print("parity:", {"identical": result["identical"], "n_file": len(f), "n_kafka": len(k)})
    return result


def main(argv=None):
    argv = sys.argv[1:] if argv is None else argv
    cmd = argv[0] if argv else "file"
    if cmd == "produce":
        return {"produced": produce_to_kafka()}
    if cmd == "kafka":
        produce_to_kafka()
        return run_stream_kafka()
    if cmd == "parity":
        return run_parity()
    produce_stream()
    return run_stream()
```

Replace the existing `if __name__ == "__main__": main()` block with:

```python
if __name__ == "__main__":
    result = main()
    if isinstance(result, dict) and result.get("identical") is False:
        raise SystemExit(1)   # `make stream-parity` fails the shell on a mismatch
```

Add `import sys` to the module-top imports if not present.

- [ ] **Step 2: Add the Kafka broker to `docker-compose.yml`**

Add a `kafka` service (single-node KRaft):

```yaml
  kafka:
    image: apache/kafka:3.8.1
    ports:
      - "9092:9092"
    environment:
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_LISTENERS: PLAINTEXT://:9092,CONTROLLER://:9093
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@localhost:9093
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
      KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 0
    healthcheck:
      test: ["CMD-SHELL", "/opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server localhost:9092 >/dev/null 2>&1 || exit 1"]
      interval: 5s
      timeout: 5s
      retries: 20
```

- [ ] **Step 3: Add the `stream` extra to `pyproject.toml`**

After the `dq` extra line:
```toml
# Streaming — real Kafka source for the wearable feed (ADR 0010). Local Docker broker; not in CI.
stream = ["kafka-python>=2.0", "pyspark>=3.5"]
```

- [ ] **Step 4: Add the Makefile targets**

Add to `.PHONY` and append (the existing `stream` file-source target stays):
```make
stream-up:      ## start the local Kafka broker (Docker KRaft) + wait until healthy
	docker compose up -d kafka
	@until [ "$$(docker inspect -f '{{.State.Health.Status}}' $$(docker compose ps -q kafka))" = "healthy" ]; do sleep 2; done
	@echo "kafka healthy on localhost:9092"

stream-down:    ## stop the Kafka broker
	docker compose stop kafka

stream-produce: ## publish the wearable bronze events to the `wearables` Kafka topic
	PYTHONPATH=src ./.venv/bin/python -m vitals.streaming produce

stream-kafka:   ## consume `wearables` with Spark Structured Streaming -> cleaned parquet
	PYTHONPATH=src ./.venv/bin/python -m vitals.streaming kafka

stream-parity:  ## run BOTH sources (file + kafka) and assert identical cleaned output
	PYTHONPATH=src ./.venv/bin/python -m vitals.streaming parity
```

- [ ] **Step 5: Verify hermetically (no live run yet)**

```bash
uv run --extra dev python -c "import vitals.streaming"      # module still imports without pyspark/kafka
uv run --extra dev python -m pytest tests/test_streaming.py -q   # Task 1 tests still pass
uv run ruff check src/vitals/streaming.py
docker compose config >/dev/null && echo "compose OK"       # kafka service YAML is valid
```
Expected: import OK; tests pass; ruff clean; `compose OK`. (Do NOT run the live broker/Spark here — that's Task 3.)

- [ ] **Step 6: Commit**

```bash
git add src/vitals/streaming.py docker-compose.yml pyproject.toml Makefile uv.lock
git commit -m "feat(streaming): real Kafka source (producer + Spark consumer + parity) + broker + make

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01F1hdqzju3WbgEALYreWUjk"
```

---

### Task 3: live acceptance — broker up, produce, stream, parity identical

**Run live (needs Docker + JDK 17/21 + pyspark + network for the connector jar).** This is the acceptance bar.

**Files:** none by default (commit only if a fix to `streaming.py`/`docker-compose.yml` is needed).

- [ ] **Step 1: Install the extra + start the broker**

```bash
uv sync --extra dev --extra stream
make stream-up
```
Expected: pyspark + kafka-python installed; `kafka healthy on localhost:9092`.

- [ ] **Step 2: Run the parity (the acceptance)**

```bash
make stream-parity; echo "exit: $?"
```
Expected: both paths run; prints `parity: {'identical': True, 'n_file': N, 'n_kafka': N}`; exit 0. The first run downloads the `spark-sql-kafka-0-10_2.13:<pyspark version>` jar (needs network).

**If it fails, resolve empirically (decision tree):**
- **Connector jar not found on Maven for the exact pyspark version** (e.g. 4.1.2 connector unpublished) → pin `pyspark` in the `stream` extra to the nearest version WITH a published connector (`spark-sql-kafka-0-10_2.13:<v>` exists on Maven — e.g. `pyspark==4.0.0`), `uv sync --extra stream`, retry. Commit the pin.
- **`localhost:9092` connection refused from the producer/Spark** → confirm the broker is healthy (`docker compose ps kafka`); the advertised listener is `localhost:9092` and the port is mapped — if Spark (JVM) can't reach it, ensure no other process holds 9092.
- **JDK 24 picked up (Spark 4 needs 17/21)** → `streaming.py` already prefers `openjdk@17`/`@21` via `JAVA_HOME`; confirm `/usr/local/opt/openjdk@17` is used (it's present).
- **Parity `identical: False`** → inspect the diff (compare `data/stream/cleaned` vs `cleaned_kafka` parquet); a real downstream difference is a bug to fix (likely the `from_json` schema vs the file-source schema) — do NOT relax the comparison. The shared `clean_wearables` should make them identical; a mismatch means the source parsing differs.

- [ ] **Step 3: Record the parity result + stop the broker**

Capture `n_file`/`n_kafka`/`identical` (used in the ADR/README, Task 4). Then:
```bash
make stream-down
```

- [ ] **Step 4: Commit (only if a fix was needed)**

```bash
git add -A
git commit -m "fix(streaming): <the empirical fix from the live run, e.g. pin pyspark for the connector>

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01F1hdqzju3WbgEALYreWUjk"
```

---

### Task 4: ADR 0010 + README + docstring

Document the closed "compatible ≠ exercised" gap with the real parity numbers.

**Files:**
- Create: `docs/adr/0010-kafka-streaming-source.md`
- Modify: `README.md`, `src/vitals/streaming.py` (docstring)

- [ ] **Step 1: Update the `streaming.py` module docstring**

Replace the "The source here is a *file* stream … **In production the only change is the source** …" paragraph with a note that the Kafka source is now real and exercised:

```python
"""Phase 3 — Spark Structured Streaming for the wearable feed.

Two interchangeable sources feed ONE cleaning transform (`clean_wearables`):
  - file source  (`run_stream`)      — micro-batch JSON files; no broker, the clone-and-run default.
  - Kafka source (`run_stream_kafka`) — a real broker (`make stream-up`), `readStream.format("kafka")`.
`run_parity` runs both and asserts the cleaned output is identical — proving only the source changed
(ADR 0010). Kafka path needs the `stream` extra (kafka-python + pyspark) + JDK 17/21 + Docker.
"""
```

- [ ] **Step 2: Write ADR 0010**

Create `docs/adr/0010-kafka-streaming-source.md`:

```markdown
# ADR 0010 — Real Kafka source for the wearable stream

**Status:** accepted · 2026-07-02

## Context
The wearable feed ran as Spark Structured Streaming, but from a *file* source (micro-batch JSON files),
with a docstring claiming "in production the only change is the source: `.format('kafka')`." Kafka was
named in the stack but never exercised — a "compatible but not exercised" gap (the trap this project
learned to distrust).

## Decision
Exercise it. Add a local **Kafka broker** (Docker, single-node **KRaft** — no Zookeeper), a
`kafka-python` **producer** that publishes the wearable events to a `wearables` topic, and a Spark
consumer (`run_stream_kafka`) reading `readStream.format("kafka")` → `from_json` → the **same**
`clean_wearables` transform → the same parquet sink. The file-source path stays as the no-broker
default and the **parity reference**: `run_parity()` runs both and asserts the cleaned output is
**identical** — the concrete proof of "only the source changed."

Key choices:
- **One shared transform.** `clean_wearables` is extracted so both sources run identical downstream
  logic — that's what makes the parity meaningful (and DRY).
- **Connector pinned to pyspark.** `spark-sql-kafka-0-10_<scala>:<version>` is derived from the
  installed pyspark (Scala 2.13 for Spark 4) — a mismatch fails fast.
- **Out of the hermetic CI gate.** Streaming needs JDK + Spark + Docker; it stays a `make` demo with a
  `stream` optional extra, never in the clone-and-run CI path (only the pure connector-helper is tested).

## Consequences
- New `stream` extra (kafka-python + pyspark), a `kafka` docker service, `make stream-up/down/produce/
  kafka/parity`. Verified live: `make stream-parity` → identical cleaned output across both sources
  (<n> events, file == kafka).
- Production/managed Kafka (MSK/Confluent), schema registry/Avro, and wiring the stream into silver are
  out of scope.

## Alternatives considered
- **Leave it file-source ("only the source would change"):** the exact claim-not-proof gap; a pointed
  interviewer asks "show me it reading from Kafka."
- **Spark-based producer:** heavier; a plain `kafka-python` producer is a clearer "events on a bus."
- **Managed Kafka now:** out of scope for a local, reproducible showcase.
```

(Fill `<n>` with the `n_file` from Task 3.)

- [ ] **Step 3: Update the README Kafka stack item**

In `README.md` tech stack, make the `**Kafka**` item reflect it's real:
```markdown
**Kafka** (real broker — the wearable stream reads `format("kafka")`; `make stream-up` + `make stream-parity`) ·
```

- [ ] **Step 4: Commit**

```bash
git add docs/adr/0010-kafka-streaming-source.md README.md src/vitals/streaming.py
git commit -m "docs(streaming): ADR 0010 — real Kafka source exercised (parity proven) + README

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01F1hdqzju3WbgEALYreWUjk"
```

---

## Self-Review

**Spec coverage:**
- Shared `clean_wearables` + `SCHEMA` → Task 1. ✓
- `kafka_connector_package` (derived) → Task 1 + hermetic test. ✓
- Kafka broker (Docker KRaft) → Task 2 Step 2. ✓
- `produce_to_kafka` (kafka-python) → Task 2 Step 1. ✓
- `run_stream_kafka` (format kafka, from_json, shared transform, own sink) → Task 2 Step 1. ✓
- `run_parity` (identical) + `stream-parity` non-zero on mismatch → Task 2 Step 1 + `main`/`__main__`. ✓
- `stream` extra + make targets → Task 2 Steps 3-4. ✓
- Live acceptance (broker → parity identical) → Task 3. ✓
- Hermetic module import + pure test in CI; streaming out of CI → Task 1 test + Task 2 Step 5. ✓
- ADR 0010 + README + docstring → Task 4. ✓
- Non-goals (no managed Kafka, no Avro, no silver wiring, keep file source) → no task violates them. ✓

**Placeholder scan:** none — all code/config/commands concrete. The `<n>`/`<fix>` in the ADR/commit are values filled from the Task 3 live run, not vague placeholders (the instruction says exactly what to fill).

**Type consistency:** `clean_wearables`/`_schema`/`kafka_connector_package` (Task 1) are consumed by `run_stream_kafka`/`run_stream` (Task 2) with matching names. `run_parity` returns `{file, kafka, identical, n_file, n_kafka}` — `identical` is what `__main__` checks and `make stream-parity` gates on. `OUT`/`OUT_KAFKA` are distinct sinks. `BOOTSTRAP`/`TOPIC` constants shared by producer + consumer.
```
