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
  (15169 events, file == kafka). Lessons from the live run: a fast TCP healthcheck was needed (the JVM
  CLI healthcheck was too slow to pass in time); each parity path must run in its own subprocess because
  `spark.jars.packages` is only honoured at JVM startup; the topic is reset before each produce run to
  keep parity deterministic.
- Production/managed Kafka (MSK/Confluent), schema registry/Avro, and wiring the stream into silver are
  out of scope.

## Alternatives considered
- **Leave it file-source ("only the source would change"):** the exact claim-not-proof gap; a pointed
  interviewer asks "show me it reading from Kafka."
- **Spark-based producer:** heavier; a plain `kafka-python` producer is a clearer "events on a bus."
- **Managed Kafka now:** out of scope for a local, reproducible showcase.
