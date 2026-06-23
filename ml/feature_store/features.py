"""Feast feature definitions for the patient surgery-risk features.

`feast apply` registers these; the offline source is the gold parquet from the pipeline.
Install with `uv pip install feast` (optional extra) to materialize.
"""
from datetime import timedelta
from pathlib import Path

from feast import Entity, FeatureView, Field, FileSource
from feast.types import Float32, Int64

PARQUET = str(Path(__file__).resolve().parents[2] / "data" / "gold" / "patient_features.parquet")

patient = Entity(name="patient", join_keys=["patient_key"])

source = FileSource(path=PARQUET, timestamp_field="event_timestamp")

patient_features = FeatureView(
    name="patient_surgery_risk_features",
    entities=[patient],
    ttl=timedelta(days=90),
    schema=[
        Field(name="age", dtype=Int64),
        Field(name="mean_pain", dtype=Float32),
        Field(name="last_pain", dtype=Float32),
        Field(name="pain_trend", dtype=Float32),
        Field(name="mean_adherence", dtype=Float32),
        Field(name="mean_glucose_mgdl", dtype=Float32),
        Field(name="mean_hr", dtype=Float32),
        Field(name="n_observations", dtype=Int64),
    ],
    source=source,
    online=True,
)
