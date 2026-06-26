"""Shared coded-vocabulary maps + unit conversions for the silver layer.

Kept in one engine-agnostic module (no DuckDB/Spark imports) so both the local DuckDB silver
(`vitals.lakehouse`) and the Databricks Delta silver (`vitals.backends.databricks_delta`) conform
to the *same* standards — the silver layer's data-quality contract lives here, in version control.
"""
from __future__ import annotations

# Glucose unit conversion: mmol/L -> mg/dL.
MMOL_TO_MGDL = 18.0182

# Free-text condition -> ICD-10 (silver recovers validity the bronze data lost).
TEXT_TO_ICD = {
    "low back pain": "M54.5",
    "knee osteoarthritis": "M17.0",
    "rotator cuff tear": "M75.100",
    "herniated disc": "M51.26",
    "right knee pain": "M25.561",
}

# Canonical display per ICD-10 code (conforms both coded and text-recovered conditions).
ICD_DISPLAY = {
    "M54.5": "Low back pain",
    "M17.0": "Bilateral primary osteoarthritis of knee",
    "M75.100": "Rotator cuff tear",
    "M51.26": "Lumbar disc displacement",
    "M25.561": "Pain in right knee",
}
