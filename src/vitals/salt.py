"""The de-identification salt for patient_key hashing (ADR 0002).

`patient_key = md5(id || SALT)` — a **salted** hash, so the public silver data can't be re-identified by
a dictionary/rainbow attack over a guessable ID space (Synthea IDs are sequential/derivable). The salt
is a *single shared secret*: the SAME value for every patient, row, and table — so it stays
deterministic (same patient -> same patient_key everywhere) and cross-table joins on `patient_key` hold.
A per-row random salt would be wrong here (it would break every join).

The salt is read from the environment at runtime and is **never committed** to this public repo: set
`VITALS_SALT` (or put it in the gitignored `.env`; `make setup` generates one). Every engine (DuckDB
silver, PySpark streaming/scale, Databricks Delta) reads the SAME salt, so `patient_key` is identical
across them.
"""
from __future__ import annotations

import os
from pathlib import Path

_ENV = "VITALS_SALT"
_DOTENV = Path(__file__).resolve().parents[2] / ".env"


def patient_salt() -> str:
    """The shared de-id salt from `VITALS_SALT` (falling back to the gitignored repo-root `.env`).

    Raises if unset — de-identification must never silently run without a salt (that was the original
    bug: an *unsalted* md5 is trivially reversible against a known ID space)."""
    salt = os.environ.get(_ENV) or _from_dotenv()
    if not salt:
        raise RuntimeError(
            f"{_ENV} is not set — the de-identification salt is required (ADR 0002). "
            f"Run `make setup` (generates one into .env) or `export {_ENV}=<secret>`."
        )
    return salt


def _from_dotenv() -> str | None:
    if not _DOTENV.exists():
        return None
    for raw in _DOTENV.read_text().splitlines():
        line = raw.strip()
        if line.startswith(f"{_ENV}=") and not line.startswith("#"):
            return line.split("=", 1)[1].strip().strip('"').strip("'") or None
    return None


def sql_literal(salt: str | None = None) -> str:
    """The salt as a safe single-quoted SQL string literal (DuckDB / Spark SQL), quotes escaped."""
    s = salt if salt is not None else patient_salt()
    return "'" + s.replace("'", "''") + "'"
