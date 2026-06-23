"""Generate synthetic, deliberately-messy, FHIR-shaped health data.

MSK-themed (matching Sword's domain). Deterministic (seeded) so the pipeline and its
data-quality metrics are reproducible. Emits per-resource NDJSON into ``data/bronze/``.

The mess is intentional — it's what the silver layer earns its keep cleaning:
  * duplicate patient records
  * missing demographics
  * glucose recorded in mixed units (mg/dL vs mmol/L) with no unit normalization
  * conditions sometimes as free text instead of an ICD-10 code
  * schema drift in observation value fields
"""
from __future__ import annotations

import json
import random
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path

SEED = 42
N_PATIENTS = 600
BRONZE = Path(__file__).resolve().parents[2] / "data" / "bronze"

FIRST = ["Maria", "João", "Ana", "Pedro", "Sofia", "Miguel", "Inês", "Tiago", "Rita", "Hugo"]
LAST = ["Silva", "Santos", "Ferreira", "Costa", "Oliveira", "Rodrigues", "Martins", "Sousa"]
GENDERS = ["male", "female"]

# MSK conditions (ICD-10) + the free-text variants we'll sometimes emit instead (the mess).
CONDITIONS = [
    ("M54.5", "Low back pain", "low back pain"),
    ("M17.0", "Bilateral primary osteoarthritis of knee", "knee osteoarthritis"),
    ("M75.100", "Rotator cuff tear", "rotator cuff tear"),
    ("M51.26", "Lumbar disc displacement", "herniated disc"),
    ("M25.561", "Pain in right knee", "right knee pain"),
]
# LOINC observations.
LOINC_HBA1C = "4548-4"
LOINC_GLUCOSE = "2339-0"
LOINC_HR = "8867-4"

NOTE_TEMPLATES = [
    "Patient reports {sev} {site} pain, worse with {agg}. Adherence to home program {adh}. Plan: continue PT, reassess in {wk} weeks.",
    "Follow-up for {site} pain. Pain {sev}. {adh} with exercises. Discussed activity modification and {agg} avoidance.",
    "{site} pain ongoing, described as {sev}. Functional limitation with {agg}. Home program adherence {adh}.",
]
SEV = ["mild", "moderate", "severe"]
SITE = ["lower back", "knee", "shoulder", "hip"]
AGG = ["prolonged sitting", "stair climbing", "lifting", "overhead reaching"]
ADH = ["good", "partial", "poor"]


@dataclass
class Counter:
    n: int = 0

    def nxt(self, prefix: str) -> str:
        self.n += 1
        return f"{prefix}-{self.n:06d}"


def _round(x: float, nd: int = 1) -> float:
    return float(f"{x:.{nd}f}")


def generate() -> dict[str, int]:
    rng = random.Random(SEED)
    BRONZE.mkdir(parents=True, exist_ok=True)
    ids = Counter()

    patients, encounters, conditions, observations, notes = [], [], [], [], []

    for _ in range(N_PATIENTS):
        pid = ids.nxt("pat")
        gender = rng.choice(GENDERS)
        # full DOB = PHI (silver will date-shift); ages 25–80
        dob = date(rng.randint(1945, 2000), rng.randint(1, 12), rng.randint(1, 28))

        # --- risk signal (drives the ML label): older + severe pain + poor adherence + OA ---
        base_pain = rng.randint(2, 9)
        adherence = _round(rng.uniform(0.1, 1.0), 2)
        age = 2026 - dob.year
        cond = rng.choice(CONDITIONS)
        risk = (
            0.015 * age
            + 0.18 * base_pain
            + 0.9 * (1 - adherence)
            + (0.8 if cond[0] in ("M17.0", "M51.26") else 0.0)
            + rng.gauss(0, 0.5)
        )
        surgery = int(risk > 3.4)

        # --- Patient (PHI present) ---
        patient = {
            "resourceType": "Patient",
            "id": pid,
            "name": [{"given": [rng.choice(FIRST)], "family": rng.choice(LAST)}],  # PHI
            "gender": gender,
            "birthDate": dob.isoformat(),  # PHI (full date)
            "identifier": [{"system": "ssn", "value": f"{rng.randint(100,999)}-{rng.randint(10,99)}-{rng.randint(1000,9999)}"}],  # PHI
            "address": [{"city": rng.choice(["Porto", "Lisboa", "Braga", "Coimbra"]), "postalCode": f"{rng.randint(1000,4999)}-{rng.randint(100,999)}"}],  # PHI
            "_label_surgery_90d": surgery,  # kept for the demo label; stripped at silver into a separate target
        }
        # mess: ~10% missing gender, ~6% missing birthDate
        if rng.random() < 0.10:
            patient.pop("gender", None)
        if rng.random() < 0.06:
            patient.pop("birthDate", None)
        patients.append(patient)

        # --- Encounters ---
        n_enc = rng.randint(1, 5)
        enc_dates = sorted(date(2025, rng.randint(1, 12), rng.randint(1, 28)) for _ in range(n_enc))
        for ed in enc_dates:
            encounters.append({
                "resourceType": "Encounter", "id": ids.nxt("enc"), "subject": {"reference": f"Patient/{pid}"},
                "period": {"start": ed.isoformat()}, "class": {"code": "AMB", "display": "ambulatory"},
            })

        # --- Condition (sometimes free text instead of code = the validity mess) ---
        if rng.random() < 0.2:
            conditions.append({
                "resourceType": "Condition", "id": ids.nxt("cond"), "subject": {"reference": f"Patient/{pid}"},
                "code": {"text": cond[2]},  # free text only, no coding
            })
        else:
            conditions.append({
                "resourceType": "Condition", "id": ids.nxt("cond"), "subject": {"reference": f"Patient/{pid}"},
                "code": {"coding": [{"system": "http://hl7.org/fhir/sid/icd-10", "code": cond[0], "display": cond[1]}]},
            })

        # --- Observations across encounters ---
        for ed in enc_dates:
            # pain (PRO, 0-10) — drift downward if adherent
            pain = max(0, min(10, round(base_pain - adherence * rng.uniform(0, 3))))
            observations.append(_obs(ids, pid, ed, "38208-5", "Pain severity", pain, "{score}", rng))
            # adherence %
            observations.append(_obs(ids, pid, ed, "adherence-pct", "Home program adherence", _round(adherence * 100), "%", rng))
            # heart rate
            observations.append(_obs(ids, pid, ed, LOINC_HR, "Heart rate", rng.randint(55, 95), "/min", rng))
            # glucose — MIXED UNITS (the canonical mess): 30% in mmol/L, no normalization
            if rng.random() < 0.3:
                observations.append(_obs(ids, pid, ed, LOINC_GLUCOSE, "Glucose", _round(rng.uniform(3.9, 9.0)), "mmol/L", rng))
            else:
                observations.append(_obs(ids, pid, ed, LOINC_GLUCOSE, "Glucose", rng.randint(70, 160), "mg/dL", rng))

        # --- Clinical note (free text → for the vector/RAG demo) ---
        notes.append({
            "resourceType": "DocumentReference", "id": ids.nxt("note"), "subject": {"reference": f"Patient/{pid}"},
            "date": enc_dates[-1].isoformat(),
            "text": rng.choice(NOTE_TEMPLATES).format(
                sev=SEV[min(2, base_pain // 4)], site=rng.choice(SITE), agg=rng.choice(AGG),
                adh=("good adherence" if adherence > 0.66 else "partial adherence" if adherence > 0.33 else "poor adherence"),
                wk=rng.randint(2, 8),
            ),
        })

        # mess: ~5% duplicate the patient + their condition (exact dupes)
        if rng.random() < 0.05:
            patients.append(dict(patient))

    _write("patients", patients)
    _write("encounters", encounters)
    _write("conditions", conditions)
    _write("observations", observations)
    _write("notes", notes)

    counts = {"patients": len(patients), "encounters": len(encounters), "conditions": len(conditions),
              "observations": len(observations), "notes": len(notes)}
    print("bronze generated:", counts)
    return counts


def _obs(ids, pid, ed, code, display, value, unit, rng):
    obs = {
        "resourceType": "Observation", "id": ids.nxt("obs"), "subject": {"reference": f"Patient/{pid}"},
        "effectiveDateTime": ed.isoformat(),
        "code": {"coding": [{"system": "http://loinc.org", "code": code, "display": display}]},
    }
    # schema drift: ~15% use flat "value" instead of valueQuantity
    if rng.random() < 0.15:
        obs["value"] = value
        obs["unit"] = unit
    else:
        obs["valueQuantity"] = {"value": value, "unit": unit}
    # mess: ~4% missing value entirely
    if rng.random() < 0.04:
        obs.pop("value", None)
        obs.pop("valueQuantity", None)
    return obs


def _write(name: str, rows: list[dict]) -> None:
    p = BRONZE / f"{name}.ndjson"
    with p.open("w") as f:
        for r in rows:
            f.write(json.dumps(r) + "\n")


if __name__ == "__main__":
    generate()
