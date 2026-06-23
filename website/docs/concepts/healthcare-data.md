# Healthcare data: FHIR · PHI · OMOP

Three ideas make Vitals a *health*-data project rather than generic ETL. Understanding how they fit
together is the key to the whole pipeline: **FHIR in → remove PHI → conform to OMOP → serve**.

## FHIR — the format data arrives in

**FHIR** (Fast Healthcare Interoperability Resources, by **HL7**) is the modern web standard for
exchanging clinical data as JSON. Everything is a **Resource** — a standardized object, one per kind:

| Resource | Represents |
|---|---|
| `Patient` | demographics |
| `Encounter` | a visit |
| `Condition` | a diagnosis |
| `Observation` | one measurement — a lab, a vital, or a score |
| `MedicationRequest` / `Procedure` | medications / procedures |
| `DocumentReference` | a clinical note |

Resources link by reference (an `Observation.subject` points to a `Patient`). They're commonly
exported as **NDJSON** (one JSON object per line — the FHIR bulk-export format).

!!! note "In Vitals"
    FHIR records (generated with [Synthea](https://github.com/synthetichealth/synthea)) land in
    **bronze** and are flattened into tabular form in **silver**. Schema variation ("extensions
    /profiles") is normalized there.

## PHI — what must be removed

**PHI** (Protected Health Information) is health data that can identify a person, defined by
**HIPAA** (the US health-privacy law). The **Safe Harbor** method lists **18 identifiers** to remove:
names, sub-state geography, *all dates finer than year*, phone/email, SSN, medical record numbers,
device IDs, and more.

!!! warning "In Vitals — the de-identification boundary"
    PHI exists **only in bronze** (access-gated). **Silver is the de-identified boundary**: the 18
    identifiers are dropped, the patient id becomes a salted hash, dates are shifted per-patient
    (preserving intervals), and age is capped at 90. A **build-time assertion fails the pipeline if
    any PHI column survives** into silver — governance enforced as code.

## OMOP CDM — the shape data is analyzed in

**OMOP CDM** (Observational Medical Outcomes Partnership Common Data Model, from **OHDSI**) is a
**standard schema + standard vocabulary** you transform disparate datasets *into*, so the same
analytics and tools run anywhere.

- **One format** — tables: `person`, `condition_occurrence`, `measurement`, `visit_occurrence`, …
- **One vocabulary** — standard integer `concept_id`s (e.g. `8507` = male) that source codes
  (ICD-10, LOINC) map to, via OHDSI's **Athena** vocabulary repository.

!!! note "In Vitals"
    Silver standardizes codes (ICD-10 / LOINC / SNOMED / RxNorm); gold conforms to OMOP
    (`omop_person`, `omop_condition_occurrence`, `omop_measurement`) using a `concept_map` seed.
    In production that map is loaded from the full OHDSI Athena vocabulary.

## FHIR vs OMOP — a common point of confusion

They're both "health-data standards," but they do opposite jobs:

| | FHIR | OMOP CDM |
|---|---|---|
| **Purpose** | **Exchange** data between systems | **Analyze** data across a population |
| **Shape** | Nested JSON Resources, transactional | Flat relational tables, analytical |
| **In Vitals** | the **ingest** format (bronze) | the **analytics** target (gold) |

**In one line:** FHIR moves data between systems; OMOP analyzes it across a population. Vitals goes
**FHIR-in → OMOP-out**.

## Who makes what

- **HL7** → publishes **FHIR** (exchange).
- **OHDSI** → maintains **OMOP** + the **Athena** vocabularies (analytics).
- **HIPAA** → defines **PHI**, removed via **Safe Harbor** (privacy).

See the [Glossary](glossary.md) for every term, and [Governance](../governance.md) for how the PHI
boundary maps to Unity Catalog in production.
