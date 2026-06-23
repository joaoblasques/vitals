# ADR 0003 — Conform to the OMOP Common Data Model

**Status:** accepted · 2026-06-23

## Context
Source health data uses competing vocabularies and shapes (FHIR resources, ICD-10, LOINC, free
text). Analysts and researchers need one recognizable, standardized representation.

## Decision
Standardize codes (ICD-10, LOINC, SNOMED targets, RxNorm) in silver and conform to the **OMOP CDM**
(`omop_person`, `omop_condition_occurrence`, `omop_measurement`) in dbt gold, mapping source codes
to standard concepts via a `concept_map` seed.

## Consequences
- Output is instantly familiar to anyone in health-data analytics (OHDSI ecosystem).
- The concept map is a small seed here (illustrative IDs); production loads the full OHDSI Athena
  vocabulary — a drop-in swap.
- Referential integrity (`person_id` FKs) is enforced with dbt tests.
