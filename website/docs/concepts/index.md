# Concepts

The building blocks behind Vitals — the data-engineering and healthcare ideas the pipeline is made
of, explained plainly. If a term on another page is unfamiliar, it's defined here.

- **[Healthcare data: FHIR · PHI · OMOP](healthcare-data.md)** — the three ideas that make this a
  *health*-data project, and how they fit together (FHIR in → de-identify → OMOP out).
- **[Glossary](glossary.md)** — every term used across the site, one line each.

## The shape of the whole thing, in five sentences

1. **Medallion** — data flows through three quality tiers: **bronze** (raw), **silver** (cleaned &
   de-identified), **gold** (ready to consume).
2. Sources arrive as **FHIR** (the clinical-data exchange standard), plus claims, patient-reported
   outcomes, wearables, and notes.
3. **Silver** is the trust boundary — **PHI** (protected health information) is removed here, codes
   are standardized, and data quality is enforced.
4. **Gold** is served three ways because analytics, ML, and LLM/RAG each need a different shape:
   dbt **analytics marts** (conformed to the **OMOP** common data model), a **feature store**, and a
   **vector index**.
5. It runs locally on DuckDB for reproducibility, with Databricks/Delta as the deployment target and
   Spark for scale.

See **[Architecture](../architecture.md)** for the diagram and **[Results](../results.md)** for what
the pipeline actually produces.
