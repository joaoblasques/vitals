# Results

This page will fill in as the pipeline produces output — the proof that the architecture works.

!!! note "Coming with Phase 1"
    Once the MVP vertical slice lands, this page will show:

    - **Data quality** — before/after the mess-injector: validity, completeness, unit-consistency,
      uniqueness scores at the silver gate.
    - **Analytics marts** — sample `dim_patient` / `fct_observation` and a governed metric.
    - **Feature store** — a registered Feast feature view and a point-in-time training set.
    - **Vector index** — a RAG query over clinical notes returning relevant passages.
    - **Demo model** — an adherence / surgery-risk model card (MLflow) consuming the features.

Check the [Dev Log](dev-log.md) for the latest progress.
