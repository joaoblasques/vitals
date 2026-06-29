-- Required by the dbt Semantic Layer (MetricFlow) — one row per calendar day.
-- DuckDB range() generates the series natively; no dbt_utils needed.
{{ config(materialized='table') }}

select
    range::date as date_day
from range(
    date '2000-01-01',
    date '2030-01-01',
    interval '1 day'
)
