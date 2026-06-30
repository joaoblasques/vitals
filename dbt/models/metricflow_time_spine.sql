-- Required by the dbt Semantic Layer (MetricFlow) — one row per calendar day.
-- Cross-dialect: DuckDB's range() generates the series natively; Spark/Databricks has no date-range()
-- (its range() is integer-only), so on that adapter we build the array with sequence() and explode it.
-- No dbt_utils needed on either. The DuckDB branch is unchanged, so local MetricFlow output is identical.
{{ config(materialized='table') }}

{% if target.type == 'duckdb' %}
select
    range::date as date_day
from range(
    date '2000-01-01',
    date '2030-01-01',
    interval '1 day'
)
{% else %}
select
    explode(sequence(date '2000-01-01', date '2030-01-01', interval 1 day)) as date_day
{% endif %}
