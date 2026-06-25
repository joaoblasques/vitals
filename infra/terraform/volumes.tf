# Managed volume for raw NDJSON/file landing in the gated bronze tier.
# MANAGED = backed by the metastore's default storage (no external location needed on Free Edition).
resource "databricks_volume" "landing" {
  name         = "landing"
  catalog_name = databricks_catalog.bronze.name
  schema_name  = databricks_schema.bronze_raw.name
  volume_type  = "MANAGED"
  comment      = "Raw source-file landing zone (PHI-bearing, engineer-only)."
}
