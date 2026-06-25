# Bronze: single raw schema (the landing volume lives here).
resource "databricks_schema" "bronze_raw" {
  catalog_name = data.databricks_catalog.bronze.name
  name         = "raw"
  comment      = "Raw landed source data, as-is."
}

# Silver: conformed clinical + OMOP CDM.
resource "databricks_schema" "silver_clinical" {
  catalog_name = data.databricks_catalog.silver.name
  name         = "clinical"
  comment      = "De-identified, conformed clinical entities."
}

resource "databricks_schema" "silver_omop" {
  catalog_name = data.databricks_catalog.silver.name
  name         = "omop"
  comment      = "OMOP CDM (person, condition_occurrence, measurement)."
}

# Gold: the three serving stores + drift monitoring.
locals {
  gold_schemas = {
    marts      = "Kimball star marts (dbt)."
    features   = "Feast offline/online feature tables."
    vectors    = "pgvector-style vector index over clinical notes."
    monitoring = "PSI drift-monitoring outputs over the feature store."
  }
}

resource "databricks_schema" "gold" {
  for_each     = local.gold_schemas
  catalog_name = data.databricks_catalog.gold.name
  name         = each.key
  comment      = each.value
}
