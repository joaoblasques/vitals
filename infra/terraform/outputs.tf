output "catalogs" {
  description = "The three medallion catalog names."
  value = {
    bronze = data.databricks_catalog.bronze.name
    silver = data.databricks_catalog.silver.name
    gold   = data.databricks_catalog.gold.name
  }
}

output "bronze_landing_volume" {
  description = "Fully-qualified path of the raw landing volume."
  value       = "${data.databricks_catalog.bronze.name}.${databricks_schema.bronze_raw.name}.${databricks_volume.landing.name}"
}

output "gold_schemas" {
  description = "Gold serving + monitoring schema names."
  value       = [for s in databricks_schema.gold : s.name]
}
