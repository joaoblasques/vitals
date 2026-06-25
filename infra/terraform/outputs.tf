output "catalogs" {
  description = "The three medallion catalog names."
  value = {
    bronze = databricks_catalog.bronze.name
    silver = databricks_catalog.silver.name
    gold   = databricks_catalog.gold.name
  }
}

output "bronze_landing_volume" {
  description = "Fully-qualified path of the raw landing volume."
  value       = "${databricks_catalog.bronze.name}.${databricks_schema.bronze_raw.name}.${databricks_volume.landing.name}"
}

output "gold_schemas" {
  description = "Gold serving + monitoring schema names."
  value       = [for s in databricks_schema.gold : s.name]
}
