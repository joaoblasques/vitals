# Catalog-per-medallion-layer. The PHI boundary is a catalog boundary:
# bronze carries identifiers (gated); silver and downstream are de-identified.
# On Free Edition these are managed catalogs backed by the metastore's default storage.

resource "databricks_catalog" "bronze" {
  name    = "${var.catalog_prefix}_bronze"
  comment = "Raw, messy, PHI-bearing ingest (FHIR/claims/wearable/PRO/notes). Gated tier."
}

resource "databricks_catalog" "silver" {
  name    = "${var.catalog_prefix}_silver"
  comment = "De-identified (HIPAA Safe Harbor + date-shift), conformed, OMOP CDM. PHI boundary crossed here."
}

resource "databricks_catalog" "gold" {
  name    = "${var.catalog_prefix}_gold"
  comment = "Consumption: analytics marts, feature store, vector index, drift monitoring."
}
