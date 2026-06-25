# Catalog-per-medallion-layer. The PHI boundary is a catalog boundary:
# bronze carries identifiers (gated); silver and downstream are de-identified.
#
# Free Edition (Default Storage) BLOCKS catalog creation via API/CLI/Terraform:
#   "Please use the UI to create a catalog with Default Storage."
# (databricks/cli#4513, closed not-planned). So the three catalog *shells* are created once,
# by hand, in the UI (Catalog Explorer -> Create catalog -> Default Storage) — the documented
# "manual GUI is the last resort" escape hatch. Terraform CONSUMES them here as data sources
# and manages everything *inside* them (schemas, the landing volume, the PHI-gating grants).
# A missing catalog fails the plan with a clear "not found", prompting the one-time UI step.
# See README "Prerequisite: create the catalog shells in the UI".

data "databricks_catalog" "bronze" {
  name = "${var.catalog_prefix}_bronze"
}

data "databricks_catalog" "silver" {
  name = "${var.catalog_prefix}_silver"
}

data "databricks_catalog" "gold" {
  name = "${var.catalog_prefix}_gold"
}
