# The signature of the project: the PHI boundary expressed as access control.
# `databricks_grants` is authoritative per securable — it sets the full grant list.
# NOTE: on Free Edition (single user) these grants are partly symbolic; the group
# names are variables so they become enforced policy on a Premium account.

# Bronze (PHI, gated): engineers only. The ABSENCE of any analyst grant IS the enforcement.
resource "databricks_grants" "bronze" {
  catalog = databricks_catalog.bronze.name

  grant {
    principal  = var.data_engineers_group
    privileges = ["ALL_PRIVILEGES"]
  }
}

# Silver (de-identified): engineers full; analysts read.
resource "databricks_grants" "silver" {
  catalog = databricks_catalog.silver.name

  grant {
    principal  = var.data_engineers_group
    privileges = ["ALL_PRIVILEGES"]
  }

  grant {
    principal  = var.analysts_group
    privileges = ["USE_CATALOG", "SELECT"]
  }
}

# Gold (consumption): engineers full; analysts read.
resource "databricks_grants" "gold" {
  catalog = databricks_catalog.gold.name

  grant {
    principal  = var.data_engineers_group
    privileges = ["ALL_PRIVILEGES"]
  }

  grant {
    principal  = var.analysts_group
    privileges = ["USE_CATALOG", "SELECT"]
  }
}
