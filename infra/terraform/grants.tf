# The signature of the project: the PHI boundary expressed as access control.
#
# On Free Edition, Unity Catalog grants resolve only ACCOUNT-level principals — the workspace
# owner's user, and the built-in `account users` group. Custom groups (data_engineers/analysts)
# require the account console (Premium) and are rejected here ("Could not find principal").
# So we map the two roles to FE-available principals — which makes the boundary LIVE and
# demonstrable here, not merely declared — and parameterize them so the SAME code targets real
# account groups on Premium:
#   engineers -> the workspace owner (current user), resolved dynamically (no email in the repo)
#   analysts  -> `account users` by default (every workspace user); set to "analysts" on Premium
#
# `databricks_grants` is authoritative per securable: it sets the FULL grant list, so it also
# strips the default `account users: BROWSE` from bronze — that removal IS the PHI gate.

data "databricks_current_user" "me" {}

locals {
  engineers_principal = var.engineers_principal != "" ? var.engineers_principal : data.databricks_current_user.me.user_name
  analysts_principal  = var.analysts_principal
}

# Bronze (PHI, gated): engineers only. The ABSENCE of any analyst grant IS the enforcement.
resource "databricks_grants" "bronze" {
  catalog = data.databricks_catalog.bronze.name

  grant {
    principal  = local.engineers_principal
    privileges = ["ALL_PRIVILEGES"]
  }
}

# Silver (de-identified): engineers full; analysts read.
resource "databricks_grants" "silver" {
  catalog = data.databricks_catalog.silver.name

  grant {
    principal  = local.engineers_principal
    privileges = ["ALL_PRIVILEGES"]
  }

  grant {
    principal  = local.analysts_principal
    privileges = ["USE_CATALOG", "SELECT"]
  }
}

# Gold (consumption): engineers full; analysts read.
resource "databricks_grants" "gold" {
  catalog = data.databricks_catalog.gold.name

  grant {
    principal  = local.engineers_principal
    privileges = ["ALL_PRIVILEGES"]
  }

  grant {
    principal  = local.analysts_principal
    privileges = ["USE_CATALOG", "SELECT"]
  }
}
