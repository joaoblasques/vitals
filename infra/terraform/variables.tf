variable "catalog_prefix" {
  description = "Prefix for the three medallion catalogs, e.g. 'vitals' -> vitals_bronze/silver/gold."
  type        = string
  default     = "vitals"

  validation {
    condition     = can(regex("^[a-z][a-z0-9_]*$", var.catalog_prefix))
    error_message = "catalog_prefix must be lower-case alphanumeric/underscore and start with a letter."
  }
}

# Engineers principal: full access to all layers incl. bronze PHI.
# Empty (default) = the workspace owner (current user), resolved dynamically so no email/PII is
# committed to this public repo. On Premium, set to an account group, e.g. "data_engineers".
variable "engineers_principal" {
  description = "UC principal with full access to all layers (incl. bronze PHI). Empty = current user."
  type        = string
  default     = ""
}

# Analysts principal: read access to silver + gold only, never bronze.
# Default `account users` is the built-in account group available on Free Edition (every workspace
# user). On Premium, set to a dedicated account group, e.g. "analysts".
variable "analysts_principal" {
  description = "UC principal with read access to silver + gold only (never bronze)."
  type        = string
  default     = "account users"
}
