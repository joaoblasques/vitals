variable "catalog_prefix" {
  description = "Prefix for the three medallion catalogs, e.g. 'vitals' -> vitals_bronze/silver/gold."
  type        = string
  default     = "vitals"

  validation {
    condition     = can(regex("^[a-z][a-z0-9_]*$", var.catalog_prefix))
    error_message = "catalog_prefix must be lower-case alphanumeric/underscore and start with a letter."
  }
}

variable "data_engineers_group" {
  description = "Unity Catalog account group with full access to all layers (incl. bronze PHI)."
  type        = string
  default     = "data_engineers"
}

variable "analysts_group" {
  description = "Unity Catalog account group with read access to silver + gold only (never bronze)."
  type        = string
  default     = "analysts"
}
