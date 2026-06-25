# Authentication comes entirely from the environment — never committed:
#   export DATABRICKS_HOST="https://<your-free-edition-workspace>.cloud.databricks.com"
#   export DATABRICKS_TOKEN="dapi..."   # workspace personal access token
# Free Edition is workspace-scoped (no account-level config).
provider "databricks" {}
