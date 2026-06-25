# Vitals — Databricks Unity Catalog (Terraform)

Provisions the Vitals lakehouse's Unity Catalog object graph on **Databricks Free Edition**:
catalog-per-medallion-layer, schemas, a raw-landing volume, and PHI-gating grants.

## What this creates

| Catalog | Tier | Schemas | Extra |
|---|---|---|---|
| `vitals_bronze` | PHI (gated) | `raw` | managed volume `landing` |
| `vitals_silver` | de-identified | `clinical`, `omop` | — |
| `vitals_gold` | consumption | `marts`, `features`, `vectors`, `monitoring` | — |

The **PHI boundary** is enforced as a catalog grant: the `analysts` group is granted read on
silver + gold only — never bronze.

## Why no metastore / storage credentials here

Free Edition ships a managed metastore with Unity Catalog enabled and serverless-only compute, and
exposes **no account-level API**. So this module provisions only **workspace-level** UC objects.
The Premium-only account setup (metastore, storage credentials, external locations) is deliberately
absent — Free Edition provides it.

## Prerequisites

- Terraform >= 1.6
- A Databricks Free Edition workspace
- A workspace **personal access token** (User Settings → Developer → Access tokens)

## Authenticate (no secrets in the repo)

```bash
export DATABRICKS_HOST="https://<your-workspace>.cloud.databricks.com"
export DATABRICKS_TOKEN="dapi..."
```

## Apply

```bash
cd infra/terraform
terraform init
terraform plan      # review the object graph
terraform apply
```

Verify in Catalog Explorer that the three catalogs, their schemas, the `landing` volume, and the
grants exist.

## Grants are partly symbolic on Free Edition

Free Edition is effectively single-user, so the `data_engineers` / `analysts` grants don't gate a
second human there. They are written against **parameterized account-group names**
(`var.data_engineers_group`, `var.analysts_group`) so the exact same code enforces real policy on a
Premium account. They are a deliberate governance artifact.

## State

Local state, gitignored. For team use, configure a remote backend (e.g. S3/GCS) — not required for
the single-operator Free Edition demo.

## Variables

| Variable | Default | Purpose |
|---|---|---|
| `catalog_prefix` | `vitals` | Prefixes the three catalogs |
| `data_engineers_group` | `data_engineers` | Full access (incl. bronze PHI) |
| `analysts_group` | `analysts` | Read silver + gold only |
