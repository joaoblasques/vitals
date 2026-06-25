# Vitals — Databricks Unity Catalog (Terraform)

Provisions the Vitals lakehouse's Unity Catalog object graph on **Databricks Free Edition**:
schemas, a raw-landing volume, and PHI-gating grants — **inside** three catalogs you create once in
the UI (see the Free Edition constraints below).

## What this creates

| Catalog | Tier | Created by | Schemas | Extra |
|---|---|---|---|---|
| `vitals_bronze` | PHI (gated) | **UI (prereq)** | `raw` | managed volume `landing` |
| `vitals_silver` | de-identified | **UI (prereq)** | `clinical`, `omop` | — |
| `vitals_gold` | consumption | **UI (prereq)** | `marts`, `features`, `vectors`, `monitoring` | — |

Terraform creates everything **inside** the catalogs (7 schemas, the `landing` volume, 3 grants);
the catalog shells themselves are a one-time UI step (see below). Catalogs are consumed via
`data "databricks_catalog"` — a missing one fails the plan with a clear "not found".

The **PHI boundary** is enforced as a catalog grant: the analysts principal is granted read on
silver + gold only — **never bronze**. It is live, not symbolic (verify with `terraform output` +
Catalog Explorer).

## Free Edition constraints (why the hybrid shape)

Free Edition ships a managed metastore (UC on, serverless-only) and exposes **no account-level
API**. Two hard limits shape this module — both verified against the live workspace, not assumed:

1. **Catalog creation is UI-only.** With Default Storage enabled, `CREATE CATALOG` via
   API/CLI/Terraform is rejected: *"Please use the UI to create a catalog with Default Storage"*
   (databricks/cli#4513, closed not-planned). So the three catalog shells are created by hand in the
   UI; Terraform manages the rest. This is the documented "manual GUI is the last resort" escape.
2. **UC grants resolve only account-level principals.** Custom groups (`data_engineers`/`analysts`)
   require the account console (Premium) and are rejected ("Could not find principal"). On Free
   Edition the grants map to the **workspace owner** (engineers) and the built-in **`account users`**
   group (analysts) — parameterized so the same code targets real account groups on Premium.

Everything else (schemas, managed volume, grants) provisions cleanly from Terraform.

## Prerequisites

- Terraform >= 1.6
- A Databricks Free Edition workspace
- A workspace **personal access token** (User Settings → Developer → Access tokens), scope `all-apis`
- **The three catalog shells created in the UI** (one-time): Catalog Explorer → Create catalog →
  name `vitals_bronze` / `vitals_silver` / `vitals_gold`, leave storage empty (Default Storage),
  Create. Do not add schemas — Terraform does that.

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

Verify in Catalog Explorer that the schemas, the `landing` volume, and the grants exist. Or:

```bash
terraform output
# bronze: owner only (PHI gated); silver/gold: owner + analysts read.
```

## Grants on Free Edition

Free Edition is effectively single-user, so the grants don't gate a second human here — but they
are **live, not symbolic**: `account users` is granted read on silver + gold and is **absent from
bronze**, so the PHI boundary is actually enforced at the catalog level. The two roles are
**parameterized** (`var.engineers_principal` — empty means the current user; `var.analysts_principal`
— default `account users`) so the exact same code enforces real policy with dedicated account groups
on a Premium account, e.g. `-var analysts_principal=analysts`.

## State

Local state, gitignored. For team use, configure a remote backend (e.g. S3/GCS) — not required for
the single-operator Free Edition demo.

## Variables

| Variable | Default | Purpose |
|---|---|---|
| `catalog_prefix` | `vitals` | Prefixes the three catalogs (`vitals_bronze/silver/gold`) |
| `engineers_principal` | `""` (→ current user) | Full access incl. bronze PHI; set to an account group on Premium |
| `analysts_principal` | `account users` | Read silver + gold only (never bronze); set to `analysts` on Premium |
