# Databricks UC Terraform IaC — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Provision the Vitals lakehouse's Unity Catalog object graph (catalog-per-medallion-layer + schemas + a raw-landing volume + PHI-gating grants) as reproducible Terraform, validated offline and ready to `apply` against a Databricks Free Edition workspace.

**Architecture:** A single root Terraform module under `infra/terraform/`, one file per responsibility. The `databricks` provider authenticates with a workspace PAT from env vars. Three catalogs (`vitals_bronze/silver/gold`) carry the medallion layers; the PHI boundary is enforced as a catalog-level grant (no bronze grant to analysts). Free Edition supplies the metastore + managed storage, so no account-level resources (metastore, storage credentials, external locations) appear.

**Tech Stack:** Terraform >= 1.6, `databricks/databricks` provider (~> 1.0), Databricks Free Edition (serverless, Unity Catalog), GitHub Actions for the fmt/validate gate.

## Global Constraints

- Terraform `required_version = ">= 1.6"`; provider `databricks/databricks` pinned `~> 1.0` (exact version recorded in committed `.terraform.lock.hcl`).
- **No secrets in repo** (public repo): host + token come only from `DATABRICKS_HOST` / `DATABRICKS_TOKEN` env vars. Never commit `*.tfstate`, `*.tfvars` (except `terraform.tfvars.example`), or `.databrickscfg`.
- Catalog names are `${var.catalog_prefix}_<layer>`; `catalog_prefix` defaults to `vitals`.
- Catalog/schema/volume layout is **exactly**: `vitals_bronze` → schema `raw` (+ managed volume `landing`); `vitals_silver` → schemas `clinical`, `omop`; `vitals_gold` → schemas `marts`, `features`, `vectors`, `monitoring`.
- Grants: `var.data_engineers_group` gets `ALL_PRIVILEGES` on all three catalogs; `var.analysts_group` gets `USE_CATALOG` + `SELECT` on **silver and gold only** — never on bronze.
- The gate is `terraform fmt -check -recursive` + `terraform validate`. `plan`/`apply` are NOT run in CI or by the implementer (no workspace/creds available); they are documented only.
- All Terraform commands run from `infra/terraform/` (the module root).

---

### Task 1: Scaffold module — versions, provider, gitignore

**Files:**
- Create: `infra/terraform/versions.tf`
- Create: `infra/terraform/providers.tf`
- Modify: `.gitignore` (repo root — append Terraform section)

**Interfaces:**
- Consumes: nothing.
- Produces: an initialized module where `terraform validate` runs. The `databricks` provider block (configured purely from env vars) is relied on by every later task.

- [ ] **Step 1: Write `infra/terraform/versions.tf`**

```hcl
terraform {
  required_version = ">= 1.6"

  required_providers {
    databricks = {
      source  = "databricks/databricks"
      version = "~> 1.0"
    }
  }
}
```

- [ ] **Step 2: Write `infra/terraform/providers.tf`**

```hcl
# Authentication comes entirely from the environment — never committed:
#   export DATABRICKS_HOST="https://<your-free-edition-workspace>.cloud.databricks.com"
#   export DATABRICKS_TOKEN="dapi..."   # workspace personal access token
# Free Edition is workspace-scoped (no account-level config).
provider "databricks" {}
```

- [ ] **Step 3: Append the Terraform section to `.gitignore`**

Add at the end of the repo-root `.gitignore`:

```gitignore
# Terraform
infra/terraform/.terraform/
*.tfstate
*.tfstate.*
*.tfvars
!*.tfvars.example
crash.log
crash.*.log
override.tf
override.tf.json
*_override.tf
*_override.tf.json
```

(Note: `.terraform.lock.hcl` is intentionally NOT ignored — it is committed for reproducible provider versions.)

- [ ] **Step 4: Initialize and validate**

Run:
```bash
cd infra/terraform
terraform init
terraform fmt -check -recursive
terraform validate
```
Expected: `init` downloads the databricks provider and writes `.terraform.lock.hcl`; `fmt -check` exits 0 (no diffs); `validate` prints `Success! The configuration is valid.`

- [ ] **Step 5: Commit**

```bash
git add infra/terraform/versions.tf infra/terraform/providers.tf infra/terraform/.terraform.lock.hcl .gitignore
git commit -m "feat(infra): scaffold Databricks Terraform module (provider + versions)"
```

---

### Task 2: Variables

**Files:**
- Create: `infra/terraform/variables.tf`
- Create: `infra/terraform/terraform.tfvars.example`

**Interfaces:**
- Consumes: nothing.
- Produces: `var.catalog_prefix` (string), `var.data_engineers_group` (string), `var.analysts_group` (string) — used by catalogs, grants, and outputs in later tasks.

- [ ] **Step 1: Write `infra/terraform/variables.tf`**

```hcl
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
```

- [ ] **Step 2: Write `infra/terraform/terraform.tfvars.example`**

```hcl
# Copy to terraform.tfvars (gitignored) and adjust. Values here are NON-SECRET.
# Auth is NOT set here — export DATABRICKS_HOST and DATABRICKS_TOKEN in your shell instead.

catalog_prefix       = "vitals"
data_engineers_group = "data_engineers"
analysts_group       = "analysts"
```

- [ ] **Step 3: Validate**

Run:
```bash
cd infra/terraform
terraform fmt -check -recursive && terraform validate
```
Expected: `fmt -check` exits 0; `validate` prints `Success! The configuration is valid.`

- [ ] **Step 4: Commit**

```bash
git add infra/terraform/variables.tf infra/terraform/terraform.tfvars.example
git commit -m "feat(infra): add Terraform variables (catalog prefix, principal groups)"
```

---

### Task 3: Catalogs

**Files:**
- Create: `infra/terraform/catalogs.tf`

**Interfaces:**
- Consumes: `var.catalog_prefix`.
- Produces: `databricks_catalog.bronze`, `databricks_catalog.silver`, `databricks_catalog.gold` (each exposes `.name`) — consumed by schemas, volumes, grants, outputs.

- [ ] **Step 1: Write `infra/terraform/catalogs.tf`**

```hcl
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
```

- [ ] **Step 2: Validate**

Run:
```bash
cd infra/terraform && terraform fmt -check -recursive && terraform validate
```
Expected: `Success! The configuration is valid.`

- [ ] **Step 3: Commit**

```bash
git add infra/terraform/catalogs.tf
git commit -m "feat(infra): three medallion catalogs (bronze/silver/gold)"
```

---

### Task 4: Schemas

**Files:**
- Create: `infra/terraform/schemas.tf`

**Interfaces:**
- Consumes: `databricks_catalog.bronze/silver/gold`.
- Produces: schema resources whose `.name` and `.catalog_name` are consumed by the volume (Task 5) and outputs (Task 7). Key resource address: `databricks_schema.bronze_raw`.

- [ ] **Step 1: Write `infra/terraform/schemas.tf`**

```hcl
# Bronze: single raw schema (the landing volume lives here).
resource "databricks_schema" "bronze_raw" {
  catalog_name = databricks_catalog.bronze.name
  name         = "raw"
  comment      = "Raw landed source data, as-is."
}

# Silver: conformed clinical + OMOP CDM.
resource "databricks_schema" "silver_clinical" {
  catalog_name = databricks_catalog.silver.name
  name         = "clinical"
  comment      = "De-identified, conformed clinical entities."
}

resource "databricks_schema" "silver_omop" {
  catalog_name = databricks_catalog.silver.name
  name         = "omop"
  comment      = "OMOP CDM (person, condition_occurrence, measurement)."
}

# Gold: the three serving stores + drift monitoring.
locals {
  gold_schemas = {
    marts      = "Kimball star marts (dbt)."
    features   = "Feast offline/online feature tables."
    vectors    = "pgvector-style vector index over clinical notes."
    monitoring = "PSI drift-monitoring outputs over the feature store."
  }
}

resource "databricks_schema" "gold" {
  for_each     = local.gold_schemas
  catalog_name = databricks_catalog.gold.name
  name         = each.key
  comment      = each.value
}
```

- [ ] **Step 2: Validate**

Run:
```bash
cd infra/terraform && terraform fmt -check -recursive && terraform validate
```
Expected: `Success! The configuration is valid.`

- [ ] **Step 3: Commit**

```bash
git add infra/terraform/schemas.tf
git commit -m "feat(infra): schemas per layer (bronze raw; silver clinical/omop; gold marts/features/vectors/monitoring)"
```

---

### Task 5: Raw-landing volume

**Files:**
- Create: `infra/terraform/volumes.tf`

**Interfaces:**
- Consumes: `databricks_catalog.bronze`, `databricks_schema.bronze_raw`.
- Produces: `databricks_volume.landing` (exposes `.name`) — consumed by outputs (Task 7).

- [ ] **Step 1: Write `infra/terraform/volumes.tf`**

```hcl
# Managed volume for raw NDJSON/file landing in the gated bronze tier.
# MANAGED = backed by the metastore's default storage (no external location needed on Free Edition).
resource "databricks_volume" "landing" {
  name         = "landing"
  catalog_name = databricks_catalog.bronze.name
  schema_name  = databricks_schema.bronze_raw.name
  volume_type  = "MANAGED"
  comment      = "Raw source-file landing zone (PHI-bearing, engineer-only)."
}
```

- [ ] **Step 2: Validate**

Run:
```bash
cd infra/terraform && terraform fmt -check -recursive && terraform validate
```
Expected: `Success! The configuration is valid.`

- [ ] **Step 3: Commit**

```bash
git add infra/terraform/volumes.tf
git commit -m "feat(infra): managed landing volume in bronze.raw"
```

---

### Task 6: Grants — PHI boundary as access control

**Files:**
- Create: `infra/terraform/grants.tf`

**Interfaces:**
- Consumes: `databricks_catalog.bronze/silver/gold`, `var.data_engineers_group`, `var.analysts_group`.
- Produces: `databricks_grants.bronze/silver/gold` (authoritative grant sets per catalog).

- [ ] **Step 1: Write `infra/terraform/grants.tf`**

```hcl
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
```

- [ ] **Step 2: Validate**

Run:
```bash
cd infra/terraform && terraform fmt -check -recursive && terraform validate
```
Expected: `Success! The configuration is valid.`

- [ ] **Step 3: Commit**

```bash
git add infra/terraform/grants.tf
git commit -m "feat(infra): catalog grants enforcing PHI boundary (analysts get silver+gold, never bronze)"
```

---

### Task 7: Outputs

**Files:**
- Create: `infra/terraform/outputs.tf`

**Interfaces:**
- Consumes: all catalog/schema/volume resources.
- Produces: named outputs (`catalogs`, `bronze_landing_volume`) for downstream pipeline wiring.

- [ ] **Step 1: Write `infra/terraform/outputs.tf`**

```hcl
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
```

- [ ] **Step 2: Validate**

Run:
```bash
cd infra/terraform && terraform fmt -check -recursive && terraform validate
```
Expected: `Success! The configuration is valid.`

- [ ] **Step 3: Commit**

```bash
git add infra/terraform/outputs.tf
git commit -m "feat(infra): outputs (catalog names, landing volume path, gold schemas)"
```

---

### Task 8: README — apply steps + Free Edition notes

**Files:**
- Create: `infra/terraform/README.md`

**Interfaces:**
- Consumes: nothing (documentation).
- Produces: operator runbook.

- [ ] **Step 1: Write `infra/terraform/README.md`**

````markdown
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
````

- [ ] **Step 2: Commit**

```bash
git add infra/terraform/README.md
git commit -m "docs(infra): Terraform README (apply steps, Free Edition notes, grants caveat)"
```

---

### Task 9: CI gate — fmt + validate

**Files:**
- Create: `.github/workflows/terraform.yml`

**Interfaces:**
- Consumes: the `infra/terraform/` module.
- Produces: a GitHub Actions job gating PRs on `fmt` + `validate`.

- [ ] **Step 1: Write `.github/workflows/terraform.yml`**

```yaml
name: Terraform

on:
  push:
    branches: [main]
    paths: ["infra/terraform/**", ".github/workflows/terraform.yml"]
  pull_request:
    paths: ["infra/terraform/**", ".github/workflows/terraform.yml"]
  workflow_dispatch:

permissions:
  contents: read

jobs:
  validate:
    runs-on: ubuntu-latest
    defaults:
      run:
        working-directory: infra/terraform
    steps:
      - uses: actions/checkout@v4
      - uses: hashicorp/setup-terraform@v3
        with:
          terraform_version: "1.9.8"
      - name: Format check
        run: terraform fmt -check -recursive
      - name: Init (no backend, no creds)
        run: terraform init -backend=false
      - name: Validate
        run: terraform validate
```

(`terraform init -backend=false` and `validate` need no Databricks credentials — they check syntax, provider schema, and references only. `plan`/`apply` are intentionally not run in CI.)

- [ ] **Step 2: Verify the workflow file parses locally (optional sanity)**

Run:
```bash
cd infra/terraform && terraform fmt -check -recursive && terraform init -backend=false && terraform validate
```
Expected: `Success! The configuration is valid.` (mirrors what CI runs)

- [ ] **Step 3: Commit**

```bash
git add .github/workflows/terraform.yml
git commit -m "ci(infra): gate Terraform on fmt + validate"
```

---

### Task 10: Roadmap + governance doc updates

**Files:**
- Modify: `website/docs/roadmap.md` (check the Phase 0 deployment box)
- Modify: `website/docs/governance.md` (link the production UC mapping to the now-real Terraform)

**Interfaces:**
- Consumes: nothing.
- Produces: docs reflecting the delivered IaC.

- [ ] **Step 1: Check the Phase 0 box in `website/docs/roadmap.md`**

Change:
```markdown
- [ ] Databricks Free Edition workspace; Unity Catalog + Delta schema (deployment target)
```
to:
```markdown
- [x] Databricks Free Edition workspace; Unity Catalog + Delta schema (deployment target) — Terraform IaC in `infra/terraform/`
```

- [ ] **Step 2: Add a line to the governance "Production (Databricks)" mapping in `website/docs/governance.md`**

In the MVP→Production table, under the `schema separation bronze/silver/gold` row's production
cell, ensure it reads (append the parenthetical if absent):
```markdown
| schema separation `bronze`/`silver`/`gold` | UC catalogs/schemas with grants per tier (provisioned by `infra/terraform/`) |
```

- [ ] **Step 3: Build docs strictly to confirm no broken Markdown**

Run:
```bash
cd website && mkdocs build --strict
```
Expected: build succeeds with no warnings (matches the `docs.yml` CI `--strict` gate).

- [ ] **Step 4: Commit**

```bash
git add website/docs/roadmap.md website/docs/governance.md
git commit -m "docs: mark Phase 0 deployment target done; link governance mapping to Terraform"
```

---

## Self-Review

**1. Spec coverage:**
- Free Edition constraints / skip account-level resources → Task 1 (provider, no metastore) + README (Task 8). ✓
- Catalog-per-layer model → Task 3. ✓
- Schemas incl. added `monitoring` → Task 4. ✓
- Raw-landing volume → Task 5. ✓
- Grants / PHI boundary as access control + symbolic caveat → Task 6 + README. ✓
- Layout (versions/providers/variables/catalogs/schemas/volumes/grants/outputs/tfvars.example/README) → Tasks 1–8. ✓
- Auth via env vars, no secrets → Task 1 providers + Task 2 tfvars.example + .gitignore. ✓
- Local gitignored state, lock committed → Task 1. ✓
- fmt + validate offline gate; no apply claimed → every task + Task 9 CI. ✓
- `catalog_prefix` dev/prod parameterization → Task 2. ✓
- Roadmap item closure → Task 10. ✓

**2. Placeholder scan:** No TBD/TODO/"handle edge cases"; all HCL is complete and concrete. ✓

**3. Type/name consistency:** Resource addresses are stable across tasks — `databricks_catalog.bronze/silver/gold`, `databricks_schema.bronze_raw`, `databricks_volume.landing`, `databricks_grants.bronze/silver/gold` are defined before they are referenced (catalogs → schemas → volume/grants → outputs). Gold schemas use a `for_each` map; outputs iterate it. ✓
