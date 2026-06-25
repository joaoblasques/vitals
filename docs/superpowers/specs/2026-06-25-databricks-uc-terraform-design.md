# Design — Terraform → Databricks Unity Catalog IaC

_Date: 2026-06-25 · Status: approved (design) · Phase: closes Phase 0 deployment target_

## Goal

Provision the Vitals lakehouse's Unity Catalog object graph **reproducibly from code**, with the
**PHI boundary expressed as an access-control boundary** (grants), not just prose. This closes the
last open roadmap item (Phase 0: "Databricks Free Edition workspace; Unity Catalog + Delta schema").

Non-goal (explicitly out of scope): wiring the existing local DuckDB pipeline to write Delta-on-UC.
That is the natural follow-up unit, tracked separately.

## Context: Databricks Free Edition constraints

Verified against current Databricks docs (2026-06):

- Free Edition = **one workspace, one metastore per account, Unity Catalog enabled by default**,
  serverless-only, **no account console / account-level APIs**.
- You **can** create catalogs, schemas, volumes, and grants at the **workspace level** via the
  Terraform `databricks` provider authenticated with a **workspace PAT**.
- Therefore we **skip** the Premium-only, account-level UC automation (metastore creation, storage
  credentials, external locations). Free Edition supplies a managed metastore with default managed
  storage, so managed catalogs/schemas/volumes/tables need no `storage_root` or external location.

This is what keeps the Terraform clean and committable: it is purely the workspace UC object graph
plus grants.

## Catalog model — catalog-per-medallion-layer

Chosen over single-catalog so the PHI boundary is a **catalog-level grant** (strongest governance
story; matches `website/docs/governance.md`'s "UC catalogs/schemas with grants per tier").

| Catalog | Tier | Schemas | Notes |
|---|---|---|---|
| `vitals_bronze` | **PHI (gated)** | `raw` | + managed **volume** `landing` for raw NDJSON; only engineers/service principal may `SELECT` |
| `vitals_silver` | de-identified | `clinical`, `omop` | PHI boundary is crossed here; downstream carries no identifiers |
| `vitals_gold` | consumption | `marts`, `features`, `vectors`, `monitoring` | the three gold stores + PSI drift outputs |

Catalog names are prefixed via a `catalog_prefix` variable (default `vitals`) so the same code can
stand up `dev`/`prod` instances without collision.

## Grants — the signature

Two **parameterized principal groups** (variables, default to Databricks account group names):

- `data_engineers` — full access to all three catalogs (incl. bronze PHI).
- `analysts` — `USE CATALOG` + `SELECT` on **silver and gold only**; **no grant on bronze**.

This encodes "PHI boundary at silver" as access control: the absence of any bronze grant to
`analysts` *is* the enforcement. The `landing` volume in bronze is likewise engineer-only.

Honest caveat (documented in the README): on Free Edition there is effectively a single user, so
these grants are partly **symbolic** in that environment. They are written against parameterized
group names so they become real, enforced policy on a Premium account. The grants are a deliberate
governance artifact, not runtime-critical for the Free Edition demo.

## Repository layout

```
infra/terraform/
  versions.tf            # terraform >= 1.6; databricks provider pinned to a known-good version
  providers.tf           # databricks provider; host + token sourced from env vars
  variables.tf           # catalog_prefix, data_engineers_group, analysts_group
  catalogs.tf            # 3 databricks_catalog (bronze/silver/gold)
  schemas.tf             # databricks_schema per the table above
  volumes.tf             # databricks_volume "landing" (managed) in vitals_bronze.raw
  grants.tf              # databricks_grants for the two principal groups + PHI gating
  outputs.tf             # fully-qualified catalog/schema/volume names
  terraform.tfvars.example
  README.md              # apply steps, Free Edition notes, the symbolic-grants caveat
```

## Auth, state, secrets

- **Auth:** workspace PAT via `DATABRICKS_HOST` and `DATABRICKS_TOKEN` environment variables. No
  host or token committed. `terraform.tfvars.example` shows the (non-secret) variable shape only.
- **State:** local state, gitignored (`*.tfstate`, `*.tfstate.*`, `.terraform/`, `.terraform.lock.hcl`
  kept). A remote backend is documented in the README as the Premium/team upgrade.
- **Secrets discipline:** repo is public; verify `.gitignore` covers all Terraform state and any
  `*.tfvars` (the `.example` is the only tracked tfvars).

## Apply strategy & verification

- I will get `terraform fmt -check` and `terraform validate` **passing offline** — these need only
  `terraform init` against the provider registry, no Databricks credentials.
- `terraform plan` / `terraform apply` **cannot be run here** (no Free Edition workspace or PAT in
  this environment). They are documented step-by-step in the README for the user to run. The work is
  **not** claimed as "applied" — only as validated IaC ready to apply.

## Testing

- `terraform fmt -check` (style gate) and `terraform validate` (config gate) are the automated
  checks; wire them as a CI gate consistent with the project's "DQ/checks as CI gates" principle
  (a `terraform validate` job — implementation-plan detail).
- Manual: documented `plan`/`apply` against a real Free Edition workspace produces the catalogs,
  schemas, volume, and grants; spot-check in Catalog Explorer.

## Out of scope

- Pipeline wiring (Delta-on-UC writes, job/asset-bundle deploy).
- Account-level UC (metastore, storage credentials, external locations) — not available and not
  needed on Free Edition.
- Compute (serverless is implicit on Free Edition; no cluster/warehouse Terraform).
