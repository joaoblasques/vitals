"""Phase 4 — auto-generate a data dictionary + lineage page from dbt's artifacts.

Reads dbt's manifest.json + catalog.json (produced by `dbt docs generate`) and writes a
human-readable catalog to the docs site: a Mermaid lineage graph plus a per-model column
dictionary (type + description). Keeps governance docs in sync with the actual models.
"""
from __future__ import annotations

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
TARGET = ROOT / "dbt" / "target"
OUT = ROOT / "website" / "docs" / "catalog.md"


def _short(uid: str) -> str:
    return uid.split(".")[-1]


def generate() -> str:
    manifest = json.loads((TARGET / "manifest.json").read_text())
    catalog = json.loads((TARGET / "catalog.json").read_text())

    models = {uid: n for uid, n in manifest["nodes"].items() if n["resource_type"] == "model"}
    sources = {uid: s for uid, s in manifest.get("sources", {}).items()}

    # ---- lineage edges ----
    edges = []
    for uid, node in models.items():
        for dep in node["depends_on"]["nodes"]:
            if dep in models or dep in sources:
                edges.append((_short(dep), _short(uid)))

    lines = ["# Data Catalog & Lineage", "",
             "Auto-generated from dbt's `manifest.json` + `catalog.json` "
             "(`python -m vitals.catalog`). Regenerated whenever the models change.", "",
             "## Lineage", "", "```mermaid", "flowchart LR"]
    for src in sorted({_short(u) for u in sources}):
        lines.append(f"  {src}[({src})]")
    for a, b in sorted(set(edges)):
        lines.append(f"  {a} --> {b}")
    lines += ["```", "", "## Tables", ""]

    # ---- per-model dictionary ----
    for uid in sorted(models, key=_short):
        node = models[uid]
        name = _short(uid)
        cat = catalog["nodes"].get(uid, {}).get("columns", {})
        lines += [f"### `{name}`", "", node.get("description") or "_(no description)_", "",
                  "| Column | Type | Description |", "|---|---|---|"]
        cols = node.get("columns", {})
        # union of declared columns + catalog columns
        names = list(dict.fromkeys(list(cols) + list(cat)))
        for col in names:
            ctype = cat.get(col, {}).get("type", "")
            desc = cols.get(col, {}).get("description", "")
            lines.append(f"| {col} | {ctype} | {desc} |")
        lines.append("")

    OUT.write_text("\n".join(lines))
    print(f"catalog written: {OUT} ({len(models)} models, {len(edges)} lineage edges)")
    return str(OUT)


if __name__ == "__main__":
    generate()
