"""Render dbt manifest graph nodes and dependencies as a layered Mermaid chart."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
from pathlib import Path
from typing import Any

LAYERS = ("sources", "staging", "snapshots", "intermediate", "marts", "ai_ready")
RESOURCE_LAYER = {"source": "sources", "snapshot": "snapshots"}


def _layer_for(node: dict[str, Any]) -> str:
    resource_type = node.get("resource_type")
    if resource_type in RESOURCE_LAYER:
        return RESOURCE_LAYER[resource_type]
    if resource_type != "model":
        raise ValueError(f"Unsupported dbt graph resource type: {resource_type!r}")

    fqn = [str(part).lower() for part in node.get("fqn", [])]
    file_path = str(node.get("original_file_path", "")).replace("\\", "/").lower()
    candidates = fqn + file_path.split("/")
    for layer in ("staging", "intermediate", "marts", "ai_ready"):
        if layer in candidates:
            return layer

    name = str(node.get("name", "")).lower()
    prefixes = (("stg_", "staging"), ("int_", "intermediate"),
                ("dim_", "marts"), ("fct_", "marts"), ("ai_", "ai_ready"))
    for prefix, layer in prefixes:
        if name.startswith(prefix):
            return layer
    raise ValueError(f"Could not assign dbt model {node.get('unique_id')!r} to a layer")


def _mermaid_id(unique_id: str) -> str:
    slug = re.sub(r"[^A-Za-z0-9_]", "_", unique_id)
    suffix = hashlib.sha1(unique_id.encode("utf-8")).hexdigest()[:8]
    return f"node_{slug}_{suffix}"


def _label(node: dict[str, Any]) -> str:
    resource_type = node.get("resource_type")
    name = str(node.get("name") or node.get("alias") or node.get("unique_id"))
    if resource_type == "source":
        source_name = str(node.get("source_name", ""))
        if source_name:
            name = f"{source_name}.{name}"
    return name.replace("&", "&amp;").replace('"', "&quot;").replace("<", "&lt;").replace(">", "&gt;")


def render_lineage(manifest: dict[str, Any]) -> str:
    """Return a left-to-right Mermaid flowchart from a dbt manifest mapping."""
    selected: dict[str, dict[str, Any]] = {}
    for section in ("nodes", "sources"):
        for unique_id, node in manifest.get(section, {}).items():
            if node.get("resource_type") in {"model", "source", "snapshot"}:
                selected[unique_id] = node

    layers: dict[str, list[str]] = {layer: [] for layer in LAYERS}
    for unique_id, node in selected.items():
        layers[_layer_for(node)].append(unique_id)

    lines = ["flowchart LR"]
    for layer in LAYERS:
        lines.append(f'  subgraph {layer}["{layer}"]')
        lines.append("    direction TB")
        for unique_id in sorted(layers[layer]):
            node_id = _mermaid_id(unique_id)
            lines.append(f'    {node_id}["{_label(selected[unique_id])}"]')
        lines.append("  end")

    edges: list[tuple[str, str]] = []
    for unique_id, node in selected.items():
        if node.get("resource_type") not in {"model", "snapshot"}:
            continue
        dependent_id = _mermaid_id(unique_id)
        for dependency_id in node.get("depends_on", {}).get("nodes", []):
            if dependency_id not in selected:
                raise ValueError(
                    f"Dependency {dependency_id!r} of {unique_id!r} is not a rendered node"
                )
            edges.append((_mermaid_id(dependency_id), dependent_id))
    for source_id, dependent_id in sorted(edges):
        lines.append(f"  {source_id} --> {dependent_id}")
    return "\n".join(lines) + "\n"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("manifest", type=Path, help="Path to dbt target/manifest.json")
    args = parser.parse_args()
    manifest = json.loads(args.manifest.read_text(encoding="utf-8"))
    output_path = Path(__file__).resolve().parents[1] / "docs" / "lineage.mmd"
    output_path.write_text(render_lineage(manifest), encoding="utf-8", newline="\n")
    print(f"Wrote {output_path}")


if __name__ == "__main__":
    main()
