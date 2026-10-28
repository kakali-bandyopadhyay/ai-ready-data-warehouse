from __future__ import annotations

import json
from pathlib import Path
import re

from scripts.lineage_to_mermaid import render_lineage

ROOT = Path(__file__).resolve().parents[1]


def test_manifest_models_sources_and_snapshots_appear_in_mermaid() -> None:
    fixture = ROOT / "tests" / "fixtures" / "lineage_manifest.json"
    manifest = json.loads(fixture.read_text(encoding="utf-8"))

    output = render_lineage(manifest)

    for unique_id, node in manifest["nodes"].items():
        if node["resource_type"] == "model":
            assert node["name"] in output, unique_id
    assert "raw.accounts" in output
    assert "account_history" in output
    assert "flowchart LR" in output
    expected_edges = sum(
        len(node.get("depends_on", {}).get("nodes", []))
        for node in manifest["nodes"].values()
        if node["resource_type"] in {"model", "snapshot"}
    )
    edge_lines = [line for line in output.splitlines() if " --> " in line]
    assert len(edge_lines) == expected_edges

    declared_nodes = {
        match.group(1)
        for line in output.splitlines()
        if (match := re.match(r"\s+([A-Za-z][A-Za-z0-9_]*)\[\"", line))
    }
    for edge in edge_lines:
        source_id, target_id = edge.strip().split(" --> ")
        assert source_id in declared_nodes
        assert target_id in declared_nodes

    layer_order = [
        line.split("[", 1)[0].split()[-1]
        for line in output.splitlines()
        if line.strip().startswith("subgraph ")
    ]
    assert layer_order == ["sources", "staging", "snapshots", "intermediate", "marts", "ai_ready"]
