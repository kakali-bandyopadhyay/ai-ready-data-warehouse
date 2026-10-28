from __future__ import annotations

import json
from pathlib import Path

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
    assert "flowchart TB" in output
    assert " --> " in output
