from __future__ import annotations

import subprocess
from pathlib import Path

import yaml

from pipeline.fact_fingerprints import FACT_COLUMNS, FACT_KEYS

ROOT = Path(__file__).resolve().parents[1]


def test_fingerprint_columns_exist_in_model_contract() -> None:
    contract = yaml.safe_load((ROOT / "models/schema.yml").read_text(encoding="utf-8"))
    models = {model["name"]: {column["name"] for column in model.get("columns", [])}
              for model in contract["models"]}
    for table, columns in FACT_COLUMNS.items():
        assert table in models
        assert set(columns) <= models[table]
        assert FACT_KEYS[table] in columns


def test_tracked_source_and_document_files_have_no_utf8_bom() -> None:
    tracked = subprocess.run(
        ["git", "ls-files", "-z"], cwd=ROOT, check=True, capture_output=True
    ).stdout.split(b"\0")
    suffixes = {".py", ".sql", ".yml", ".md"}
    offenders = []
    for encoded_path in tracked:
        if not encoded_path:
            continue
        relative = Path(encoded_path.decode("utf-8"))
        if relative.suffix.lower() in suffixes:
            path = ROOT / relative
            if path.read_bytes().startswith(b"\xef\xbb\xbf"):
                offenders.append(relative.as_posix())
    assert offenders == [], f"Tracked text files start with a UTF-8 BOM: {offenders}"

