from __future__ import annotations

import re
from pathlib import Path


def test_tracked_content_uses_only_firm_neutral_terms() -> None:
    root = Path(__file__).resolve().parents[1]
    tokens = ["gold" + "man", "morgan " + "stanley", "jp" + "morgan", "c" + "iti",
              "h" + "sbc", "bar" + "clays", "black" + "rock", "black" + "stone",
              "delo" + "itte", "p" + "wc", "inter" + "view"]
    pattern = "(?i)(?:" + "|".join(r"\b" + re.escape(term) + r"\b" for term in tokens) + ")"
    forbidden = re.compile(pattern)
    excluded = {".git", "_brief", ".venv", ".venv-cloud", "target", "dbt_packages", "logs", "data"}
    offenders = []
    for path in root.rglob("*"):
        if not path.is_file() or any(part in excluded for part in path.parts):
            continue
        try:
            text = path.read_text(encoding="utf-8")
        except (UnicodeError, OSError):
            continue
        if forbidden.search(text):
            offenders.append(str(path.relative_to(root)))
    assert not offenders, f"Blocklisted text found in: {offenders}; patterns={tokens}"
