"""Shared dbt command construction for local and cloud runners."""
from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path


def build_dbt_command(command: str | Sequence[str], *args: str, project_dir: Path,
                      profiles_dir: Path, target: str, target_path: Path | None = None,
                      executable: str = "dbt") -> list[str]:
    """Return a consistently ordered dbt argv with an explicit artifact directory."""
    command_parts = command.split() if isinstance(command, str) else list(command)
    if not command_parts or any(not part for part in command_parts):
        raise ValueError("A dbt command is required")
    if command_parts[0].startswith("-"):
        raise ValueError("A dbt command must precede all command-line options")
    target_path = target_path or project_dir / "target"
    return [executable, *command_parts, *args,
            "--project-dir", str(project_dir),
            "--profiles-dir", str(profiles_dir),
            "--target", target,
            "--target-path", str(target_path)]
