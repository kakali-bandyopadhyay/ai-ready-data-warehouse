from __future__ import annotations

from pathlib import Path

import pytest

from pipeline import run


def test_profile_created_in_requested_external_directory(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("POSTGRES_PASSWORD", "test-only")
    profile = run.create_profile(tmp_path / "outside", "postgres")
    assert profile.name == "profiles.yml"
    assert "type: postgres" in profile.read_text(encoding="utf-8")


def test_profile_reads_credentials_from_environment(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("POSTGRES_USER", "local_user")
    monkeypatch.setenv("POSTGRES_PASSWORD", "local_password")
    profile = run.create_profile(tmp_path, "postgres").read_text(encoding="utf-8")
    assert 'user: "local_user"' in profile
    assert 'password: "local_password"' in profile


def test_profile_has_no_cloud_secrets_or_repository_path(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("POSTGRES_PASSWORD", "test-only")
    profile = run.create_profile(tmp_path, "postgres").read_text(encoding="utf-8")
    assert "C:/Projects" not in profile and "C:\\Projects" not in profile
    assert "account:" not in profile


def test_only_phase_one_target_is_accepted() -> None:
    with pytest.raises(ValueError, match="only --target postgres"):
        run.run_pipeline(target="snowflake")


def test_pipeline_runs_seed_snapshot_build_twice_and_checks_idempotency(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("POSTGRES_PASSWORD", "test-only")
    calls: list[list[str]] = []
    monkeypatch.setattr(run, "run_command", lambda args, **kwargs: calls.append(args))
    monkeypatch.setattr(run, "wait_for_postgres", lambda: None)
    results = iter([{"facts": (3, "abc")}, {"facts": (3, "abc")}])
    monkeypatch.setattr(run, "mart_fingerprints", lambda: next(results))
    monkeypatch.setattr("generator.generate.generate", lambda *args, **kwargs: {"companies": 8})
    monkeypatch.setattr(run.tempfile, "mkdtemp", lambda **kwargs: str(tmp_path / "profile"))
    monkeypatch.setattr("pipeline.vector_search.populate_embeddings", lambda provider: 1)
    run.run_pipeline()
    assert calls[0][-2:] == ["seed", "--full-refresh"]
    assert calls[1][-2:] == ["source", "freshness"]
    assert calls[2][-1] == "snapshot"
    assert sum(call[-1] == "build" for call in calls) == 2


def test_pipeline_fails_when_repeat_build_differs(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("POSTGRES_PASSWORD", "test-only")
    monkeypatch.setattr(run, "run_command", lambda *args, **kwargs: None)
    monkeypatch.setattr(run, "wait_for_postgres", lambda: None)
    results = iter([{"facts": (3, "abc")}, {"facts": (4, "def")}])
    monkeypatch.setattr(run, "mart_fingerprints", lambda: next(results))
    monkeypatch.setattr("generator.generate.generate", lambda *args, **kwargs: {})
    monkeypatch.setattr(run.tempfile, "mkdtemp", lambda **kwargs: str(tmp_path / "profile"))
    with pytest.raises(RuntimeError, match="Idempotency check failed"):
        run.run_pipeline(skip_embeddings=True)

