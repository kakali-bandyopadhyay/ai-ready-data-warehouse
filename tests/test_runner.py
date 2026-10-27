from __future__ import annotations

from pathlib import Path

import pytest

from pipeline import run


def test_profile_created_in_requested_external_directory(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("POSTGRES_PASSWORD", "test-only")
    profile = run.create_profile(tmp_path / "outside", "postgres")
    assert profile.name == "profiles.yml"
    assert "type: postgres" in profile.read_text(encoding="utf-8")
    assert "threads: 1" in profile.read_text(encoding="utf-8")


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


def test_dbt_argv_starts_with_executable_and_command() -> None:
    profile_dir = Path("temporary-profile")
    args = run.build_dbt_command("build", profile_dir, "postgres")
    assert args[:2] == ["dbt", "build"]
    assert args[2:4] == ["--project-dir", str(run.ROOT)]
    assert args[4:6] == ["--profiles-dir", str(profile_dir)]
    assert args[6:] == ["--target", "postgres"]


def test_pipeline_sequences_scd2_promotion_then_checks_idempotency(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setenv("POSTGRES_PASSWORD", "test-only")
    calls: list[list[str]] = []
    sequence: list[str] = []

    def record_command(args: list[str], **kwargs: object) -> None:
        calls.append(args)
        if args[1] in {"source", "docs"}:
            sequence.append(f"dbt:{args[1]} {args[2]}")
        elif args[1] == "seed":
            options = args[args.index("--target") + 2:]
            sequence.append("dbt:seed " + " ".join(options))
        elif args[1] == "build":
            options = args[args.index("--target") + 2:]
            sequence.append("dbt:build " + " ".join(options))
        else:
            sequence.append(f"dbt:{args[1]}")

    monkeypatch.setattr(run, "run_command", record_command)
    monkeypatch.setattr(run, "wait_for_postgres", lambda: None)
    results = iter([{"facts": (3, "abc")}, {"facts": (3, "abc")}])
    def fingerprints() -> dict[str, tuple[int, str]]:
        sequence.append("fingerprints")
        return next(results)

    monkeypatch.setattr(run, "mart_fingerprints", fingerprints)
    monkeypatch.setattr(run, "verify_scd2_version", lambda: sequence.append("verify_scd2"))
    monkeypatch.setattr("generator.generate.promote_changed_company_version",
                        lambda path: sequence.append("promote_company_change"))
    monkeypatch.setattr("generator.generate.generate", lambda *args, **kwargs: {"companies": 8})
    monkeypatch.setattr(run.tempfile, "mkdtemp", lambda **kwargs: str(tmp_path / "profile"))
    monkeypatch.setattr("pipeline.vector_search.populate_embeddings", lambda provider: 1)
    run.run_pipeline()
    assert "Idempotency check passed: facts row counts and checksums identical" in capsys.readouterr().out
    assert calls[0][:2] == ["dbt", "seed"]
    assert calls[0][-1] == "--full-refresh"
    assert calls[1][:3] == ["dbt", "source", "freshness"]
    assert calls[1][3] == "--project-dir"
    assert calls[2][:2] == ["dbt", "build"]
    assert calls[2][-2:] == ["--exclude", "resource_type:seed"]
    assert calls[3][:2] == ["dbt", "seed"]
    assert calls[3][-2:] == ["--select", "companies"]
    assert "--full-refresh" not in calls[3]
    assert calls[4][:2] == ["dbt", "build"]
    assert calls[4][-2:] == ["--exclude", "resource_type:seed"]
    assert calls[5][:2] == ["dbt", "build"]
    assert calls[5][-2:] == ["--exclude", "resource_type:seed"]
    assert calls[6][:3] == ["dbt", "docs", "generate"]
    assert sequence == [
        "dbt:seed --full-refresh", "dbt:source freshness",
        "dbt:build --exclude resource_type:seed", "promote_company_change",
        "dbt:seed --select companies", "dbt:build --exclude resource_type:seed",
        "verify_scd2", "fingerprints", "dbt:build --exclude resource_type:seed",
        "fingerprints", "dbt:docs generate",
    ]
    for call in calls:
        assert call[0] == "dbt"
        assert call[1] in {"seed", "source", "build", "docs"}
        assert call.index("--project-dir") < call.index("--profiles-dir") < call.index("--target")


def test_pipeline_fails_when_repeat_build_differs(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("POSTGRES_PASSWORD", "test-only")
    monkeypatch.setattr(run, "run_command", lambda *args, **kwargs: None)
    monkeypatch.setattr(run, "wait_for_postgres", lambda: None)
    monkeypatch.setattr(run, "verify_scd2_version", lambda: (2, 1))
    monkeypatch.setattr("generator.generate.promote_changed_company_version", lambda *args, **kwargs: None)
    results = iter([{"facts": (3, "abc")}, {"facts": (4, "def")}])
    monkeypatch.setattr(run, "mart_fingerprints", lambda: next(results))
    monkeypatch.setattr("generator.generate.generate", lambda *args, **kwargs: {})
    monkeypatch.setattr(run.tempfile, "mkdtemp", lambda **kwargs: str(tmp_path / "profile"))
    with pytest.raises(RuntimeError, match="Idempotency check failed"):
        run.run_pipeline(skip_embeddings=True)

