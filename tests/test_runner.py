from __future__ import annotations

import sys
from contextlib import nullcontext
from pathlib import Path
from types import SimpleNamespace

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
    args = run.build_dbt_command("build", "--exclude", "resource_type:seed",
                                 project_dir=run.ROOT, profiles_dir=profile_dir, target="postgres")
    assert args[:2] == ["dbt", "build"]
    assert args[2:4] == ["--exclude", "resource_type:seed"]
    assert args[4:6] == ["--project-dir", str(run.ROOT)]
    assert args[6:8] == ["--profiles-dir", str(profile_dir)]
    assert args[8:] == ["--target", "postgres", "--target-path", str(run.ROOT / "target")]
    with pytest.raises(ValueError, match="command must precede"):
        run.build_dbt_command("--project-dir", project_dir=run.ROOT,
                              profiles_dir=profile_dir, target="postgres")


def test_pipeline_python_files_never_put_options_before_dbt_command() -> None:
    import ast

    pipeline_dir = Path(__file__).resolve().parents[1] / "pipeline"
    for path in sorted(pipeline_dir.glob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if not isinstance(node, (ast.List, ast.Tuple)) or not node.elts:
                continue
            first = node.elts[0]
            if not isinstance(first, ast.Constant) or first.value != "dbt":
                continue
            assert len(node.elts) > 1, f"dbt argv missing command in {path}"
            second = node.elts[1]
            assert not (isinstance(second, ast.Constant) and str(second.value).startswith("-")), (
                f"dbt argv places an option before its command in {path}"
            )


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
            options = args[2:args.index("--project-dir")]
            sequence.append("dbt:seed " + " ".join(options))
        elif args[1] == "build":
            options = args[2:args.index("--project-dir")]
            sequence.append("dbt:build " + " ".join(options))
        else:
            sequence.append(f"dbt:{args[1]}")

    monkeypatch.setattr(run, "run_command", record_command)
    monkeypatch.setattr(run, "wait_for_postgres", lambda: None)
    monkeypatch.setattr(run, "baseline_company_seed", lambda _path: nullcontext())
    results = iter([{"facts": (3, "abc")}, {"facts": (3, "abc")}])
    def fingerprints() -> dict[str, tuple[int, str]]:
        sequence.append("fingerprints")
        return next(results)

    monkeypatch.setattr(run, "mart_fingerprints", fingerprints)
    monkeypatch.setattr(run, "verify_scd2_version", lambda: sequence.append("verify_scd2"))
    monkeypatch.setattr("generator.generate.promote_changed_company_version",
                        lambda path: sequence.append("promote_company_change"))
    monkeypatch.setattr(run, "append_changed_company_version",
                        lambda path: sequence.append("append_company_change"))
    monkeypatch.setattr("generator.generate.generate", lambda *args, **kwargs: {"companies": 8})
    monkeypatch.setattr(run.tempfile, "mkdtemp", lambda **kwargs: str(tmp_path / "profile"))
    monkeypatch.setattr("pipeline.vector_search.populate_embeddings", lambda provider: 1)
    run.run_pipeline()
    assert "Idempotency check passed: facts row counts and checksums identical" in capsys.readouterr().out
    assert calls[0][:2] == ["dbt", "seed"]
    assert calls[0][2:calls[0].index("--project-dir")] == ["--full-refresh"]
    assert calls[1][:3] == ["dbt", "source", "freshness"]
    assert calls[1][3] == "--project-dir"
    assert calls[2][:2] == ["dbt", "build"]
    assert calls[2][2:calls[2].index("--project-dir")] == ["--exclude", "resource_type:seed"]
    assert calls[3][:2] == ["dbt", "build"]
    assert calls[3][2:calls[3].index("--project-dir")] == ["--exclude", "resource_type:seed"]
    assert calls[4][:2] == ["dbt", "build"]
    assert calls[4][2:calls[4].index("--project-dir")] == ["--exclude", "resource_type:seed"]
    assert calls[5][:3] == ["dbt", "docs", "generate"]
    assert sequence == [
        "dbt:seed --full-refresh", "dbt:source freshness",
        "dbt:build --exclude resource_type:seed", "promote_company_change",
        "append_company_change", "dbt:build --exclude resource_type:seed",
        "verify_scd2", "fingerprints", "dbt:build --exclude resource_type:seed",
        "fingerprints", "dbt:docs generate",
    ]
    for call in calls:
        assert call[0] == "dbt"
        assert call[1] in {"seed", "source", "build", "docs"}
        assert call.index("--project-dir") < call.index("--profiles-dir") < call.index("--target")


def test_append_changed_company_version_inserts_a_new_raw_row(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    path = tmp_path / "companies.csv"
    path.write_text(
        "company_id,company_name,sector,country_code,parent_group_id,employee_count,annual_revenue,"
        "classification,owner,ingested_at,loaded_at\n"
        "C0001,Example,healthcare,US,G001,100.0,5000000.0,synthetic_public,data_platform,"
        "2026-10-27T00:00:00+00:00,2026-10-27T00:00:00+00:00\n"
        "C0001,Example,business_services,US,G001,100.0,5000000.0,synthetic_public,data_platform,"
        "2026-10-27T15:36:22+00:00,2026-10-27T00:00:00+00:00\n",
        encoding="utf-8",
    )
    monkeypatch.setenv("POSTGRES_PASSWORD", "test-only")
    statements: list[tuple[str, tuple[str | None, ...]]] = []

    class Cursor:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return None

        def execute(self, sql: str, values: tuple[str | None, ...]) -> None:
            statements.append((sql, values))

    class Connection:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return None

        def cursor(self):
            return Cursor()

    monkeypatch.setitem(sys.modules, "psycopg", SimpleNamespace(connect=lambda **_kwargs: Connection()))
    run.append_changed_company_version(path)

    assert len(statements) == 1
    assert "insert into raw.companies" in statements[0][0]
    assert statements[0][1][0:3] == ("C0001", "Example", "business_services")
    assert statements[0][1][5:7] == (100, 5000000)
    assert isinstance(statements[0][1][5], int)
    assert isinstance(statements[0][1][6], int)


def test_pipeline_fails_when_repeat_build_differs(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("POSTGRES_PASSWORD", "test-only")
    monkeypatch.setattr(run, "run_command", lambda *args, **kwargs: None)
    monkeypatch.setattr(run, "wait_for_postgres", lambda: None)
    monkeypatch.setattr(run, "baseline_company_seed", lambda _path: nullcontext())
    monkeypatch.setattr(run, "verify_scd2_version", lambda: (2, 1))
    monkeypatch.setattr("generator.generate.promote_changed_company_version", lambda *args, **kwargs: None)
    monkeypatch.setattr(run, "append_changed_company_version", lambda *args, **kwargs: None)
    results = iter([{"facts": (3, "abc")}, {"facts": (4, "def")}])
    monkeypatch.setattr(run, "mart_fingerprints", lambda: next(results))
    monkeypatch.setattr("generator.generate.generate", lambda *args, **kwargs: {})
    monkeypatch.setattr(run.tempfile, "mkdtemp", lambda **kwargs: str(tmp_path / "profile"))
    with pytest.raises(RuntimeError, match="Idempotency check failed"):
        run.run_pipeline(skip_embeddings=True)

