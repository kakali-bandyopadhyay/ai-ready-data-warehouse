from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from pipeline.measure_cloud_run import build_measurement_record, _identifier


ROOT = Path(__file__).resolve().parents[1]


def _read(relative_path: str) -> str:
    return (ROOT / relative_path).read_text(encoding="utf-8")


def test_cloud_dependencies_are_separate_and_pinned() -> None:
    requirements = _read("requirements-cloud.txt")
    for package in ("dbt-core", "dbt-postgres", "dbt-snowflake", "dbt-databricks"):
        assert any(line.startswith(package + "==") for line in requirements.splitlines())


def test_profile_template_uses_environment_credentials_only() -> None:
    profile = _read("profiles.yml.example")
    for variable in (
        "SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER", "SNOWFLAKE_PASSWORD",
        "DATABRICKS_HOST", "DATABRICKS_HTTP_PATH", "DATABRICKS_TOKEN", "DATABRICKS_CATALOG",
    ):
        assert f"env_var('{variable}'" in profile
    assert "password: " in profile
    assert "token: " in profile
    assert "<password>" not in profile.lower()
    assert "<token>" not in profile.lower()


def test_platform_models_have_expected_storage_and_clustering() -> None:
    holdings = _read("cloud_project/models/marts/fct_holdings_daily.sql")
    chunks = _read("cloud_project/models/ai_ready/ai_text_chunks.sql")
    project = _read("cloud_project/dbt_project.yml")
    assert "cluster_by=['date_key', 'company_sk']" in holdings
    assert "liquid_clustered_by=['date_key', 'company_sk']" in holdings
    assert "file_format='delta'" in holdings
    assert "databricks_tags=" in holdings
    assert "vector(float, 8)" in chunks
    assert "array<float>" in chunks
    assert "../target/cloud" in project


def test_cloud_company_profile_uses_current_snapshot_and_latest_company_kpi() -> None:
    profile = _read("cloud_project/models/ai_ready/ai_company_profile.sql")
    kpis = _read("cloud_project/models/staging/stg_company_kpis.sql")
    assert "ref('company_history')" in profile
    assert "c.dbt_valid_to is null" in profile
    assert "on c.company_id = k.company_id" in profile
    assert "ref('stg_companies')" not in profile
    assert "config(materialized='table')" in kpis
    assert "partition by company_id" in kpis
    assert "ref('stg_companies')" not in kpis


def test_snowflake_uses_named_stage_copy_and_bounded_warehouse() -> None:
    loader = _read("pipeline/measure_cloud_run.py")
    setup = _read("cloud_project/ops/snowflake_setup.sql")
    assert "CREATE STAGE IF NOT EXISTS" in loader
    assert "PUT '" in loader
    assert "COPY INTO" in loader
    assert "WAREHOUSE_SIZE = XSMALL" in setup
    assert "AUTO_SUSPEND = 60" in setup
    assert "CREDIT_QUOTA = 10" in setup
    assert "loaded_at_field: ingested_at" in _read("cloud_project/models/sources.yml")


def test_snowflake_demo_resources_are_owned_and_torn_down_as_airdw_role() -> None:
    setup = _read("cloud_project/ops/snowflake_setup.sql")
    runbook = _read("docs/runbooks/SNOWFLAKE.md")
    teardown = _read("pipeline/teardown_cloud.py")
    for resource in (
        "ON DATABASE AI_READY_DEMO", "ON SCHEMA AI_READY_DEMO.AI_READY_DEMO_RAW",
        "ON WAREHOUSE AI_READY_XS_WH", "ON RESOURCE MONITOR AI_READY_DEMO_MONITOR",
    ):
        assert f"GRANT OWNERSHIP {resource}" in setup
        assert "TO ROLE AIRDW_ROLE COPY CURRENT GRANTS" in setup
    assert "SNOWFLAKE_ROLE = \"AIRDW_ROLE\"" in teardown
    assert "SELECT CURRENT_ROLE()" in teardown
    assert "dbt and teardown as `AIRDW_ROLE`, never as `ACCOUNTADMIN`" in runbook


def test_private_brief_and_generated_cloud_seed_copies_are_ignored() -> None:
    ignore = _read(".gitignore")
    assert "_brief/" in ignore
    assert "/cloud_project/seeds/*.csv" in ignore
    assert ".venv-cloud/" in ignore


def test_measurement_record_is_dated_labelled_and_keeps_cloud_usage() -> None:
    start = datetime(2026, 10, 27, tzinfo=timezone.utc)
    record = build_measurement_record(
        target="databricks", days=30, companies=400,
        generated_counts={"holdings": 1823, "funds": 8},
        load_counts={"holdings": 1823, "funds": 8},
        generation_seconds=1.0, load_seconds=2.0, snapshot_seconds=3.0,
        build_seconds=4.0, credits=None, dbus=0.25,
        billing_status="available", started_utc=start, build_end_utc=start,
        measured_at=start, row_counts={"marts.fct_holdings_daily": 1700},
    )
    assert record["synthetic_data"] is True
    assert "30 days" in record["data_size"]["label"]
    assert "400 companies" in record["data_size"]["label"]
    assert record["timings_seconds"]["load"] == 2.0
    assert record["timings_seconds"]["build"] == 4.0
    assert record["platform_usage"]["databricks_dbus"] == 0.25
    assert record["mart_and_ai_ready_row_counts"]["marts.fct_holdings_daily"] == 1700


def test_identifier_guard_rejects_sql_injection() -> None:
    assert _identifier("ai_ready_demo") == "AI_READY_DEMO"
    try:
        _identifier("AI_READY_DEMO; DROP DATABASE OTHER")
    except ValueError:
        pass
    else:
        raise AssertionError("unsafe SQL identifier should fail validation")
