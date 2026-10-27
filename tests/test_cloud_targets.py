from __future__ import annotations

import csv
import re
from datetime import datetime, timezone
from pathlib import Path

import yaml
import pytest
from generator.generate import generate
from pipeline import measure_cloud_run as measure
from pipeline import teardown_cloud
from pipeline.measure_cloud_run import (
    RAW_COLUMN_TYPES,
    build_measurement_record,
    _ensure_changed_company_attribute,
    _identifier,
    _normalize_database_type,
    _snowflake_auth_kwargs,
    _validate_raw_schema,
    _write_timestamp_ntz_csv,
    _assert_raw_company_reloaded,
    load_snowflake_csvs,
    snowflake_profile_target,
)
from pipeline.dbt_commands import build_dbt_command


ROOT = Path(__file__).resolve().parents[1]


def _read(relative_path: str) -> str:
    return (ROOT / relative_path).read_text(encoding="utf-8")


def test_cloud_dependencies_are_separate_and_pinned() -> None:
    requirements = _read("requirements-cloud.txt")
    for package in ("dbt-core", "dbt-postgres", "dbt-snowflake", "dbt-databricks"):
        assert any(line.startswith(package + "==") for line in requirements.splitlines())


def test_profile_template_has_no_secrets_and_supports_key_pair_auth() -> None:
    profile = _read("profiles.yml.example")
    parsed = yaml.safe_load(profile)
    assert "{%" not in profile
    outputs = parsed["ai_ready_data_warehouse"]["outputs"]
    assert {"snowflake", "snowflake_password", "databricks"}.issubset(outputs)
    assert "SNOWFLAKE_PRIVATE_KEY_PATH" in outputs["snowflake"]["private_key_path"]
    assert "SNOWFLAKE_PRIVATE_KEY_PASSPHRASE" in outputs["snowflake"]["private_key_passphrase"]
    assert "SNOWFLAKE_PASSWORD" in outputs["snowflake_password"]["password"]
    for variable in (
        "SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER", "SNOWFLAKE_PRIVATE_KEY_PATH",
        "SNOWFLAKE_PRIVATE_KEY_PASSPHRASE", "SNOWFLAKE_PASSWORD",
        "DATABRICKS_HOST", "DATABRICKS_HTTP_PATH", "DATABRICKS_TOKEN", "DATABRICKS_CATALOG",
    ):
        assert f"env_var('{variable}'" in profile
    assert "private_key_path: " in profile
    assert "private_key_passphrase: " in profile
    assert "password: \"{{ env_var('SNOWFLAKE_PASSWORD', '') }}\"" in profile
    assert "token: " in profile
    assert "-----BEGIN PRIVATE KEY-----" not in profile
    assert "-----BEGIN ENCRYPTED PRIVATE KEY-----" not in profile
    assert "<password>" not in profile.lower() and "example-password" not in profile.lower()
    assert "<token>" not in profile.lower() and "example-token" not in profile.lower()


def test_snowflake_connector_prefers_key_pair_and_uses_password_only_as_fallback(monkeypatch, tmp_path) -> None:
    key_file = tmp_path / "service_key.p8"
    key_file.write_text("test key path only", encoding="utf-8")
    monkeypatch.setenv("SNOWFLAKE_PRIVATE_KEY_PATH", str(key_file))
    monkeypatch.setenv("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE", "key passphrase")
    monkeypatch.setenv("SNOWFLAKE_PASSWORD", "fallback password")
    assert _snowflake_auth_kwargs() == {
        "private_key_file": str(key_file),
        "private_key_file_pwd": "key passphrase",
    }
    assert snowflake_profile_target() == "snowflake"

    monkeypatch.delenv("SNOWFLAKE_PRIVATE_KEY_PATH")
    monkeypatch.delenv("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE")
    assert _snowflake_auth_kwargs() == {"password": "fallback password"}
    assert snowflake_profile_target() == "snowflake_password"


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
    assert "target-path:" not in project
    assert "seeds:" not in project
    assert 'target_path=ROOT / "target" / "cloud"' in _read("pipeline/measure_cloud_run.py")


def test_cloud_seed_properties_and_freshness_use_typed_raw_timestamp_columns() -> None:
    properties = yaml.safe_load(_read("cloud_project/seeds/properties.yml"))
    seed_properties = {seed["name"]: seed["config"]["column_types"] for seed in properties["seeds"]}
    assert set(seed_properties) == set(RAW_COLUMN_TYPES)
    for table, columns in RAW_COLUMN_TYPES.items():
        configured = seed_properties[table]
        assert set(configured) == {name for name, _, _ in columns}
        assert configured == {name: databricks_type for name, _, databricks_type in columns}
        assert configured["ingested_at"] == "TIMESTAMP_NTZ"
        assert configured["loaded_at"] == "TIMESTAMP_NTZ"

    cloud_sources = yaml.safe_load(_read("cloud_project/models/sources.yml"))["sources"][0]
    root_sources = yaml.safe_load(_read("models/sources.yml"))["sources"][0]
    for source in (cloud_sources, root_sources):
        assert "freshness" not in source
        assert "loaded_at_field" not in source
        assert "meta" not in source
        assert source["config"]["freshness"]["warn_after"]["count"] == 31
        assert source["config"]["loaded_at_field"] in {"loaded_at", "ingested_at"}
        assert "owner" in source["config"]["meta"]
    assert "target-path:" not in _read("cloud_project/dbt_project.yml")
    assert "target-path:" not in _read("dbt_project.yml")
    assert _normalize_database_type("TIMESTAMP_NTZ(9)") == "TIMESTAMP_NTZ"


def test_raw_schema_validation_rejects_string_timestamp_before_freshness(monkeypatch) -> None:
    monkeypatch.setenv("DATABRICKS_CATALOG", "demo")

    class Cursor:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return None

        def execute(self, _sql: str) -> None:
            return None

        def fetchall(self):
            rows = [[name, data_type] for name, _, data_type in RAW_COLUMN_TYPES["companies"]]
            return [[name, "STRING" if name == "ingested_at" else data_type]
                    for name, data_type in rows]

    class Connection:
        def cursor(self):
            return Cursor()

        def close(self):
            return None

    monkeypatch.setattr(measure, "_cloud_connect", lambda _target: Connection())
    with pytest.raises(RuntimeError, match=r"Raw schema validation failed for databricks\.companies: INGESTED_AT expected TIMESTAMP_NTZ received STRING"):
        _validate_raw_schema("databricks", tables=("companies",))


def test_snowflake_loader_creates_typed_raw_tables_before_copy(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("SNOWFLAKE_DATABASE", "AI_READY_DEMO")
    statements: list[str] = []

    class Cursor:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return None

        def execute(self, sql: str) -> None:
            statements.append(sql)

        def fetchone(self):
            return (0,)

    class Connection:
        def cursor(self):
            return Cursor()

        def commit(self):
            return None

        def close(self):
            return None

    monkeypatch.setattr(measure, "_snowflake_connect", lambda: Connection())
    generate(tmp_path, days=1, companies_count=8, as_of=datetime.now(timezone.utc).date())
    company_input_counts: list[int] = []
    write_timestamps = measure._write_timestamp_ntz_csv

    def record_company_counts(table: str, source: Path, destination: Path) -> None:
        if table == "companies":
            with source.open("r", newline="", encoding="utf-8") as stream:
                company_input_counts.append(sum(1 for _ in csv.DictReader(stream)))
        write_timestamps(table, source, destination)

    monkeypatch.setattr(measure, "_write_timestamp_ntz_csv", record_company_counts)

    load_snowflake_csvs(tmp_path)
    initial_puts = [sql for sql in statements if sql.startswith("PUT ")]
    load_snowflake_csvs(tmp_path, tables=("companies",), truncate_tables=("companies",),
                        include_future_company_change=True)
    create_statements = [sql for sql in statements if sql.startswith("CREATE OR REPLACE TABLE")]
    put_statements = [sql for sql in statements if sql.startswith("PUT ")]
    assert len(create_statements) == len(RAW_COLUMN_TYPES)
    assert len(put_statements) == len(RAW_COLUMN_TYPES) + 1
    assert len(initial_puts) == len(RAW_COLUMN_TYPES)
    assert company_input_counts == [8, 9]
    assert all(statements.index(create) < statements.index(put)
               for create, put in zip(create_statements, initial_puts, strict=True))
    ddl = " ".join(create_statements)
    for expected_type in ("TIMESTAMP_NTZ", "DATE", "NUMBER(18,2)", "NUMBER(18,4)", "NUMBER(8,4)"):
        assert expected_type in ddl
    assert re.search(r"companies_[0-9a-f]{32}\.csv", put_statements[-1])
    truncate_index = next(index for index, sql in enumerate(statements)
                          if sql == "TRUNCATE TABLE AI_READY_DEMO.AI_READY_DEMO_RAW.COMPANIES")
    assert truncate_index < statements.index(put_statements[-1]) < next(
        index for index, sql in enumerate(statements)
        if index > truncate_index and sql.startswith("COPY INTO AI_READY_DEMO.AI_READY_DEMO_RAW.COMPANIES")
    )


def test_cloud_timestamp_csvs_are_normalized_for_ntz_columns(tmp_path: Path) -> None:
    columns = [name for name, _, _ in RAW_COLUMN_TYPES["companies"]]
    row = {column: "" for column in columns}
    row.update(company_id="C0001", employee_count="117.0", annual_revenue="5372000.0",
               ingested_at="2026-01-02T08:34:05+05:30",
               loaded_at="2026-01-02T08:34:05.123456+05:30")
    source = tmp_path / "source.csv"
    destination = tmp_path / "typed.csv"
    with source.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=columns)
        writer.writeheader()
        writer.writerow(row)

    _write_timestamp_ntz_csv("companies", source, destination)
    with destination.open("r", newline="", encoding="utf-8") as stream:
        converted = next(csv.DictReader(stream))
    assert converted["ingested_at"] == "2026-01-02 03:04:05"
    assert converted["loaded_at"] == "2026-01-02 03:04:05.123456"
    assert converted["employee_count"] == "117"
    assert converted["annual_revenue"] == "5372000"


def test_databricks_initial_company_seed_holds_back_future_change(monkeypatch, tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    generate(data_dir, days=1, companies_count=8, as_of=datetime.now(timezone.utc).date())
    monkeypatch.setattr(measure, "CLOUD_PROJECT", tmp_path / "cloud_project")

    seed_dir = measure._dbt_seed_dir(data_dir)
    with (seed_dir / "companies.csv").open(newline="", encoding="utf-8") as stream:
        assert sum(1 for _ in csv.DictReader(stream)) == 8

    measure._dbt_seed_dir(data_dir, tables=("companies",), include_future_company_change=True)
    with (seed_dir / "companies.csv").open(newline="", encoding="utf-8") as stream:
        rows = list(csv.DictReader(stream))
    assert len(rows) == 9
    changed = max((row for row in rows if row["company_id"] == "C0001"),
                  key=lambda row: row["ingested_at"])
    assert changed["employee_count"].isdigit() and ".0" not in changed["employee_count"]
    assert changed["annual_revenue"].isdigit() and ".0" not in changed["annual_revenue"]


def test_raw_company_reload_asserts_changed_sector_and_newer_timestamp(monkeypatch, capsys) -> None:
    baseline_time = datetime(2026, 1, 1)
    changed_time = datetime(2026, 1, 2)
    monkeypatch.setenv("DATABRICKS_CATALOG", "demo")

    class Cursor:
        def __init__(self, row):
            self.row = row

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return None

        def execute(self, _sql: str) -> None:
            return None

        def fetchone(self):
            return self.row

    class Connection:
        def __init__(self, row):
            self.row = row

        def cursor(self):
            return Cursor(self.row)

        def close(self):
            return None

    monkeypatch.setattr(measure, "_cloud_connect",
                        lambda _target: Connection(("business_services", changed_time)))
    _assert_raw_company_reloaded("databricks", "healthcare", baseline_time,
                                 "business_services", changed_time)
    assert "Changed-company raw reload check passed: C0001" in capsys.readouterr().out

    monkeypatch.setattr(measure, "_cloud_connect", lambda _target: Connection(("healthcare", baseline_time)))
    with pytest.raises(RuntimeError, match="Changed-company reload failed: raw C0001 row"):
        _assert_raw_company_reloaded("databricks", "healthcare", baseline_time,
                                     "business_services", changed_time)


def test_changed_company_attribute_is_adjusted_to_snapshot_baseline(tmp_path: Path) -> None:
    path = tmp_path / "companies.csv"
    path.write_text(
        "company_id,sector,ingested_at\n"
        "C0001,healthcare,2026-01-01T00:00:00+00:00\n"
        "C0001,business_services,2026-01-02T00:00:00+00:00\n"
        "C0002,technology,2026-01-01T00:00:00+00:00\n",
        encoding="utf-8",
    )

    _ensure_changed_company_attribute(path, "business_services")

    with path.open(newline="", encoding="utf-8") as stream:
        changed = [row for row in csv.DictReader(stream) if row["company_id"] == "C0001"][-1]
    assert changed["sector"] == "technology"


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
    assert "snowflake_profile_target()" in teardown
    assert "with `AIRDW_ROLE`, never as `ACCOUNTADMIN`" in runbook


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


@pytest.mark.parametrize(("target", "expected"), [
    ("snowflake", "HASH_AGG(*)"),
    ("databricks", "SUM(CAST(XXHASH64("),
])
def test_cloud_fingerprints_are_computed_in_warehouse(monkeypatch, target: str, expected: str) -> None:
    monkeypatch.setenv("DATABRICKS_CATALOG", "demo")
    statements: list[str] = []

    class Cursor:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return None

        def execute(self, sql: str) -> None:
            statements.append(sql)

        def fetchone(self):
            return (4, 4, 987654)

    class Connection:
        def cursor(self):
            return Cursor()

        def close(self):
            return None

    monkeypatch.setattr(measure, "_cloud_connect", lambda _target: Connection())
    actual = measure._cloud_fact_fingerprints(target)
    assert set(actual) == {"fct_holdings_daily", "fct_transactions"}
    assert all(item == (4, "987654") for item in actual.values())
    assert len(statements) == 2
    assert all(expected in sql for sql in statements)
    assert all("COUNT(DISTINCT" in sql for sql in statements)


@pytest.mark.parametrize(("target", "env_name", "value", "database"), [
    ("databricks", "DATABRICKS_CATALOG", "demo", "DEMO"),
    ("snowflake", "SNOWFLAKE_DATABASE", "AI_READY_DEMO", "AI_READY_DEMO"),
])
def test_cloud_reset_recreates_only_derived_schemas(monkeypatch, capsys, target, env_name, value, database) -> None:
    monkeypatch.setenv(env_name, value)
    statements: list[str] = []

    class Cursor:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return None

        def execute(self, sql: str) -> None:
            statements.append(sql)

    class Connection:
        def cursor(self):
            return Cursor()

        def close(self):
            return None

    monkeypatch.setattr(measure, "_cloud_connect", lambda _target: Connection())
    measure._reset_cloud_derived_schemas(target)
    assert len(statements) == 10
    assert all(sql.startswith((f"DROP SCHEMA IF EXISTS {database}.AI_READY_DEMO_",
                               f"CREATE SCHEMA {database}.AI_READY_DEMO_"))
               for sql in statements)
    assert all("raw" not in sql.lower() for sql in statements)
    assert f"Clean-state reset passed: recreated 5 derived schemas for {target}" in capsys.readouterr().out


def test_cloud_scd2_rejects_more_than_two_versions(monkeypatch) -> None:
    monkeypatch.setenv("DATABRICKS_CATALOG", "demo")
    class Cursor:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return None

        def execute(self, _sql: str) -> None:
            return None

        def fetchone(self):
            return (3, 1)

    class Connection:
        def cursor(self):
            return Cursor()

        def close(self):
            return None

    monkeypatch.setattr(measure, "_cloud_connect", lambda _target: Connection())
    with pytest.raises(RuntimeError, match="versions=3, current=1"):
        measure._verify_cloud_scd2("databricks")


def test_databricks_default_teardown_never_deletes_warehouse(monkeypatch) -> None:
    monkeypatch.setenv("DATABRICKS_CATALOG", "workspace")
    statements: list[str] = []

    class Cursor:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return None

        def execute(self, sql: str) -> None:
            statements.append(sql)

    class Connection:
        def cursor(self):
            return Cursor()

        def close(self):
            return None

    monkeypatch.setattr(measure, "_databricks_connect", lambda: Connection())
    teardown_cloud.teardown_databricks()
    assert len(statements) == len(teardown_cloud.DEMO_SCHEMAS)
    assert all(sql.startswith("DROP SCHEMA IF EXISTS workspace.AI_READY_DEMO_") for sql in statements)
    assert not any("WAREHOUSE" in sql.upper() for sql in statements)


def test_cloud_measurement_runs_load_build_scd2_and_idempotency_in_order(
    monkeypatch, tmp_path: Path, capsys
) -> None:
    monkeypatch.setenv("DATABRICKS_TOKEN", "test-token")
    monkeypatch.setenv("DATABRICKS_HOST", "workspace.example")
    monkeypatch.setenv("DATABRICKS_HTTP_PATH", "/sql/warehouse")
    monkeypatch.setenv("DATABRICKS_CATALOG", "demo")
    monkeypatch.setenv("DATABRICKS_WAREHOUSE_ID", "warehouse-id")
    monkeypatch.setattr(measure, "ROOT", tmp_path)
    monkeypatch.setattr(measure, "CLOUD_PROJECT", tmp_path / "cloud_project")

    events: list[str] = []
    commands: list[list[str]] = []
    generated_counts = {table: 1 for table in measure.SNOWFLAKE_TABLES}

    def generate(data_dir: Path, **kwargs: object) -> dict[str, int]:
        data_dir.mkdir(parents=True, exist_ok=True)
        for table, columns in RAW_COLUMN_TYPES.items():
            header = ",".join(name for name, _, _ in columns)
            (data_dir / f"{table}.csv").write_text(header + "\n", encoding="utf-8")
        events.append("generate")
        return generated_counts.copy()

    monkeypatch.setattr("generator.generate.generate", generate)
    monkeypatch.setattr("generator.generate.promote_changed_company_version",
                        lambda path: events.append("promote_company_change"))
    def seed_dir(data_dir: Path, tables=measure.SNOWFLAKE_TABLES,
                 include_future_company_change: bool = False) -> Path:
        phase = "future" if include_future_company_change else "baseline"
        events.append("load:" + (",".join(tables)) + ":" + phase)
        return data_dir

    monkeypatch.setattr(measure, "_dbt_seed_dir", seed_dir)

    def dbt(command: str, project_dir: Path, profiles_dir: Path, target: str, *args: str,
            executable: str = "dbt") -> None:
        commands.append(build_dbt_command(command, *args, project_dir=project_dir,
                                           profiles_dir=profiles_dir, target=target,
                                           executable=executable))
        events.append("dbt:" + " ".join([command, *args]))

    monkeypatch.setattr(measure, "_dbt_command", dbt)
    monkeypatch.setattr(measure, "_reset_cloud_derived_schemas",
                        lambda target: events.append("reset:" + target))
    monkeypatch.setattr(measure, "_validate_raw_schema",
                        lambda target, tables=measure.SNOWFLAKE_TABLES: events.append(
                            "validate:" + (",".join(tables) if tables != measure.SNOWFLAKE_TABLES else "all")))
    fingerprints = iter([
        {"fct_holdings_daily": (2, "abc"), "fct_transactions": (1, "def")},
        {"fct_holdings_daily": (2, "abc"), "fct_transactions": (1, "def")},
    ])
    monkeypatch.setattr(measure, "_cloud_fact_fingerprints",
                        lambda target: (events.append("fingerprint") or next(fingerprints)))
    baseline_ingested_at = datetime(2026, 1, 1)
    changed_ingested_at = datetime(2026, 1, 2)
    monkeypatch.setattr(measure, "_current_snapshot_company",
                        lambda target: (events.append("snapshot_baseline") or ("healthcare", baseline_ingested_at)))
    monkeypatch.setattr(measure, "_ensure_changed_company_attribute",
                        lambda *args: events.append("ensure_changed_attribute"))
    monkeypatch.setattr(measure, "_changed_company_values",
                        lambda *args: (events.append("changed_values") or ("business_services", changed_ingested_at)))
    monkeypatch.setattr(measure, "_truncate_databricks_raw_table",
                        lambda table: events.append("truncate:" + table))
    monkeypatch.setattr(measure, "_assert_raw_company_reloaded",
                        lambda *args: events.append("assert_raw_changed"))
    def verify_scd2(target: str) -> None:
        events.append("verify_scd2")
        print("SCD2 version check passed: C0001 versions=2, current=1")

    monkeypatch.setattr(measure, "_verify_cloud_scd2", verify_scd2)
    monkeypatch.setattr(measure, "_databricks_row_counts",
                        lambda: (events.append("measure_rows") or {"marts.dim_company": 1}))
    monkeypatch.setattr(measure, "_databricks_dbus", lambda start, end: (0.1, "available"))

    result_path = measure.run_measurement(target="databricks", days=30, companies=400, seed=1,
                                          profiles_dir=tmp_path / "profiles")
    assert result_path.is_file()
    output = capsys.readouterr().out
    assert "SCD2 version check passed: C0001 versions=2, current=1" in output
    assert "Idempotency check passed: fct_holdings_daily, fct_transactions row counts and checksums identical" in output
    assert events == [
        "reset:databricks", "generate", "load:" + ",".join(measure.SNOWFLAKE_TABLES) + ":baseline",
        "dbt:seed --full-refresh", "validate:all", "dbt:source freshness",
        "dbt:build --exclude resource_type:seed", "snapshot_baseline", "ensure_changed_attribute",
        "promote_company_change",
        "changed_values", "load:companies:future", "truncate:companies", "dbt:seed --select companies",
        "validate:companies", "assert_raw_changed",
        "dbt:build --exclude resource_type:seed", "verify_scd2", "fingerprint",
        "dbt:build --exclude resource_type:seed", "fingerprint", "measure_rows",
    ]
    for command in commands:
        assert command[0] == "dbt" and not command[1].startswith("-")
        assert command.index("--project-dir") < command.index("--profiles-dir") < command.index("--target")
