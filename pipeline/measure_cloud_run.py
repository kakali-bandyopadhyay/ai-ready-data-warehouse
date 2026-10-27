"""Load/build a cloud target and write a dated, synthetic-data measurement record.

Cloud billing tables can be delayed or permission-gated. In that case the JSON
records null credit/DBU values with an accounting status instead of inventing a
number. This module never commits, pushes, or creates a real cloud result here.
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4

from generator.generate import normalize_integer_columns, write_baseline_company_seed

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pipeline.dbt_commands import build_dbt_command

CLOUD_PROJECT = ROOT / "cloud_project"
TABLES = (
    ("ai_ready_demo_marts", "dim_fund"),
    ("ai_ready_demo_marts", "dim_company"),
    ("ai_ready_demo_marts", "dim_date"),
    ("ai_ready_demo_marts", "fct_holdings_daily"),
    ("ai_ready_demo_marts", "fct_transactions"),
    ("ai_ready_demo_ai_ready", "ai_company_profile"),
    ("ai_ready_demo_ai_ready", "ai_text_chunks"),
)
SNOWFLAKE_TABLES = ("funds", "companies", "holdings", "transactions", "valuations", "company_kpis", "company_notes")
IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
# (column, Snowflake type, Databricks type); shared by loader validation and
# Snowflake DDL. Databricks seed column_types are declared in seeds/properties.yml.
RAW_COLUMN_TYPES: dict[str, tuple[tuple[str, str, str], ...]] = {
    "funds": (
        ("fund_id", "VARCHAR", "STRING"), ("fund_name", "VARCHAR", "STRING"),
        ("base_currency", "VARCHAR", "STRING"), ("vintage_year", "NUMBER(10,0)", "INT"),
        ("fund_nav", "NUMBER(18,2)", "DECIMAL(18,2)"), ("ingested_at", "TIMESTAMP_NTZ", "TIMESTAMP_NTZ"),
        ("loaded_at", "TIMESTAMP_NTZ", "TIMESTAMP_NTZ"),
    ),
    "companies": (
        ("company_id", "VARCHAR", "STRING"), ("company_name", "VARCHAR", "STRING"),
        ("sector", "VARCHAR", "STRING"), ("country_code", "VARCHAR", "STRING"),
        ("parent_group_id", "VARCHAR", "STRING"), ("employee_count", "NUMBER(10,0)", "INT"),
        ("annual_revenue", "NUMBER(18,2)", "DECIMAL(18,2)"),
        ("classification", "VARCHAR", "STRING"), ("owner", "VARCHAR", "STRING"),
        ("ingested_at", "TIMESTAMP_NTZ", "TIMESTAMP_NTZ"), ("loaded_at", "TIMESTAMP_NTZ", "TIMESTAMP_NTZ"),
    ),
    "holdings": (
        ("holding_id", "VARCHAR", "STRING"), ("fund_id", "VARCHAR", "STRING"),
        ("company_id", "VARCHAR", "STRING"), ("as_of_date", "DATE", "DATE"),
        ("units", "NUMBER(18,4)", "DECIMAL(18,4)"), ("price", "NUMBER(18,4)", "DECIMAL(18,4)"),
        ("market_value", "NUMBER(18,2)", "DECIMAL(18,2)"),
        ("ingested_at", "TIMESTAMP_NTZ", "TIMESTAMP_NTZ"), ("loaded_at", "TIMESTAMP_NTZ", "TIMESTAMP_NTZ"),
    ),
    "transactions": (
        ("transaction_id", "VARCHAR", "STRING"), ("fund_id", "VARCHAR", "STRING"),
        ("company_id", "VARCHAR", "STRING"), ("transaction_date", "DATE", "DATE"),
        ("transaction_type", "VARCHAR", "STRING"), ("amount", "NUMBER(18,2)", "DECIMAL(18,2)"),
        ("currency", "VARCHAR", "STRING"), ("ingested_at", "TIMESTAMP_NTZ", "TIMESTAMP_NTZ"),
        ("batch_id", "VARCHAR", "STRING"), ("loaded_at", "TIMESTAMP_NTZ", "TIMESTAMP_NTZ"),
    ),
    "valuations": (
        ("valuation_id", "VARCHAR", "STRING"), ("company_id", "VARCHAR", "STRING"),
        ("valuation_date", "DATE", "DATE"), ("enterprise_value", "NUMBER(18,2)", "DECIMAL(18,2)"),
        ("ingested_at", "TIMESTAMP_NTZ", "TIMESTAMP_NTZ"), ("loaded_at", "TIMESTAMP_NTZ", "TIMESTAMP_NTZ"),
    ),
    "company_kpis": (
        ("kpi_id", "VARCHAR", "STRING"), ("company_id", "VARCHAR", "STRING"),
        ("as_of_date", "DATE", "DATE"), ("revenue_growth_pct", "NUMBER(8,4)", "DECIMAL(8,4)"),
        ("ebitda_margin_pct", "NUMBER(8,4)", "DECIMAL(8,4)"),
        ("ingested_at", "TIMESTAMP_NTZ", "TIMESTAMP_NTZ"), ("loaded_at", "TIMESTAMP_NTZ", "TIMESTAMP_NTZ"),
    ),
    "company_notes": (
        ("note_id", "VARCHAR", "STRING"), ("company_id", "VARCHAR", "STRING"),
        ("note_text", "VARCHAR", "STRING"), ("source_name", "VARCHAR", "STRING"),
        ("as_of_date", "DATE", "DATE"), ("owner", "VARCHAR", "STRING"),
        ("classification", "VARCHAR", "STRING"),
        ("ingested_at", "TIMESTAMP_NTZ", "TIMESTAMP_NTZ"), ("loaded_at", "TIMESTAMP_NTZ", "TIMESTAMP_NTZ"),
    ),
}


def _identifier(value: str, label: str = "SQL identifier") -> str:
    if not IDENTIFIER.fullmatch(value):
        raise ValueError(f"Invalid {label}: {value!r}")
    return value.upper()


def _validate_csv_columns(table: str, columns: list[str]) -> None:
    expected = [name for name, _, _ in RAW_COLUMN_TYPES[table]]
    actual = [name.strip().lower() for name in columns]
    if actual != expected:
        raise RuntimeError(
            f"Raw schema validation failed for {table}: CSV columns expected {expected}, received {actual}"
        )


def _write_timestamp_ntz_csv(table: str, source: Path, destination: Path) -> None:
    """Write CSV timestamps as UTC-naive values for TIMESTAMP_NTZ raw columns."""
    timestamp_columns = {
        name for name, _, databricks_type in RAW_COLUMN_TYPES[table]
        if databricks_type == "TIMESTAMP_NTZ"
    }
    with source.open("r", newline="", encoding="utf-8") as input_stream, \
            destination.open("w", newline="", encoding="utf-8") as output_stream:
        reader = csv.DictReader(input_stream)
        if not reader.fieldnames:
            raise RuntimeError(f"Raw schema validation failed for {table}: CSV has no header")
        _validate_csv_columns(table, reader.fieldnames)
        writer = csv.DictWriter(output_stream, fieldnames=reader.fieldnames)
        writer.writeheader()
        for row in reader:
            row = normalize_integer_columns(table, row)
            for column in timestamp_columns:
                value = row.get(column, "")
                if value:
                    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
                    if parsed.tzinfo is None:
                        parsed = parsed.replace(tzinfo=timezone.utc)
                    parsed = parsed.astimezone(timezone.utc).replace(tzinfo=None)
                    row[column] = parsed.isoformat(sep=" ")
            writer.writerow(row)


def _normalize_database_type(value: object) -> str:
    normalized = re.sub(r"\s+", "", str(value)).upper()
    if normalized.startswith("VARCHAR"):
        return "VARCHAR"
    if normalized.startswith("STRING"):
        return "STRING"
    if normalized.startswith("TIMESTAMP_NTZ"):
        return "TIMESTAMP_NTZ"
    return normalized


def _validate_raw_schema(target: str, tables: tuple[str, ...] = SNOWFLAKE_TABLES) -> None:
    """Stop before freshness/build if a raw table does not have its typed schema."""
    connection = _cloud_connect(target)
    try:
        with connection.cursor() as cursor:
            for table in tables:
                relation = _cloud_relation(target, "ai_ready_demo_raw", table)
                try:
                    cursor.execute(f"DESCRIBE TABLE {relation}")
                    described_columns = cursor.fetchall()
                except Exception as exc:
                    detail = re.sub(r"\s+", " ", str(exc)).strip()
                    raise RuntimeError(
                        f"Raw schema validation failed for {target}.{table}: unable to inspect columns ({detail})"
                    ) from None
                actual: dict[str, str] = {}
                for row in described_columns:
                    if len(row) < 2 or row[0] is None:
                        continue
                    name = str(row[0]).strip()
                    if not name or name.startswith("#"):
                        continue
                    actual[name.upper()] = _normalize_database_type(row[1])

                type_index = 0 if target == "snowflake" else 1
                expected = {
                    name.upper(): _normalize_database_type(types[type_index])
                    for name, *types in RAW_COLUMN_TYPES[table]
                }
                mismatches = [
                    f"{name} expected {expected[name]} received {actual.get(name, 'MISSING')}"
                    for name in expected if actual.get(name) != expected[name]
                ]
                extras = sorted(set(actual) - set(expected))
                if mismatches or extras:
                    detail = "; ".join(mismatches)
                    if extras:
                        detail += ("; " if detail else "") + "unexpected columns " + ", ".join(extras)
                    raise RuntimeError(f"Raw schema validation failed for {target}.{table}: {detail}")
    finally:
        connection.close()


def snowflake_profile_target() -> str:
    """Select the matching dbt output and connector auth mode from the key path."""
    return "snowflake" if os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH", "").strip() else "snowflake_password"


def _snowflake_auth_kwargs() -> dict[str, str]:
    """Prefer key-pair auth; use password only when no private key path is set."""
    key_path = os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH", "").strip()
    if snowflake_profile_target() == "snowflake":
        key_file = Path(key_path).expanduser()
        if not key_file.is_file():
            raise RuntimeError(f"Snowflake private key file does not exist: {key_file}")
        auth = {"private_key_file": str(key_file)}
        passphrase = os.getenv("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE", "")
        if passphrase:
            auth["private_key_file_pwd"] = passphrase
        return auth

    password = os.getenv("SNOWFLAKE_PASSWORD", "")
    if password:
        return {"password": password}
    raise RuntimeError(
        "Set SNOWFLAKE_PRIVATE_KEY_PATH (recommended) or SNOWFLAKE_PASSWORD for Snowflake authentication"
    )


def _snowflake_connect(*, include_warehouse: bool = True, include_database: bool = True,
                       profile_target: str | None = None):
    try:
        import snowflake.connector  # type: ignore
    except ImportError as exc:
        raise RuntimeError("Install requirements-cloud.txt to use the Snowflake target") from exc
    required = ("SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER")
    missing = [key for key in required if not os.getenv(key)]
    if missing:
        raise RuntimeError("Missing environment variables: " + ", ".join(missing))
    selected_target = snowflake_profile_target()
    if profile_target is not None and profile_target != selected_target:
        raise ValueError(f"Selected Snowflake profile target {profile_target!r} does not match {selected_target!r}")
    kwargs: dict[str, Any] = {
        "account": os.environ["SNOWFLAKE_ACCOUNT"],
        "user": os.environ["SNOWFLAKE_USER"],
        "role": os.getenv("SNOWFLAKE_ROLE") or None,
    }
    kwargs.update(_snowflake_auth_kwargs())
    if include_warehouse:
        kwargs["warehouse"] = os.getenv("SNOWFLAKE_WAREHOUSE", "AI_READY_XS_WH")
    if include_database and os.getenv("SNOWFLAKE_DATABASE"):
        kwargs["database"] = _identifier(os.environ["SNOWFLAKE_DATABASE"], "database")
    return snowflake.connector.connect(**kwargs)


def load_snowflake_csvs(data_dir: Path, tables: tuple[str, ...] = SNOWFLAKE_TABLES,
                       truncate_tables: tuple[str, ...] = (),
                       include_future_company_change: bool = False) -> dict[str, int]:
    """PUT generated CSVs to an internal stage and COPY them into raw tables."""
    database = _identifier(os.getenv("SNOWFLAKE_DATABASE", "AI_READY_DEMO"), "database")
    raw_schema = "AI_READY_DEMO_RAW"
    stage_name = "SYNTHETIC_LOAD_STAGE"
    data_dir = Path(data_dir).resolve()
    if not data_dir.is_dir():
        raise FileNotFoundError(data_dir)
    upload_dir = Path(tempfile.mkdtemp(prefix=".airdw-snowflake-load-", dir=data_dir))
    connection = None
    row_counts: dict[str, int] = {}
    try:
        connection = _snowflake_connect()
        with connection.cursor() as cursor:
            cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {database}.{raw_schema}")
            cursor.execute(f"CREATE STAGE IF NOT EXISTS {database}.{raw_schema}.{stage_name}")
            for table in tables:
                file_path = data_dir / f"{table}.csv"
                if not file_path.is_file():
                    raise FileNotFoundError(file_path)
                source_path = file_path
                if table == "companies" and not include_future_company_change:
                    source_path = upload_dir / "companies_baseline.csv"
                    write_baseline_company_seed(file_path, source_path)
                with file_path.open(encoding="utf-8", newline="") as stream:
                    columns = next(csv.reader(stream))
                _validate_csv_columns(table, columns)
                typed_columns = [
                    f"{_identifier(name, 'CSV column')} {snowflake_type}"
                    for name, snowflake_type, _ in RAW_COLUMN_TYPES[table]
                ]
                qualified_table = f"{database}.{raw_schema}.{_identifier(table, 'table')}"
                if table in truncate_tables:
                    cursor.execute(f"TRUNCATE TABLE {qualified_table}")
                else:
                    cursor.execute("CREATE OR REPLACE TABLE " + qualified_table + " (" +
                                   ", ".join(typed_columns) + ")")
                upload_path = upload_dir / f"{table}_{uuid4().hex}.csv"
                _write_timestamp_ntz_csv(table, source_path, upload_path)
                file_uri = "file://" + upload_path.as_posix().replace("'", "''")
                cursor.execute(f"PUT '{file_uri}' @{database}.{raw_schema}.{stage_name}/{table} AUTO_COMPRESS=TRUE OVERWRITE=TRUE")
                cursor.execute(
                    f'''COPY INTO {qualified_table}
                        FROM @{database}.{raw_schema}.{stage_name}/{table}
                        FILE_FORMAT=(TYPE=CSV SKIP_HEADER=1 FIELD_OPTIONALLY_ENCLOSED_BY='"' EMPTY_FIELD_AS_NULL=TRUE)
                        ON_ERROR='ABORT_STATEMENT' PURGE=TRUE'''
                )
                cursor.execute(f"SELECT COUNT(*) FROM {qualified_table}")
                row_counts[table] = int(cursor.fetchone()[0])
        connection.commit()
    finally:
        if connection is not None:
            connection.close()
        shutil.rmtree(upload_dir, ignore_errors=True)
    return row_counts


def _dbt_command(command_name: str, project_dir: Path, profiles_dir: Path, target: str, *args: str,
                 executable: str = "dbt") -> None:
    command = build_dbt_command(command_name, *args, project_dir=project_dir,
                                profiles_dir=profiles_dir, target=target,
                                target_path=ROOT / "target" / "cloud", executable=executable)
    print("+", " ".join(command), flush=True)
    subprocess.run(command, cwd=ROOT, check=True)


def _dbt_seed_dir(data_dir: Path, tables: tuple[str, ...] = SNOWFLAKE_TABLES,
                  include_future_company_change: bool = False) -> Path:
    """Copy generator CSVs into the Databricks cloud project's ignored seed path."""
    destination = CLOUD_PROJECT / "seeds"
    destination.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix=".airdw-databricks-seed-", dir=Path(data_dir)) as temp_dir:
        for table in tables:
            source = Path(data_dir) / f"{table}.csv"
            if not source.is_file():
                raise FileNotFoundError(source)
            if table == "companies" and not include_future_company_change:
                baseline_path = Path(temp_dir) / source.name
                write_baseline_company_seed(source, baseline_path)
                source = baseline_path
            _write_timestamp_ntz_csv(table, source, destination / source.name)
    return destination


def _snowflake_row_counts() -> dict[str, int]:
    database = _identifier(os.getenv("SNOWFLAKE_DATABASE", "AI_READY_DEMO"), "database")
    connection = _snowflake_connect()
    counts: dict[str, int] = {}
    try:
        with connection.cursor() as cursor:
            for schema, table in TABLES:
                cursor.execute(f"SELECT COUNT(*) FROM {database}.{_identifier(schema)}.{_identifier(table)}")
                counts[f"{schema}.{table}"] = int(cursor.fetchone()[0])
    finally:
        connection.close()
    return counts


def _databricks_connect():
    try:
        from databricks import sql  # type: ignore
    except ImportError as exc:
        raise RuntimeError("Install requirements-cloud.txt to use the Databricks target") from exc
    required = ("DATABRICKS_HOST", "DATABRICKS_HTTP_PATH", "DATABRICKS_TOKEN", "DATABRICKS_CATALOG")
    missing = [key for key in required if not os.getenv(key)]
    if missing:
        raise RuntimeError("Missing environment variables: " + ", ".join(missing))
    return sql.connect(server_hostname=os.environ["DATABRICKS_HOST"].removeprefix("https://").rstrip("/"),
                       http_path=os.environ["DATABRICKS_HTTP_PATH"], access_token=os.environ["DATABRICKS_TOKEN"])


def _databricks_row_counts() -> dict[str, int]:
    catalog = _identifier(os.environ["DATABRICKS_CATALOG"], "catalog")
    connection = _databricks_connect()
    counts: dict[str, int] = {}
    try:
        with connection.cursor() as cursor:
            for schema, table in TABLES:
                cursor.execute(f"SELECT COUNT(*) FROM {catalog}.{_identifier(schema)}.{_identifier(table)}")
                counts[f"{schema}.{table}"] = int(cursor.fetchone()[0])
    finally:
        connection.close()
    return counts


def _cloud_connect(target: str):
    if target == "snowflake":
        return _snowflake_connect()
    return _databricks_connect()


def _cloud_relation(target: str, schema: str, table: str) -> str:
    if target == "snowflake":
        database = _identifier(os.getenv("SNOWFLAKE_DATABASE", "AI_READY_DEMO"), "database")
    else:
        database = _identifier(os.getenv("DATABRICKS_CATALOG", ""), "catalog")
    return f"{database}.{_identifier(schema)}.{_identifier(table, 'table')}"


def _verify_cloud_scd2(target: str, company_id: str = "C0001") -> tuple[int, int]:
    if not re.fullmatch(r"C[0-9]+", company_id):
        raise ValueError("Invalid synthetic company ID")
    connection = _cloud_connect(target)
    try:
        with connection.cursor() as cursor:
            snapshot = _cloud_relation(target, "ai_ready_demo_snapshots", "company_history")
            cursor.execute(
                f"""SELECT COUNT(*), COALESCE(SUM(CASE WHEN DBT_VALID_TO IS NULL THEN 1 ELSE 0 END), 0)
                    FROM {snapshot} WHERE COMPANY_ID = '{company_id}'"""
            )
            versions, current = cursor.fetchone()
    finally:
        connection.close()
    if versions < 2 or current != 1:
        raise RuntimeError(f"SCD2 check failed for {company_id}: versions={versions}, current={current}")
    print(f"SCD2 version check passed: {company_id} versions={versions}, current={current}")
    return int(versions), int(current)


def _timestamp_as_utc_naive(value: object) -> datetime:
    parsed = value if isinstance(value, datetime) else datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc).replace(tzinfo=None)


def _current_snapshot_company(target: str, company_id: str = "C0001") -> tuple[str, datetime]:
    connection = _cloud_connect(target)
    try:
        with connection.cursor() as cursor:
            snapshot = _cloud_relation(target, "ai_ready_demo_snapshots", "company_history")
            cursor.execute(
                f"SELECT SECTOR, INGESTED_AT FROM {snapshot} "
                f"WHERE COMPANY_ID = '{company_id}' AND DBT_VALID_TO IS NULL"
            )
            row = cursor.fetchone()
    finally:
        connection.close()
    if not row:
        raise RuntimeError(f"Changed-company reload failed: no baseline snapshot row for {company_id}")
    return str(row[0]), _timestamp_as_utc_naive(row[1])


def _changed_company_values(companies_csv: Path, baseline_sector: str,
                            baseline_ingested_at: datetime) -> tuple[str, datetime]:
    with Path(companies_csv).open("r", newline="", encoding="utf-8") as stream:
        rows = [row for row in csv.DictReader(stream) if row.get("company_id") == "C0001"]
    changed = [row for row in rows if row.get("sector") != baseline_sector]
    if not changed:
        raise RuntimeError("Changed-company reload failed: generated C0001 sector does not differ from baseline")
    latest = max(changed, key=lambda row: row["ingested_at"])
    changed_ingested_at = _timestamp_as_utc_naive(latest["ingested_at"])
    now_utc = datetime.now(timezone.utc).replace(tzinfo=None)
    if changed_ingested_at <= baseline_ingested_at or changed_ingested_at > now_utc:
        raise RuntimeError(
            "Changed-company reload failed: C0001 ingested_at must be newer than baseline and no later than now"
        )
    return latest["sector"], changed_ingested_at


def _ensure_changed_company_attribute(companies_csv: Path, baseline_sector: str,
                                      company_id: str = "C0001") -> None:
    """Keep the generated SCD2 attribute different from the current snapshot row."""
    path = Path(companies_csv)
    with path.open("r", newline="", encoding="utf-8") as stream:
        reader = csv.DictReader(stream)
        fieldnames = reader.fieldnames
        rows = list(reader)
    if not fieldnames:
        raise RuntimeError("Changed-company reload failed: generated companies CSV has no header")
    versions = [row for row in rows if row.get("company_id") == company_id]
    if len(versions) < 2:
        raise RuntimeError(f"Changed-company reload failed: expected two generated rows for {company_id}")
    changed = max(versions, key=lambda row: row["ingested_at"])
    if changed["sector"] == baseline_sector:
        alternatives = sorted({row["sector"] for row in rows
                               if row.get("company_id") != company_id and row.get("sector") != baseline_sector})
        if not alternatives:
            raise RuntimeError(f"Changed-company reload failed: no alternate sector is available for {company_id}")
        changed["sector"] = alternatives[0]
    temporary_path = path.with_suffix(path.suffix + ".tmp")
    try:
        with temporary_path.open("w", newline="", encoding="utf-8") as stream:
            writer = csv.DictWriter(stream, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)
        temporary_path.replace(path)
    finally:
        temporary_path.unlink(missing_ok=True)


def _assert_raw_company_reloaded(target: str, baseline_sector: str, baseline_ingested_at: datetime,
                                 changed_sector: str, changed_ingested_at: datetime,
                                 company_id: str = "C0001") -> None:
    if not re.fullmatch(r"C[0-9]+", company_id) or not re.fullmatch(r"[A-Za-z_]+", changed_sector):
        raise ValueError("Invalid generated company values for reload assertion")
    connection = _cloud_connect(target)
    try:
        with connection.cursor() as cursor:
            raw_companies = _cloud_relation(target, "ai_ready_demo_raw", "companies")
            cursor.execute(
                f"SELECT SECTOR, INGESTED_AT FROM {raw_companies} "
                f"WHERE COMPANY_ID = '{company_id}' ORDER BY INGESTED_AT DESC LIMIT 1"
            )
            row = cursor.fetchone()
    finally:
        connection.close()
    if not row:
        raise RuntimeError(f"Changed-company reload failed: raw {company_id} row is missing")
    raw_sector = str(row[0])
    raw_ingested_at = _timestamp_as_utc_naive(row[1])
    now_utc = datetime.now(timezone.utc).replace(tzinfo=None)
    if (raw_sector != changed_sector or raw_sector == baseline_sector
            or raw_ingested_at <= baseline_ingested_at
            or raw_ingested_at != changed_ingested_at or raw_ingested_at > now_utc):
        raise RuntimeError(
            f"Changed-company reload failed: raw {company_id} row is sector={raw_sector}, "
            f"ingested_at={raw_ingested_at.isoformat()}; expected sector={changed_sector} "
            f"with ingested_at newer than baseline {baseline_ingested_at.isoformat()}"
        )
    print(f"Changed-company raw reload check passed: {company_id} ingested_at={raw_ingested_at.isoformat()}")


def _truncate_databricks_raw_table(table: str) -> None:
    relation = _cloud_relation("databricks", "ai_ready_demo_raw", table)
    connection = _databricks_connect()
    try:
        with connection.cursor() as cursor:
            cursor.execute(f"TRUNCATE TABLE {relation}")
    finally:
        connection.close()


def _cloud_fact_fingerprints(target: str) -> dict[str, tuple[int, str]]:
    """Stream stable fact rows into SHA-256 checksums on either cloud target."""
    connection = _cloud_connect(target)
    fingerprints: dict[str, tuple[int, str]] = {}
    order_by = {
        "fct_holdings_daily": "HOLDING_ID",
        "fct_transactions": "TRANSACTION_ID",
    }
    try:
        with connection.cursor() as cursor:
            for table, ordering in order_by.items():
                relation = _cloud_relation(target, "ai_ready_demo_marts", table)
                cursor.execute(f"SELECT COUNT(*) FROM {relation}")
                count = int(cursor.fetchone()[0])
                cursor.execute(f"SELECT * FROM {relation} ORDER BY {ordering}")
                digest = hashlib.sha256()
                while True:
                    batch = cursor.fetchmany(2000)
                    if not batch:
                        break
                    for row in batch:
                        digest.update(json.dumps(row, default=str, separators=(",", ":")).encode("utf-8"))
                        digest.update(b"\n")
                fingerprints[table] = (count, digest.hexdigest())
    finally:
        connection.close()
    return fingerprints


def _snowflake_credits(start: datetime, end: datetime) -> tuple[float | None, str]:
    try:
        # ACCOUNT_USAGE queries need a warehouse. This uses the same XS warehouse
        # and is outside the recorded build window, so the measurement is explicit.
        connection = _snowflake_connect()
        try:
            warehouse = _identifier(os.getenv("SNOWFLAKE_WAREHOUSE", "AI_READY_XS_WH"), "warehouse")
            with connection.cursor() as cursor:
                cursor.execute("""SELECT SUM(CREDITS_USED) FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
                                WHERE WAREHOUSE_NAME = %s AND START_TIME <= %s AND END_TIME >= %s""",
                               (warehouse, end, start))
                value = cursor.fetchone()[0]
            return (float(value) if value is not None else None,
                    "available" if value is not None else "no visible metering rows; account usage can be delayed")
        finally:
            connection.close()
    except Exception as exc:
        return None, f"unavailable ({type(exc).__name__}); check ACCOUNT_USAGE grants and reporting delay"


def _databricks_dbus(start: datetime, end: datetime) -> tuple[float | None, str]:
    try:
        warehouse_id = os.environ["DATABRICKS_WAREHOUSE_ID"]
        if not re.fullmatch(r"[A-Za-z0-9_-]+", warehouse_id):
            raise ValueError("Invalid Databricks warehouse ID")
        connection = _databricks_connect()
        try:
            # All interpolated values are validated identifiers or generated ISO timestamps.
            start_sql = start.strftime("%Y-%m-%d %H:%M:%S")
            end_sql = end.strftime("%Y-%m-%d %H:%M:%S")
            sql_text = f"""SELECT SUM(usage_quantity)
                          FROM system.billing.usage
                          WHERE usage_unit = 'DBU'
                            AND usage_metadata.warehouse_id = '{warehouse_id}'
                            AND usage_start_time < TIMESTAMP '{end_sql}'
                            AND usage_end_time > TIMESTAMP '{start_sql}'"""
            with connection.cursor() as cursor:
                cursor.execute(sql_text)
                value = cursor.fetchone()[0]
            return (float(value) if value is not None else None,
                    "available" if value is not None else "no visible billing rows; system usage can be delayed")
        finally:
            connection.close()
    except Exception as exc:
        return None, f"unavailable ({type(exc).__name__}); check system.billing.usage access and reporting delay"


def run_measurement(*, target: str, days: int, companies: int, seed: int,
                    profiles_dir: Path, dbt_executable: str = "dbt") -> Path:
    if target not in ("snowflake", "databricks"):
        raise ValueError("Cloud measurement target must be snowflake or databricks")
    if target == "snowflake":
        _snowflake_auth_kwargs()
    if target == "databricks" and not os.getenv("DATABRICKS_TOKEN"):
        raise RuntimeError("Set Databricks connection environment variables before running this target")

    from generator.generate import generate, promote_changed_company_version
    dbt_target = snowflake_profile_target() if target == "snowflake" else target

    data_dir = ROOT / "data" / "generated"
    generated_at = time.perf_counter()
    generated_counts = generate(data_dir, seed=seed, days=days, companies_count=companies)
    generation_seconds = time.perf_counter() - generated_at

    started_utc = datetime.now(timezone.utc)
    load_start = time.perf_counter()
    if target == "snowflake":
        load_counts = load_snowflake_csvs(data_dir)
    else:
        _dbt_seed_dir(data_dir)
        _dbt_command("seed", CLOUD_PROJECT, profiles_dir, dbt_target, "--full-refresh",
                     executable=dbt_executable)
        load_counts = generated_counts.copy()
        # Hold the generated future-dated company version until after the
        # baseline snapshot, when it is promoted and reloaded deliberately.
        load_counts["companies"] -= 1
    load_seconds = time.perf_counter() - load_start
    _validate_raw_schema(target)
    _dbt_command("source freshness", CLOUD_PROJECT, profiles_dir, dbt_target, executable=dbt_executable)

    build_seconds = 0.0
    baseline_company: tuple[str, datetime] | None = None
    for step in range(1, 4):
        if step == 2:
            # The generated future-dated C0001 attribute change now becomes visible.
            if baseline_company is None:
                raise RuntimeError("Changed-company reload failed: baseline snapshot was not captured")
            baseline_sector, baseline_ingested_at = baseline_company
            _ensure_changed_company_attribute(data_dir / "companies.csv", baseline_sector)
            promote_changed_company_version(data_dir / "companies.csv")
            changed_sector, changed_ingested_at = _changed_company_values(
                data_dir / "companies.csv", baseline_sector, baseline_ingested_at
            )
            changed_load_start = time.perf_counter()
            if target == "snowflake":
                changed_counts = load_snowflake_csvs(
                    data_dir, tables=("companies",), truncate_tables=("companies",),
                    include_future_company_change=True,
                )
            else:
                _dbt_seed_dir(data_dir, tables=("companies",), include_future_company_change=True)
                _truncate_databricks_raw_table("companies")
                _dbt_command("seed", CLOUD_PROJECT, profiles_dir, dbt_target,
                             "--select", "companies", executable=dbt_executable)
            _validate_raw_schema(target, tables=("companies",))
            _assert_raw_company_reloaded(target, baseline_sector, baseline_ingested_at,
                                         changed_sector, changed_ingested_at)
            load_counts["companies"] = changed_counts["companies"] if target == "snowflake" else generated_counts["companies"]
            load_seconds += time.perf_counter() - changed_load_start

        build_start = time.perf_counter()
        _dbt_command("build", CLOUD_PROJECT, profiles_dir, dbt_target,
                     "--exclude", "resource_type:seed", executable=dbt_executable)
        build_seconds += time.perf_counter() - build_start

        if step == 2:
            _verify_cloud_scd2(target)
            first_fingerprints = _cloud_fact_fingerprints(target)
        elif step == 3:
            second_fingerprints = _cloud_fact_fingerprints(target)
        elif step == 1:
            baseline_company = _current_snapshot_company(target)

    if first_fingerprints != second_fingerprints:
        raise RuntimeError(
            f"Idempotency check failed; first={first_fingerprints}, second={second_fingerprints}"
        )
    table_names = ", ".join(first_fingerprints)
    print(f"Idempotency check passed: {table_names} row counts and checksums identical")

    build_end_utc = datetime.now(timezone.utc)

    if target == "snowflake":
        row_counts = _snowflake_row_counts()
        credits, billing_status = _snowflake_credits(started_utc, build_end_utc)
        dbus = None
    else:
        row_counts = _databricks_row_counts()
        dbus, billing_status = _databricks_dbus(started_utc, build_end_utc)
        credits = None

    # Include the verified fact row counts from the fingerprint step in the report.
    for table, (count, _) in second_fingerprints.items():
        key = f"ai_ready_demo_marts.{table}"
        row_counts[key] = count

    measured_at = datetime.now(timezone.utc)
    record = build_measurement_record(
        target=target,
        days=days,
        companies=companies,
        generated_counts=generated_counts,
        load_counts=load_counts,
        generation_seconds=generation_seconds,
        load_seconds=load_seconds,
        snapshot_seconds=None,
        build_seconds=build_seconds,
        credits=credits,
        dbus=dbus,
        billing_status=billing_status,
        started_utc=started_utc,
        build_end_utc=build_end_utc,
        measured_at=measured_at,
        row_counts=row_counts,
    )
    size_slug = f"{days}d-{companies}co-{generated_counts['holdings']}h"
    results_dir = ROOT / "results" / "cloud_runs"
    results_dir.mkdir(parents=True, exist_ok=True)
    result_path = results_dir / f"{measured_at:%Y-%m-%dT%H%M%S%fZ}_{target}_{size_slug}.json"
    result_path.write_text(json.dumps(record, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(f"Measurement recorded: {result_path}")
    return result_path


def build_measurement_record(*, target: str, days: int, companies: int,
                             generated_counts: dict[str, int], load_counts: dict[str, int],
                             generation_seconds: float, load_seconds: float,
                             snapshot_seconds: float | None, build_seconds: float,
                             credits: float | None, dbus: float | None,
                             billing_status: str, started_utc: datetime,
                             build_end_utc: datetime, measured_at: datetime,
                             row_counts: dict[str, int]) -> dict[str, Any]:
    """Build a portable measurement payload, separated from cloud I/O."""
    return {
        "synthetic_data": True,
        "target": target,
        "measured_at_utc": measured_at.isoformat(),
        "data_size": {
            "label": f"synthetic; {days} days; {companies} companies; {generated_counts['holdings']} generated holdings rows",
            "days": days,
            "companies": companies,
            "generated_source_rows": generated_counts,
            "loaded_source_rows": load_counts,
        },
        "timings_seconds": {
            "generation": round(generation_seconds, 3),
            "load": round(load_seconds, 3),
            "snapshot": round(snapshot_seconds, 3) if snapshot_seconds is not None else None,
            "snapshot_note": "Included in dbt build DAG order" if snapshot_seconds is None else None,
            "build": round(build_seconds, 3),
        },
        "platform_usage": {
            "snowflake_credits": credits,
            "databricks_dbus": dbus,
            "status": billing_status,
            "measurement_window_start_utc": started_utc.isoformat(),
            "measurement_window_end_utc": build_end_utc.isoformat(),
        },
        "mart_and_ai_ready_row_counts": row_counts,
        "results_status": "measured locally by the user; synthetic data only",
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--target", choices=("snowflake", "databricks"), required=True)
    parser.add_argument("--days", type=int, default=30)
    parser.add_argument("--companies", type=int, default=400)
    parser.add_argument("--seed", type=int, default=20261027)
    parser.add_argument("--profiles-dir", type=Path,
                        default=Path(os.getenv("DBT_PROFILES_DIR", Path.home() / ".dbt")))
    parser.add_argument("--dbt-executable", default="dbt")
    args = parser.parse_args(argv)
    try:
        run_measurement(target=args.target, days=args.days, companies=args.companies, seed=args.seed,
                        profiles_dir=args.profiles_dir, dbt_executable=args.dbt_executable)
    except (OSError, RuntimeError, ValueError, subprocess.CalledProcessError) as exc:
        print(f"Cloud measurement failed: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
