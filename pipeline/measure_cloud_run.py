"""Load/build a cloud target and write a dated, synthetic-data measurement record.

Cloud billing tables can be delayed or permission-gated. In that case the JSON
records null credit/DBU values with an accounting status instead of inventing a
number. This module never commits, pushes, or creates a real cloud result here.
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import re
import shutil
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
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


def _identifier(value: str, label: str = "SQL identifier") -> str:
    if not IDENTIFIER.fullmatch(value):
        raise ValueError(f"Invalid {label}: {value!r}")
    return value.upper()


def _snowflake_connect(*, include_warehouse: bool = True, include_database: bool = True):
    try:
        import snowflake.connector  # type: ignore
    except ImportError as exc:
        raise RuntimeError("Install requirements-cloud.txt to use the Snowflake target") from exc
    required = ("SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER", "SNOWFLAKE_PASSWORD")
    missing = [key for key in required if not os.getenv(key)]
    if missing:
        raise RuntimeError("Missing environment variables: " + ", ".join(missing))
    kwargs: dict[str, Any] = {
        "account": os.environ["SNOWFLAKE_ACCOUNT"],
        "user": os.environ["SNOWFLAKE_USER"],
        "password": os.environ["SNOWFLAKE_PASSWORD"],
        "role": os.getenv("SNOWFLAKE_ROLE") or None,
    }
    if include_warehouse:
        kwargs["warehouse"] = os.getenv("SNOWFLAKE_WAREHOUSE", "AI_READY_XS_WH")
    if include_database and os.getenv("SNOWFLAKE_DATABASE"):
        kwargs["database"] = _identifier(os.environ["SNOWFLAKE_DATABASE"], "database")
    return snowflake.connector.connect(**kwargs)


def load_snowflake_csvs(data_dir: Path) -> dict[str, int]:
    """PUT generated CSVs to an internal stage and COPY them into raw tables."""
    database = _identifier(os.getenv("SNOWFLAKE_DATABASE", "AI_READY_DEMO"), "database")
    raw_schema = "AI_READY_DEMO_RAW"
    stage_name = "SYNTHETIC_LOAD_STAGE"
    data_dir = Path(data_dir).resolve()
    if not data_dir.is_dir():
        raise FileNotFoundError(data_dir)
    connection = _snowflake_connect()
    row_counts: dict[str, int] = {}
    try:
        with connection.cursor() as cursor:
            cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {database}.{raw_schema}")
            cursor.execute(f"CREATE STAGE IF NOT EXISTS {database}.{raw_schema}.{stage_name}")
            for table in SNOWFLAKE_TABLES:
                file_path = data_dir / f"{table}.csv"
                if not file_path.is_file():
                    raise FileNotFoundError(file_path)
                with file_path.open(encoding="utf-8", newline="") as stream:
                    columns = next(csv.reader(stream))
                safe_columns = [_identifier(name, "CSV column") for name in columns]
                if not safe_columns:
                    raise ValueError(f"No header columns in {file_path}")
                qualified_table = f"{database}.{raw_schema}.{_identifier(table, 'table')}"
                cursor.execute("CREATE OR REPLACE TABLE " + qualified_table + " (" +
                               ", ".join(f"{column} VARCHAR" for column in safe_columns) + ")")
                file_uri = "file://" + file_path.as_posix().replace("'", "''")
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
        connection.close()
    return row_counts


def _dbt_command(project_dir: Path, profiles_dir: Path, target: str, *args: str,
                 executable: str = "dbt") -> None:
    command = [executable, "--project-dir", str(project_dir), "--profiles-dir", str(profiles_dir),
               "--target", target, *args]
    print("+", " ".join(command), flush=True)
    subprocess.run(command, cwd=ROOT, check=True)


def _dbt_seed_dir(data_dir: Path) -> Path:
    """Copy generator CSVs into the Databricks cloud project's ignored seed path."""
    destination = CLOUD_PROJECT / "seeds"
    destination.mkdir(parents=True, exist_ok=True)
    for table in SNOWFLAKE_TABLES:
        source = Path(data_dir) / f"{table}.csv"
        if not source.is_file():
            raise FileNotFoundError(source)
        shutil.copy2(source, destination / source.name)
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
    if target == "snowflake" and not os.getenv("SNOWFLAKE_PASSWORD"):
        raise RuntimeError("Set Snowflake connection environment variables before running this target")
    if target == "databricks" and not os.getenv("DATABRICKS_TOKEN"):
        raise RuntimeError("Set Databricks connection environment variables before running this target")

    from generator.generate import generate

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
        _dbt_command(CLOUD_PROJECT, profiles_dir, target, "seed", "--full-refresh", executable=dbt_executable)
        load_counts = generated_counts.copy()
    load_seconds = time.perf_counter() - load_start

    _dbt_command(CLOUD_PROJECT, profiles_dir, target, "source", "freshness", executable=dbt_executable)
    snapshot_start = time.perf_counter()
    _dbt_command(CLOUD_PROJECT, profiles_dir, target, "snapshot", executable=dbt_executable)
    snapshot_seconds = time.perf_counter() - snapshot_start
    build_start = time.perf_counter()
    _dbt_command(CLOUD_PROJECT, profiles_dir, target, "build", executable=dbt_executable)
    build_seconds = time.perf_counter() - build_start
    build_end_utc = datetime.now(timezone.utc)
    _dbt_command(CLOUD_PROJECT, profiles_dir, target, "docs", "generate", executable=dbt_executable)

    if target == "snowflake":
        row_counts = _snowflake_row_counts()
        credits, billing_status = _snowflake_credits(started_utc, build_end_utc)
        dbus = None
    else:
        row_counts = _databricks_row_counts()
        dbus, billing_status = _databricks_dbus(started_utc, build_end_utc)
        credits = None

    measured_at = datetime.now(timezone.utc)
    record = build_measurement_record(
        target=target,
        days=days,
        companies=companies,
        generated_counts=generated_counts,
        load_counts=load_counts,
        generation_seconds=generation_seconds,
        load_seconds=load_seconds,
        snapshot_seconds=snapshot_seconds,
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
                             snapshot_seconds: float, build_seconds: float,
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
            "snapshot": round(snapshot_seconds, 3),
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
