"""Interactively remove only the named Phase 2 demo resources."""
from __future__ import annotations

import argparse
import os
import re
import sys

SNOWFLAKE_DATABASE = "AI_READY_DEMO"
SNOWFLAKE_WAREHOUSE = "AI_READY_XS_WH"
SNOWFLAKE_MONITOR = "AI_READY_DEMO_MONITOR"
SNOWFLAKE_ROLE = "AIRDW_ROLE"
DEMO_SCHEMAS = (
    "AI_READY_DEMO_RAW", "AI_READY_DEMO_STAGING", "AI_READY_DEMO_INTERMEDIATE",
    "AI_READY_DEMO_MARTS", "AI_READY_DEMO_AI_READY", "AI_READY_DEMO_SNAPSHOTS",
)


def _confirmation(target: str) -> bool:
    if target == "snowflake":
        expected = "DROP AI_READY_DEMO"
        message = "This drops the dedicated demo database and warehouse. "
    else:
        catalog = os.getenv("DATABRICKS_CATALOG", "")
        expected = f"DROP {catalog}.AI_READY_DEMO_SCHEMAS_AND_WAREHOUSE"
        message = "This drops the demo-prefixed schemas in the selected catalog and deletes its SQL warehouse. "
    print(message + f"Type {expected!r} to continue.")
    return input("> ").strip() == expected


def teardown_snowflake() -> None:
    if os.getenv("SNOWFLAKE_DATABASE", SNOWFLAKE_DATABASE).upper() != SNOWFLAKE_DATABASE:
        raise ValueError("Teardown is limited to SNOWFLAKE_DATABASE=AI_READY_DEMO")
    if os.getenv("SNOWFLAKE_WAREHOUSE", SNOWFLAKE_WAREHOUSE).upper() != SNOWFLAKE_WAREHOUSE:
        raise ValueError("Teardown is limited to SNOWFLAKE_WAREHOUSE=AI_READY_XS_WH")
    if os.getenv("SNOWFLAKE_ROLE", "").upper() != SNOWFLAKE_ROLE:
        raise ValueError("Set SNOWFLAKE_ROLE=AIRDW_ROLE; Snowflake teardown must run as the demo role")
    from pipeline.measure_cloud_run import _snowflake_connect

    connection = _snowflake_connect(include_warehouse=False, include_database=False)
    try:
        with connection.cursor() as cursor:
            cursor.execute("SELECT CURRENT_ROLE()")
            current_role = cursor.fetchone()[0]
            if str(current_role).upper() != SNOWFLAKE_ROLE:
                raise RuntimeError(f"Connected Snowflake role is {current_role!r}, expected {SNOWFLAKE_ROLE}")
            cursor.execute(f"DROP DATABASE IF EXISTS {SNOWFLAKE_DATABASE}")
            cursor.execute(f"DROP WAREHOUSE IF EXISTS {SNOWFLAKE_WAREHOUSE}")
            cursor.execute(f"DROP RESOURCE MONITOR IF EXISTS {SNOWFLAKE_MONITOR}")
        connection.commit()
    finally:
        connection.close()


def teardown_databricks() -> None:
    catalog = os.getenv("DATABRICKS_CATALOG", "")
    warehouse_id = os.getenv("DATABRICKS_WAREHOUSE_ID", "")
    if not catalog or not warehouse_id:
        raise RuntimeError("Set DATABRICKS_CATALOG and DATABRICKS_WAREHOUSE_ID before teardown")
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", catalog) or not re.fullmatch(r"[A-Za-z0-9_-]+", warehouse_id):
        raise ValueError("Catalog and warehouse ID must contain only letters, digits, underscores, and hyphens")
    from pipeline.measure_cloud_run import _databricks_connect

    connection = _databricks_connect()
    try:
        with connection.cursor() as cursor:
            for schema in DEMO_SCHEMAS:
                cursor.execute(f"DROP SCHEMA IF EXISTS {catalog}.{schema} CASCADE")
    finally:
        connection.close()
    try:
        from databricks.sdk import WorkspaceClient  # type: ignore
    except ImportError as exc:
        raise RuntimeError("Install requirements-cloud.txt to delete the Databricks SQL warehouse") from exc
    host = os.environ["DATABRICKS_HOST"].rstrip("/")
    if not host.startswith("https://"):
        host = "https://" + host.removeprefix("http://")
    WorkspaceClient(host=host, token=os.environ["DATABRICKS_TOKEN"]).warehouses.delete(id=warehouse_id)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--target", choices=("snowflake", "databricks"), required=True)
    args = parser.parse_args(argv)
    try:
        if not _confirmation(args.target):
            print("Teardown cancelled; no resources were changed.")
            return 2
        if args.target == "snowflake":
            teardown_snowflake()
        else:
            teardown_databricks()
    except Exception as exc:
        print(f"Cloud teardown failed: {exc}", file=sys.stderr)
        return 1
    print(f"{args.target} Phase 2 demo resources removed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
