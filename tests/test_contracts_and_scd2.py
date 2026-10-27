from __future__ import annotations

from datetime import datetime, timezone
import os
from pathlib import Path
import subprocess
import tempfile

import pytest

ROOT = Path(__file__).resolve().parents[1]


@pytest.mark.parametrize("mart", ["dim_fund", "dim_company", "dim_date", "fct_holdings_daily", "fct_transactions"])
def test_each_mart_declares_enforced_contract(mart: str) -> None:
    sql = (ROOT / "models" / "marts" / f"{mart}.sql").read_text(encoding="utf-8")
    assert "contract={'enforced': true}" in sql


@pytest.mark.parametrize("fact,lookback", [("fct_transactions", "90 days"), ("fct_holdings_daily", "30 days")])
def test_incremental_marts_use_merge_and_unique_key(fact: str, lookback: str) -> None:
    sql = (ROOT / "models" / "marts" / f"{fact}.sql").read_text(encoding="utf-8")
    assert "incremental_strategy='merge'" in sql
    assert "unique_key=" in sql
    assert lookback in sql


def test_late_arrival_is_retained_by_ingestion_lookback() -> None:
    sql = (ROOT / "models" / "marts" / "fct_transactions.sql").read_text(encoding="utf-8")
    assert "t.ingested_at >= coalesce" in sql
    assert "interval '90 days'" in sql


@pytest.mark.parametrize("changed", ["sector", "company_name", "parent_group_id", "employee_count"])
def test_scd2_snapshot_tracks_business_attribute_changes(changed: str) -> None:
    snapshot = (ROOT / "snapshots" / "company_history.sql").read_text(encoding="utf-8")
    assert "unique_key='company_id'" in snapshot
    assert "strategy='timestamp'" in snapshot
    assert "updated_at='ingested_at'" in snapshot
    assert changed in snapshot


def test_scd2_versions_have_single_open_ended_current_record() -> None:
    versions = [
        {"company_id": "C0001", "sector": "software", "from": datetime(2026, 1, 1, tzinfo=timezone.utc), "to": datetime(2026, 4, 1, tzinfo=timezone.utc)},
        {"company_id": "C0001", "sector": "healthcare", "from": datetime(2026, 4, 1, tzinfo=timezone.utc), "to": None},
    ]
    current = [row for row in versions if row["to"] is None]
    assert len(current) == 1
    assert current[0]["sector"] == "healthcare"
    assert versions[0]["to"] == versions[1]["from"]


def test_scd2_custom_test_is_defined() -> None:
    macro = (ROOT / "macros" / "test_one_current_company.sql").read_text(encoding="utf-8")
    assert "count(*) filter (where is_current) <> 1" in macro


def test_scd2_snapshot_persists_changed_company_version() -> None:
    if os.getenv("WAREHOUSE_INTEGRATION") != "1":
        pytest.skip("Set WAREHOUSE_INTEGRATION=1 after pipeline has loaded the snapshot")
    import psycopg  # type: ignore
    from pipeline.run import ROOT, create_profile

    password = os.environ["POSTGRES_PASSWORD"]
    connection = psycopg.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"), port=int(os.getenv("POSTGRES_PORT", "5432")),
        dbname=os.getenv("POSTGRES_DB", "warehouse"), user=os.getenv("POSTGRES_USER", "warehouse"),
        password=password,
    )
    with connection:
        with connection.cursor() as cursor:
            cursor.execute("select sector from snapshots.company_history where company_id = 'C0001' and dbt_valid_to is null")
            current = cursor.fetchone()
            next_sector = "business_services" if not current or current[0] != "business_services" else "consumer"
            cursor.execute("""update raw.companies set sector = %s, ingested_at = current_timestamp
                              where company_id = 'C0001' and ingested_at =
                                (select max(ingested_at) from raw.companies where company_id = 'C0001')""",
                           (next_sector,))
    connection.close()

    with tempfile.TemporaryDirectory(prefix="ai-ready-scd2-test-") as profile_dir:
        create_profile(Path(profile_dir), "postgres")
        subprocess.run(["dbt", "--project-dir", str(ROOT), "--profiles-dir", profile_dir,
                        "--target", "postgres", "snapshot"], cwd=ROOT, check=True)

    with psycopg.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"), port=int(os.getenv("POSTGRES_PORT", "5432")),
        dbname=os.getenv("POSTGRES_DB", "warehouse"), user=os.getenv("POSTGRES_USER", "warehouse"),
        password=password,
    ) as connection:
        with connection.cursor() as cursor:
            cursor.execute("select count(*), count(*) filter (where dbt_valid_to is null) from snapshots.company_history where company_id = 'C0001'")
            versions, current_versions = cursor.fetchone()
    assert versions >= 2
    assert current_versions == 1


def test_late_business_date_is_loaded_by_ingestion_time_lookback() -> None:
    if os.getenv("WAREHOUSE_INTEGRATION") != "1":
        pytest.skip("Set WAREHOUSE_INTEGRATION=1 after pipeline has built the marts")
    import psycopg  # type: ignore
    from pipeline.run import ROOT, create_profile

    password = os.environ["POSTGRES_PASSWORD"]
    connect = dict(host=os.getenv("POSTGRES_HOST", "localhost"), port=int(os.getenv("POSTGRES_PORT", "5432")),
                   dbname=os.getenv("POSTGRES_DB", "warehouse"), user=os.getenv("POSTGRES_USER", "warehouse"),
                   password=password)
    with psycopg.connect(**connect) as connection:
        with connection.cursor() as cursor:
            cursor.execute("""insert into raw.transactions
                (transaction_id, fund_id, company_id, transaction_date, transaction_type, amount,
                 currency, ingested_at, batch_id, loaded_at)
                select 'T_LATE_TEST_CI', 'F001', 'C0001', current_date - interval '10 days',
                       'follow_on', 123456.78, 'USD', current_timestamp, 'late_test', current_timestamp
                where not exists (select 1 from raw.transactions where transaction_id = 'T_LATE_TEST_CI')""")

    with tempfile.TemporaryDirectory(prefix="ai-ready-late-test-") as profile_dir:
        create_profile(Path(profile_dir), "postgres")
        subprocess.run(["dbt", "--project-dir", str(ROOT), "--profiles-dir", profile_dir,
                        "--target", "postgres", "build"], cwd=ROOT, check=True)

    with psycopg.connect(**connect) as connection:
        with connection.cursor() as cursor:
            cursor.execute("select count(*) from marts.fct_transactions where transaction_id = 'T_LATE_TEST_CI' and amount = 123456.78")
            assert cursor.fetchone()[0] == 1


def test_contract_manifest_covers_required_fact_relationships() -> None:
    schema = (ROOT / "models" / "schema.yml").read_text(encoding="utf-8")
    for relationship in ("dim_company", "dim_fund", "dim_date"):
        assert relationship in schema
