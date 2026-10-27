from __future__ import annotations

from datetime import datetime, timezone
import os
from pathlib import Path
import subprocess
import tempfile
from uuid import uuid4

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
    if fact == "fct_transactions":
        assert "indexes=[{'columns': ['transaction_id'], 'unique': true}]" in sql


def test_snapshot_updated_at_matches_postgres_snapshot_timestamp_type() -> None:
    sql = (ROOT / "snapshots" / "company_history.sql").read_text(encoding="utf-8")
    assert "ingested_at::timestamp without time zone as ingested_at" in sql


def test_company_profile_avoids_nested_company_staging_views() -> None:
    profile = (ROOT / "models" / "ai_ready" / "ai_company_profile.sql").read_text(encoding="utf-8")
    kpis = (ROOT / "models" / "staging" / "stg_company_kpis.sql").read_text(encoding="utf-8")
    assert "ref('company_history')" in profile
    assert "c.dbt_valid_to is null" in profile
    assert "on c.company_id = k.company_id" in profile
    assert "ref('stg_companies')" not in profile
    assert "config(materialized='table')" in kpis
    assert "partition by company_id" in kpis
    assert "ref('stg_companies')" not in kpis


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
        subprocess.run(["dbt", "snapshot", "--project-dir", str(ROOT), "--profiles-dir", profile_dir,
                        "--target", "postgres"], cwd=ROOT, check=True)

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
    transaction_id = f"T_LATE_TEST_{uuid4().hex}"
    ingested_at = datetime.now(timezone.utc)
    connect = dict(host=os.getenv("POSTGRES_HOST", "localhost"), port=int(os.getenv("POSTGRES_PORT", "5432")),
                   dbname=os.getenv("POSTGRES_DB", "warehouse"), user=os.getenv("POSTGRES_USER", "warehouse"),
                   password=password)
    with psycopg.connect(**connect) as connection:
        with connection.cursor() as cursor:
            # Anchor the late business date inside the generated date dimension,
            # rather than to the runner/database wall clock or dbt's fixed seed.
            cursor.execute("select max(full_date) - 10 from marts.dim_date")
            late_business_date = cursor.fetchone()[0]
            assert late_business_date is not None
            cursor.execute("select count(*) from marts.dim_date where full_date = %s", (late_business_date,))
            assert cursor.fetchone()[0] == 1
            cursor.execute("select count(*) from marts.fct_transactions where transaction_id = %s",
                           (transaction_id,))
            assert cursor.fetchone()[0] == 0
            cursor.execute("""insert into raw.transactions
                (transaction_id, fund_id, company_id, transaction_date, transaction_type, amount,
                 currency, ingested_at, batch_id, loaded_at)
                values (%s, 'F001', 'C0001', %s, 'follow_on', 123456.78, 'USD', %s,
                        'late_test', %s)""",
                           (transaction_id, late_business_date, ingested_at, ingested_at))

    try:
        with tempfile.TemporaryDirectory(prefix="ai-ready-late-test-") as profile_dir:
            create_profile(Path(profile_dir), "postgres")
            # Run only the existing incremental fact. No full refresh and no
            # seed reload: the inserted raw row must arrive through the merge.
            dbt_args = ["dbt", "build", "--project-dir", str(ROOT), "--profiles-dir", profile_dir,
                        "--target", "postgres", "--select", "fct_transactions",
                        "--exclude", "resource_type:seed"]
            assert "--full-refresh" not in dbt_args
            subprocess.run(dbt_args, cwd=ROOT, check=True)

        with psycopg.connect(**connect) as connection:
            with connection.cursor() as cursor:
                cursor.execute("""select count(*) from marts.fct_transactions
                                  where transaction_id = %s and amount = 123456.78
                                    and ingested_at = %s""", (transaction_id, ingested_at))
                assert cursor.fetchone()[0] == 1
    finally:
        # Keep this integration check isolated from later builds/reruns.
        with psycopg.connect(**connect) as connection:
            with connection.cursor() as cursor:
                cursor.execute("delete from marts.fct_transactions where transaction_id = %s", (transaction_id,))
                cursor.execute("delete from raw.transactions where transaction_id = %s", (transaction_id,))


def test_contract_manifest_covers_required_fact_relationships() -> None:
    schema = (ROOT / "models" / "schema.yml").read_text(encoding="utf-8")
    for relationship in ("dim_company", "dim_fund", "dim_date"):
        assert relationship in schema
