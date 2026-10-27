"""Local orchestration: generate -> seed -> snapshot builds -> append SCD2 change -> verify idempotency."""
from __future__ import annotations

import argparse
import csv
import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    # Also support `python pipeline/run.py` from a clean checkout, where Python
    # otherwise puts only the pipeline/ directory at the front of sys.path.
    sys.path.insert(0, str(ROOT))

from pipeline.dbt_commands import build_dbt_command
from generator.generate import baseline_company_seed


def create_profile(directory: Path, target: str) -> Path:
    """Write a throwaway dbt profile outside the repository from environment variables."""
    directory.mkdir(parents=True, exist_ok=True)
    db = os.getenv("POSTGRES_DB", "warehouse")
    user = os.getenv("POSTGRES_USER", "warehouse")
    password = os.getenv("POSTGRES_PASSWORD")
    if not password:
        raise ValueError("Set POSTGRES_PASSWORD in the environment before creating a dbt profile")
    host = os.getenv("POSTGRES_HOST", "localhost")
    port = int(os.getenv("POSTGRES_PORT", "5432"))
    content = f'''ai_ready_data_warehouse:\n  target: {target}\n  outputs:\n    {target}:\n      type: postgres\n      host: {json.dumps(host)}\n      user: {json.dumps(user)}\n      password: {json.dumps(password)}\n      port: {port}\n      dbname: {json.dumps(db)}\n      schema: warehouse\n      threads: 1\n      connect_timeout: 10\n'''
    path = directory / "profiles.yml"
    path.write_text(content, encoding="utf-8")
    return path


def run_command(args: list[str], *, env: dict[str, str] | None = None, cwd: Path = ROOT) -> None:
    print("+", " ".join(args), flush=True)
    subprocess.run(args, cwd=cwd, env=env, check=True)


def mart_fingerprints() -> dict[str, tuple[int, str]]:
    """Count and checksum both facts using the exact database contents."""
    try:
        import psycopg  # type: ignore
    except ImportError as exc:
        raise RuntimeError("Install requirements.txt before running the pipeline") from exc
    password = os.getenv("POSTGRES_PASSWORD")
    if not password:
        raise RuntimeError("Set POSTGRES_PASSWORD in the environment before connecting to PostgreSQL")
    conn = psycopg.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"), port=int(os.getenv("POSTGRES_PORT", "5432")),
        dbname=os.getenv("POSTGRES_DB", "warehouse"), user=os.getenv("POSTGRES_USER", "warehouse"),
        password=password,
    )
    results: dict[str, tuple[int, str]] = {}
    with conn:
        with conn.cursor() as cur:
            for table in ("fct_holdings_daily", "fct_transactions"):
                cur.execute(f'''select count(*), coalesce(md5(string_agg(md5(to_jsonb(t)::text), '' order by to_jsonb(t)::text)), md5(''))
                                from marts.{table} t''')
                count, checksum = cur.fetchone()
                results[table] = (count, checksum)
    conn.close()
    return results


def verify_scd2_version(company_id: str = "C0001") -> tuple[int, int]:
    """Require the generated changed company to have two versions and one current row."""
    try:
        import psycopg  # type: ignore
    except ImportError as exc:
        raise RuntimeError("Install requirements.txt before running the pipeline") from exc
    password = os.getenv("POSTGRES_PASSWORD")
    if not password:
        raise RuntimeError("Set POSTGRES_PASSWORD in the environment before connecting to PostgreSQL")
    with psycopg.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"), port=int(os.getenv("POSTGRES_PORT", "5432")),
        dbname=os.getenv("POSTGRES_DB", "warehouse"), user=os.getenv("POSTGRES_USER", "warehouse"),
        password=password,
    ) as conn:
        with conn.cursor() as cur:
            cur.execute("""select count(*), count(*) filter (where dbt_valid_to is null)
                           from snapshots.company_history where company_id = %s""", (company_id,))
            versions, current_versions = cur.fetchone()
    if versions < 2 or current_versions != 1:
        raise RuntimeError(f"SCD2 check failed for {company_id}: versions={versions}, current={current_versions}")
    print(f"SCD2 version check passed: {company_id} versions={versions}, current={current_versions}")
    return int(versions), int(current_versions)


def append_changed_company_version(companies_csv: Path, company_id: str = "C0001") -> None:
    """Append the promoted company version to raw without replacing prior raw rows."""
    from generator.generate import normalize_integer_columns

    path = Path(companies_csv)
    with path.open("r", newline="", encoding="utf-8") as stream:
        rows = [row for row in csv.DictReader(stream) if row.get("company_id") == company_id]
    if len(rows) < 2:
        raise RuntimeError(f"Expected at least two generated versions for {company_id} in {path}")
    changed = normalize_integer_columns("companies", max(rows, key=lambda row: row["ingested_at"]))
    baseline = min(rows, key=lambda row: row["ingested_at"])
    if changed["sector"] == baseline["sector"]:
        raise RuntimeError(f"Generated {company_id} version does not change sector")
    changed_at = datetime.fromisoformat(changed["ingested_at"])
    baseline_at = datetime.fromisoformat(baseline["ingested_at"])
    if changed_at.tzinfo is None:
        changed_at = changed_at.replace(tzinfo=timezone.utc)
    else:
        changed_at = changed_at.astimezone(timezone.utc)
    if baseline_at.tzinfo is None:
        baseline_at = baseline_at.replace(tzinfo=timezone.utc)
    else:
        baseline_at = baseline_at.astimezone(timezone.utc)
    if changed_at <= baseline_at:
        raise RuntimeError(f"Generated {company_id} ingestion time is not newer than baseline")

    try:
        import psycopg  # type: ignore
    except ImportError as exc:
        raise RuntimeError("Install requirements.txt before appending the changed company row") from exc
    password = os.getenv("POSTGRES_PASSWORD")
    if not password:
        raise RuntimeError("Set POSTGRES_PASSWORD in the environment before connecting to PostgreSQL")
    loaded_at = datetime.fromisoformat(changed["loaded_at"])
    if loaded_at.tzinfo is None:
        loaded_at = loaded_at.replace(tzinfo=timezone.utc)
    else:
        loaded_at = loaded_at.astimezone(timezone.utc)
    with psycopg.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"), port=int(os.getenv("POSTGRES_PORT", "5432")),
        dbname=os.getenv("POSTGRES_DB", "warehouse"), user=os.getenv("POSTGRES_USER", "warehouse"),
        password=password,
    ) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """insert into raw.companies
                   (company_id, company_name, sector, country_code, parent_group_id,
                    employee_count, annual_revenue, classification, owner, ingested_at, loaded_at)
                   values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                (
                    changed["company_id"], changed["company_name"], changed["sector"],
                    changed["country_code"], changed["parent_group_id"] or None,
                    int(changed["employee_count"]), int(changed["annual_revenue"]),
                    changed["classification"], changed["owner"], changed_at,
                    loaded_at,
                ),
            )
    print(f"Appended raw company version: {company_id} sector={changed['sector']} ingested_at={changed['ingested_at']}")


def wait_for_postgres(timeout_seconds: int = 60) -> None:
    """Wait for the configured PostgreSQL service to accept connections."""
    try:
        import psycopg  # type: ignore
    except ImportError as exc:
        raise RuntimeError("Install requirements.txt before running the pipeline") from exc
    password = os.getenv("POSTGRES_PASSWORD")
    if not password:
        raise RuntimeError("Set POSTGRES_PASSWORD in the environment before connecting to PostgreSQL")
    deadline = time.monotonic() + timeout_seconds
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        try:
            with psycopg.connect(
                host=os.getenv("POSTGRES_HOST", "localhost"), port=int(os.getenv("POSTGRES_PORT", "5432")),
                dbname=os.getenv("POSTGRES_DB", "warehouse"), user=os.getenv("POSTGRES_USER", "warehouse"),
                password=password, connect_timeout=3,
            ):
                return
        except psycopg.OperationalError as exc:
            last_error = exc
            time.sleep(2)
    raise RuntimeError(f"PostgreSQL did not become ready within {timeout_seconds}s: {last_error}")


def run_pipeline(*, target: str = "postgres", days: int = 30, companies: int = 400,
                 seed: int = 20261027, embeddings: str = "test", skip_embeddings: bool = False,
                 dbt_executable: str = "dbt") -> None:
    if target != "postgres":
        raise ValueError("Phase 1 supports only --target postgres")
    from generator.generate import generate, promote_changed_company_version
    data_dir = ROOT / "data" / "generated"
    generate(data_dir, seed=seed, days=days, companies_count=companies)
    # The profile lives in the OS temp directory, never under the repository.
    profile_dir = Path(tempfile.mkdtemp(prefix="ai-ready-dbt-profile-"))
    try:
        create_profile(profile_dir, target)
        wait_for_postgres()
        env = os.environ.copy()
        env["DBT_PROFILES_DIR"] = str(profile_dir)
        # Raw relations are declared as sources, so their freshness metadata
        # does not create DAG edges from the dbt seed resources. Load them first.
        # A generated future-dated C0001 version is not raw until explicitly
        # promoted after the baseline snapshot. Seed the arrived baseline only.
        with baseline_company_seed(data_dir / "companies.csv"):
            run_command(build_dbt_command("seed", "--full-refresh", project_dir=ROOT,
                                          profiles_dir=profile_dir, target=target,
                                          executable=dbt_executable), env=env)
        run_command(build_dbt_command("source freshness", project_dir=ROOT,
                                      profiles_dir=profile_dir, target=target,
                                      executable=dbt_executable), env=env)
        # Keep seeds out of every build: sources are not seed dependencies, and
        # the explicit initial load ensures staging has raw relations to read.
        run_command(build_dbt_command("build", "--exclude", "resource_type:seed", project_dir=ROOT,
                                      profiles_dir=profile_dir, target=target,
                                      executable=dbt_executable), env=env)
        # Expose the generated change as a new raw event. Raw is append-only;
        # the following build rebuilds latest-wins staging and advances SCD2.
        promote_changed_company_version(data_dir / "companies.csv")
        append_changed_company_version(data_dir / "companies.csv")
        run_command(build_dbt_command("build", "--exclude", "resource_type:seed", project_dir=ROOT,
                                      profiles_dir=profile_dir, target=target,
                                      executable=dbt_executable), env=env)
        verify_scd2_version()
        first_result = mart_fingerprints()
        # Compare the settled SCD2 build with one more build for idempotency.
        run_command(build_dbt_command("build", "--exclude", "resource_type:seed", project_dir=ROOT,
                                      profiles_dir=profile_dir, target=target,
                                      executable=dbt_executable), env=env)
        second_result = mart_fingerprints()
        if first_result != second_result:
            raise RuntimeError(f"Idempotency check failed; first={first_result}, second={second_result}")
        table_names = ", ".join(first_result)
        print(f"Idempotency check passed: {table_names} row counts and checksums identical")
        run_command(build_dbt_command("docs generate", project_dir=ROOT,
                                      profiles_dir=profile_dir, target=target,
                                      executable=dbt_executable), env=env)
        if not skip_embeddings:
            from pipeline.vector_search import populate_embeddings
            n = populate_embeddings(embeddings)
            print(f"Updated {n} AI text chunk embeddings using provider={embeddings!r}.")
    finally:
        shutil.rmtree(profile_dir, ignore_errors=True)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--target", choices=("postgres",), default="postgres")
    parser.add_argument("--days", type=int, default=30)
    parser.add_argument("--companies", type=int, default=400)
    parser.add_argument("--seed", type=int, default=20261027)
    parser.add_argument("--embeddings", choices=("test", "openai"), default="test")
    parser.add_argument("--skip-embeddings", action="store_true")
    parser.add_argument("--dbt-executable", default="dbt")
    args = parser.parse_args(argv)
    try:
        run_pipeline(target=args.target, days=args.days, companies=args.companies, seed=args.seed,
                     embeddings=args.embeddings, skip_embeddings=args.skip_embeddings,
                     dbt_executable=args.dbt_executable)
    except (subprocess.CalledProcessError, OSError, RuntimeError, ValueError) as exc:
        print(f"Pipeline failed: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
