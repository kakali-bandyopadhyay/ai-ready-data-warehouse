"""Local orchestration: generate -> seed -> build snapshot -> reload changed seed -> verify idempotency."""
from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    # Also support `python pipeline/run.py` from a clean checkout, where Python
    # otherwise puts only the pipeline/ directory at the front of sys.path.
    sys.path.insert(0, str(ROOT))


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


def build_dbt_command(command: str, profile_dir: Path, target: str, *args: str,
                      executable: str = "dbt") -> list[str]:
    """Build a dbt argv list with the command chain before its global options."""
    return [executable, *command.split(), "--project-dir", str(ROOT), "--profiles-dir", str(profile_dir),
            "--target", target, *args]


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
        run_command(build_dbt_command("seed", profile_dir, target, "--full-refresh",
                                      executable=dbt_executable), env=env)
        run_command(build_dbt_command("source freshness", profile_dir, target,
                                      executable=dbt_executable), env=env)
        # Keep seeds out of every build: sources are not seed dependencies, and
        # the explicit initial load ensures staging has raw relations to read.
        run_command(build_dbt_command("build", profile_dir, target, "--exclude", "resource_type:seed",
                                      executable=dbt_executable), env=env)
        # Expose the generated attribute change and reload raw seeds.
        # Reload only the changed seed without full refresh. dbt truncates and
        # inserts changed seed contents, retaining dependent views. The build
        # below rebuilds staging and advances the snapshot in DAG order.
        promote_changed_company_version(data_dir / "companies.csv")
        run_command(build_dbt_command("seed", profile_dir, target, "--select", "companies",
                                      executable=dbt_executable), env=env)
        run_command(build_dbt_command("build", profile_dir, target, "--exclude", "resource_type:seed",
                                      executable=dbt_executable), env=env)
        verify_scd2_version()
        first_result = mart_fingerprints()
        # Compare the settled SCD2 build with one more build for idempotency.
        run_command(build_dbt_command("build", profile_dir, target, "--exclude", "resource_type:seed",
                                      executable=dbt_executable), env=env)
        second_result = mart_fingerprints()
        if first_result != second_result:
            raise RuntimeError(f"Idempotency check failed; first={first_result}, second={second_result}")
        table_names = ", ".join(first_result)
        print(f"Idempotency check passed: {table_names} row counts and checksums identical")
        run_command(build_dbt_command("docs generate", profile_dir, target,
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
