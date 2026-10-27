from __future__ import annotations

import csv
from datetime import date, datetime, timezone
from pathlib import Path

import pytest

from generator.generate import generate, promote_changed_company_version


@pytest.mark.parametrize("seed", [0, 1, 42, 20261027])
def test_same_seed_reproduces_identical_files(tmp_path: Path, seed: int) -> None:
    left, right = tmp_path / "left", tmp_path / "right"
    generate(left, seed=seed, days=4, companies_count=12, as_of=date(2026, 10, 27))
    generate(right, seed=seed, days=4, companies_count=12, as_of=date(2026, 10, 27))
    assert {p.name: p.read_bytes() for p in left.glob("*")} == {p.name: p.read_bytes() for p in right.glob("*")}


@pytest.mark.parametrize("days,companies", [(1, 8), (2, 9), (5, 20), (30, 40)])
def test_generator_writes_all_source_tables(tmp_path: Path, days: int, companies: int) -> None:
    counts = generate(tmp_path, seed=2, days=days, companies_count=companies, as_of=date(2026, 10, 27))
    assert set(counts) == {"funds", "companies", "holdings", "transactions", "valuations", "company_kpis", "company_notes"}
    assert counts["funds"] == 8
    assert counts["companies"] == companies + 1  # changed attribute version
    for name in counts:
        assert (tmp_path / f"{name}.csv").is_file()


@pytest.mark.parametrize("bad_days,bad_companies", [(0, 8), (-1, 8), (1, 0), (1, 7)])
def test_rejects_invalid_sizes(tmp_path: Path, bad_days: int, bad_companies: int) -> None:
    with pytest.raises(ValueError):
        generate(tmp_path, days=bad_days, companies_count=bad_companies)


def test_includes_duplicate_and_orphan_defects(tmp_path: Path) -> None:
    generate(tmp_path, days=2, companies_count=12, as_of=date(2026, 10, 27))
    with (tmp_path / "holdings.csv").open(encoding="utf-8", newline="") as handle:
        holdings = list(csv.DictReader(handle))
    ids = [row["holding_id"] for row in holdings]
    assert len(ids) > len(set(ids))
    assert any(row["company_id"] == "C999999" for row in holdings)


def test_includes_late_arriving_transaction(tmp_path: Path) -> None:
    generate(tmp_path, days=3, companies_count=8, as_of=date(2026, 10, 27))
    with (tmp_path / "transactions.csv").open(encoding="utf-8", newline="") as handle:
        tx = next(row for row in csv.DictReader(handle) if row["batch_id"] == "late_batch")
    assert tx["ingested_at"] > tx["transaction_date"]


def test_includes_changed_company_version(tmp_path: Path) -> None:
    generate(tmp_path, days=1, companies_count=8, as_of=date(2026, 10, 27))
    with (tmp_path / "companies.csv").open(encoding="utf-8", newline="") as handle:
        rows = [row for row in csv.DictReader(handle) if row["company_id"] == "C0001"]
    assert len(rows) == 2
    assert len({row["sector"] for row in rows}) == 2


def test_changed_company_version_is_future_dated_and_promotable(tmp_path: Path) -> None:
    as_of = date(2030, 4, 12)
    generate(tmp_path, days=1, companies_count=8, as_of=as_of)
    path = tmp_path / "companies.csv"
    with path.open(newline="", encoding="utf-8") as stream:
        rows = [row for row in csv.DictReader(stream) if row["company_id"] == "C0001"]
    changed = max(rows, key=lambda row: row["ingested_at"])
    assert datetime.fromisoformat(changed["ingested_at"]).date() == date(2030, 4, 13)
    promoted_at = datetime(2030, 4, 13, 12, tzinfo=timezone.utc)

    promote_changed_company_version(path, promoted_at=promoted_at)

    with path.open(newline="", encoding="utf-8") as stream:
        promoted_rows = [row for row in csv.DictReader(stream) if row["company_id"] == "C0001"]
    promoted = max(promoted_rows, key=lambda row: row["ingested_at"])
    assert promoted["sector"] == "business_services"
    assert datetime.fromisoformat(promoted["ingested_at"]) == promoted_at

