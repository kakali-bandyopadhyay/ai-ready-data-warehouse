"""Generate reproducible synthetic private-markets source files.

All records are fictional. Defaults are intentionally small for a local/CI run.
"""
from __future__ import annotations

import argparse
import csv
import json
import random
from contextlib import contextmanager
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Iterable, Iterator

FUNDS = 8
COMPANIES = 400
COMPANY_PREFIXES = ("Northstar", "Juniper", "Cedar", "Bluefield", "Silverline", "Harbor", "Pioneer", "Redwood")
SECTORS = ("software", "healthcare", "industrials", "consumer", "business_services", "energy")
INTEGER_COLUMNS = {
    "funds": ("vintage_year",),
    # Annual revenue is whole-dollar synthetic data and is serialized as an
    # integer even though warehouse schemas store it in a decimal column.
    "companies": ("employee_count", "annual_revenue"),
}


def normalize_integer_columns(table: str, row: dict) -> dict:
    """Return a row with integral numeric fields represented as Python ints."""
    normalized = dict(row)
    for column in INTEGER_COLUMNS.get(table, ()):
        value = normalized.get(column)
        if value in (None, ""):
            continue
        try:
            numeric = Decimal(str(value))
        except InvalidOperation as exc:
            raise ValueError(f"{table}.{column} must contain an integer value: {value!r}") from exc
        if not numeric.is_finite() or numeric != numeric.to_integral_value():
            raise ValueError(f"{table}.{column} must contain an integer value: {value!r}")
        normalized[column] = int(numeric)
    return normalized


def _parse_utc(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def write_baseline_company_seed(source: Path, destination: Path) -> int:
    """Write companies that have arrived by now, withholding future-dated test changes."""
    with Path(source).open("r", newline="", encoding="utf-8") as stream:
        reader = csv.DictReader(stream)
        fieldnames = reader.fieldnames
        rows = list(reader)
    if not fieldnames or "ingested_at" not in fieldnames:
        raise ValueError(f"{source} is not a generated companies CSV")
    now_utc = datetime.now(timezone.utc)
    arrived = [row for row in rows if _parse_utc(row["ingested_at"]) <= now_utc]
    if not arrived:
        raise ValueError(f"{source} contains no company rows ingested by the current UTC time")
    destination = Path(destination)
    destination.parent.mkdir(parents=True, exist_ok=True)
    with destination.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(normalize_integer_columns("companies", row) for row in arrived)
    return len(arrived)


@contextmanager
def baseline_company_seed(path: Path) -> Iterator[None]:
    """Temporarily hide future-dated company rows while dbt loads initial raw seeds."""
    path = Path(path)
    original = path.read_bytes()
    try:
        write_baseline_company_seed(path, path)
        yield
    finally:
        path.write_bytes(original)


def _write_csv(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        return
    with path.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(normalize_integer_columns(path.stem, row) for row in rows)


def generate(output_dir: Path, *, seed: int = 20261027, days: int = 30,
             companies_count: int = COMPANIES, as_of: date | None = None) -> dict[str, int]:
    """Write CSV inputs, with deterministic intentional data-quality defects."""
    if days < 1 or companies_count < 8:
        raise ValueError("days must be positive and companies_count must be at least 8")
    rng = random.Random(seed)
    as_of = as_of or datetime.now(timezone.utc).date()
    # All generated ingestion/load timestamps are UTC-aware ISO-8601 values.
    loaded_at = datetime.combine(as_of, datetime.min.time(), tzinfo=timezone.utc).isoformat()
    company_baseline = datetime.combine(as_of, datetime.min.time(), tzinfo=timezone.utc).isoformat()
    out = Path(output_dir)

    funds = [{"fund_id": f"F{i:03d}", "fund_name": f"Synthetic Fund {i:02d}",
              "base_currency": "USD", "vintage_year": 2016 + i % 9,
              "fund_nav": round(1_000_000_000 + i * 85_000_000, 2), "ingested_at": loaded_at}
             for i in range(1, FUNDS + 1)]
    companies = []
    for i in range(1, companies_count + 1):
        parent = f"G{i // 4:03d}" if i % 4 else ""
        companies.append({"company_id": f"C{i:04d}", "company_name": f"{COMPANY_PREFIXES[i % len(COMPANY_PREFIXES)]} Company {i:04d}",
                          "sector": SECTORS[i % len(SECTORS)], "country_code": ("US", "GB", "DE", "CA")[i % 4],
                          "parent_group_id": parent, "employee_count": 80 + (i * 37) % 5000,
                          "annual_revenue": 5_000_000 + i * 372_000,
                          "classification": "synthetic_public", "owner": "data_platform", "ingested_at": company_baseline})
    # Changed attribute is supplied as a later source version for snapshot/SCD2 checks.
    changed = dict(companies[0]); changed["sector"] = "business_services"
    changed["ingested_at"] = datetime.combine(as_of + timedelta(days=1), datetime.min.time(), tzinfo=timezone.utc).isoformat()
    company_source = companies + [changed]

    dates = [as_of - timedelta(days=x) for x in range(days - 1, -1, -1)]
    holdings = []
    for day in dates:
        for company in companies:
            if rng.random() < .16:
                fund = funds[rng.randrange(len(funds))]
                units = round(rng.uniform(10_000, 200_000), 4)
                price = round(rng.uniform(15, 180), 4)
                holdings.append({"holding_id": f"H{day:%Y%m%d}{company['company_id']}", "fund_id": fund["fund_id"],
                                 "company_id": company["company_id"], "as_of_date": day.isoformat(), "units": units,
                                 "price": price, "market_value": round(units * price, 2), "ingested_at": loaded_at})
    if not holdings:
        company, fund, day = companies[0], funds[0], dates[0]
        holdings.append({"holding_id": f"H{day:%Y%m%d}{company['company_id']}", "fund_id": fund["fund_id"],
                         "company_id": company["company_id"], "as_of_date": day.isoformat(), "units": 1000,
                         "price": 50, "market_value": 50000, "ingested_at": loaded_at})
    # Duplicate row and orphan key deliberately exercise raw-quality handling.
    if holdings:
        holdings.append(dict(holdings[0]))
        orphan = dict(holdings[-2]); orphan.update(holding_id="HORPHAN", company_id="C999999")
        holdings.append(orphan)
    # Make the latest daily position value the synthetic fund NAV, so the
    # reconciliation test measures agreement rather than a hard-coded cap.
    valid_companies = {company["company_id"] for company in companies}
    latest_positions = {row["holding_id"]: row for row in holdings
                        if row["as_of_date"] == dates[-1].isoformat() and row["company_id"] in valid_companies}
    nav_by_fund: dict[str, float] = {}
    for row in latest_positions.values():
        nav_by_fund[row["fund_id"]] = nav_by_fund.get(row["fund_id"], 0.0) + float(row["market_value"])
    for fund in funds:
        fund["fund_nav"] = round(max(nav_by_fund.get(fund["fund_id"], 0.0), 1_000_000.0), 2)

    transactions = []
    for i in range(max(100, companies_count * 2)):
        day = dates[rng.randrange(len(dates))]
        transactions.append({"transaction_id": f"T{i:06d}", "fund_id": funds[rng.randrange(len(funds))]["fund_id"],
                             "company_id": companies[rng.randrange(len(companies))]["company_id"],
                             "transaction_date": day.isoformat(), "transaction_type": ("investment", "realization", "follow_on")[i % 3],
                             "amount": round(rng.uniform(100_000, 5_000_000), 2), "currency": "USD",
                             "ingested_at": (datetime.combine(day, datetime.min.time(), tzinfo=timezone.utc) + (timedelta(days=2) if i == 0 else timedelta())).isoformat(),
                             "batch_id": "late_batch" if i == 0 else "initial"})
    valuations = [{"valuation_id": f"V{c['company_id']}{d:%Y%m%d}", "company_id": c["company_id"],
                   "valuation_date": d.isoformat(), "enterprise_value": round(float(c["annual_revenue"]) * rng.uniform(2, 8), 2),
                   "ingested_at": loaded_at} for c in companies for d in dates[::max(1, days // 4)]]
    kpis = [{"kpi_id": f"K{c['company_id']}", "company_id": c["company_id"], "as_of_date": as_of.isoformat(),
             "revenue_growth_pct": round(rng.uniform(-.1, .45), 4), "ebitda_margin_pct": round(rng.uniform(.05, .4), 4),
             "ingested_at": loaded_at} for c in companies]
    notes = [{"note_id": f"N{c['company_id']}", "company_id": c["company_id"],
              "note_text": f"Synthetic company note: {c['company_name']} operates in {c['sector']} in {c['country_code']}. Revenue and operating trends are illustrative only.",
              "source_name": "synthetic_generator", "as_of_date": as_of.isoformat(), "owner": "data_platform",
              "classification": "synthetic_public", "ingested_at": loaded_at} for c in companies]
    # An orphan source record remains raw and is excluded by staging joins.
    notes.append({"note_id": "NORPHAN", "company_id": "C999999", "note_text": "Synthetic orphan record for data-quality testing.",
                  "source_name": "synthetic_generator", "as_of_date": as_of.isoformat(), "owner": "data_platform",
                  "classification": "synthetic_public", "ingested_at": loaded_at})

    records = {"funds": funds, "companies": company_source, "holdings": holdings,
               "transactions": transactions, "valuations": valuations, "company_kpis": kpis, "company_notes": notes}
    for name, rows in records.items():
        for row in rows:
            row["loaded_at"] = loaded_at
        _write_csv(out / f"{name}.csv", rows)
    return {name: len(rows) for name, rows in records.items()}


def promote_changed_company_version(companies_csv: Path, *, company_id: str = "C0001",
                                    promoted_at: datetime | None = None) -> None:
    """Make the generated future-dated company change visible to the next load."""
    path = Path(companies_csv)
    with path.open("r", newline="", encoding="utf-8") as stream:
        reader = csv.DictReader(stream)
        fieldnames = reader.fieldnames
        rows = list(reader)
    if not fieldnames or "company_id" not in fieldnames or "ingested_at" not in fieldnames:
        raise ValueError(f"{path} is not a generated companies CSV")
    versions = [row for row in rows if row["company_id"] == company_id]
    if len(versions) < 2:
        raise ValueError(f"Expected a baseline and changed version for {company_id} in {path}")
    baseline = min(versions, key=lambda row: row["ingested_at"])
    changed = max(versions, key=lambda row: row["ingested_at"])
    if baseline["sector"] == changed["sector"]:
        raise ValueError(f"Generated company version for {company_id} has no sector change")
    promoted_at = promoted_at or datetime.now(timezone.utc)
    promoted_at = promoted_at.astimezone(timezone.utc)
    if promoted_at.tzinfo is None:
        promoted_at = promoted_at.replace(tzinfo=timezone.utc)
    else:
        promoted_at = promoted_at.astimezone(timezone.utc)
    if promoted_at <= _parse_utc(baseline["ingested_at"]):
        raise ValueError(f"Promoted timestamp must be later than the baseline for {company_id}")
    changed["ingested_at"] = promoted_at.isoformat()
    temporary_path = path.with_suffix(path.suffix + ".tmp")
    try:
        with temporary_path.open("w", newline="", encoding="utf-8") as stream:
            writer = csv.DictWriter(stream, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(normalize_integer_columns("companies", row) for row in rows)
        temporary_path.replace(path)
    finally:
        temporary_path.unlink(missing_ok=True)


def main(argv: Iterable[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=Path("data/generated"))
    parser.add_argument("--seed", type=int, default=20261027)
    parser.add_argument("--days", type=int, default=30)
    parser.add_argument("--companies", type=int, default=COMPANIES)
    parser.add_argument("--as-of", type=date.fromisoformat, default=None, help="ISO date; defaults to current UTC date")
    parser.add_argument("--format", choices=("csv", "parquet", "both"), default="csv")
    args = parser.parse_args(argv)
    counts = generate(args.output, seed=args.seed, days=args.days, companies_count=args.companies, as_of=args.as_of)
    if args.format in ("parquet", "both"):
        try:
            import pyarrow as pa  # type: ignore
            import pyarrow.csv as pacsv  # type: ignore
            import pyarrow.parquet as pq  # type: ignore
        except ImportError as exc:
            raise SystemExit("Parquet output requires the official pyarrow package. Install it with: python -m pip install pyarrow") from exc
        for name in counts:
            table = pacsv.read_csv(args.output / f"{name}.csv")
            pq.write_table(table, args.output / f"{name}.parquet")
    print(json.dumps(counts, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
