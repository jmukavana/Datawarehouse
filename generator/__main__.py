"""CLI: python -m generator --start-date 2026-01-05 --days 90

First run bootstraps the world and writes an initial full extract; later runs
resume from the persisted state file and append daily incremental batches.
"""

import argparse
import json
from collections import Counter
from datetime import date, timedelta
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from .engine import Engine, new_state

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_DATA_DIR = PROJECT_ROOT / "data" / "raw"
DEFAULT_STATE_FILE = PROJECT_ROOT / "data" / "generator_state.json"


def write_batch(data_dir: Path, source: str, load_date: date, tables: dict) -> Counter:
    counts = Counter()
    for name, rows in tables.items():
        if not rows:
            continue
        target = data_dir / source / load_date.isoformat()
        target.mkdir(parents=True, exist_ok=True)
        pq.write_table(pa.Table.from_pylist(rows), target / f"{name}.parquet")
        counts[f"{source}.{name}"] = len(rows)
    return counts


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate synthetic bancassurance source data")
    parser.add_argument("--start-date", type=date.fromisoformat, default=date(2026, 1, 5),
                        help="Simulation start (used on first run only)")
    parser.add_argument("--days", type=int, default=1, help="Number of days to generate")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--data-dir", type=Path, default=DEFAULT_DATA_DIR)
    parser.add_argument("--state-file", type=Path, default=DEFAULT_STATE_FILE)
    args = parser.parse_args()

    if args.state_file.exists():
        state = json.loads(args.state_file.read_text())
        first_day = date.fromisoformat(state["last_date"]) + timedelta(days=1)
    else:
        state = new_state(args.seed, args.start_date)
        first_day = args.start_date

    engine = Engine(state)
    totals = Counter()
    for offset in range(args.days):
        day = first_day + timedelta(days=offset)
        batch = engine.simulate_day(day)
        for source, tables in batch.items():
            totals += write_batch(args.data_dir, source, day, tables)

    args.state_file.parent.mkdir(parents=True, exist_ok=True)
    args.state_file.write_text(json.dumps(state))

    last_day = first_day + timedelta(days=args.days - 1)
    print(f"Generated {first_day} .. {last_day} into {args.data_dir}")
    for table, count in sorted(totals.items()):
        print(f"  {table:45s} {count:7d} rows")


if __name__ == "__main__":
    main()
