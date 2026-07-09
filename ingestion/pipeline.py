"""dlt ingestion: data/raw/<source>/<load_date>/<table>.parquet -> DuckDB raw schemas.

Each source system becomes its own dlt pipeline and DuckDB schema
(raw_core_insurance, raw_mpesa_gateway, raw_bank_payments). Incremental
loading is keyed on the partition load_date via dlt.sources.incremental, so
re-running only picks up new partitions. Schema evolution (the paybill_ref
column appearing mid-history in the M-Pesa feed) is absorbed by dlt.
"""

from pathlib import Path

import dlt
import pyarrow.parquet as pq

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data" / "raw"
WAREHOUSE_PATH = PROJECT_ROOT / "warehouse.duckdb"
PIPELINES_DIR = PROJECT_ROOT / "data" / ".dlt_pipelines"

SOURCES = ["core_insurance", "mpesa_gateway", "bank_payments"]

# Claim measures are null until the relevant status event occurs; without hints
# dlt would skip materializing an all-null column, leaving the raw table shape
# dependent on which events happened to be in the first batch.
COLUMN_HINTS: dict[str, dict] = {
    "claims": {
        "claimed_amount": {"data_type": "bigint", "nullable": True},
        "assessed_amount": {"data_type": "bigint", "nullable": True},
        "paid_amount": {"data_type": "bigint", "nullable": True},
    },
    "customers": {
        "national_id": {"data_type": "text", "nullable": True},
    },
}


def _discover_tables(source_dir: Path) -> list[str]:
    return sorted({f.stem for part in source_dir.iterdir() if part.is_dir() for f in part.glob("*.parquet")})


def _table_resource(source_dir: Path, table_name: str):
    @dlt.resource(name=table_name, write_disposition="append", columns=COLUMN_HINTS.get(table_name))
    def rows(
        load_date=dlt.sources.incremental("_load_date", initial_value=""),
    ):
        for partition in sorted(source_dir.iterdir()):
            if not partition.is_dir() or partition.name <= (load_date.start_value or ""):
                continue
            parquet_file = partition / f"{table_name}.parquet"
            if not parquet_file.exists():
                continue
            for row in pq.read_table(parquet_file).to_pylist():
                row["_load_date"] = partition.name
                yield row

    return rows


def run(
    data_dir: Path = DATA_DIR,
    warehouse_path: Path = WAREHOUSE_PATH,
    pipelines_dir: Path = PIPELINES_DIR,
) -> dict[str, int]:
    """Load all new partitions for every source; returns rows loaded per table."""
    loaded: dict[str, int] = {}
    for source in SOURCES:
        source_dir = data_dir / source
        if not source_dir.exists():
            continue
        pipeline = dlt.pipeline(
            pipeline_name=f"raw_{source}",
            destination=dlt.destinations.duckdb(str(warehouse_path)),
            dataset_name=f"raw_{source}",
            pipelines_dir=str(pipelines_dir),
        )
        resources = [_table_resource(source_dir, t) for t in _discover_tables(source_dir)]
        info = pipeline.run(resources)
        for package in info.load_packages:
            for job in package.jobs.get("completed_jobs", []):
                table = job.job_file_info.table_name
                if not table.startswith("_dlt"):
                    loaded[f"{source}.{table}"] = loaded.get(f"{source}.{table}", 0)
        # Row counts from dlt's normalize metrics are fiddly; report via trace.
        trace = pipeline.last_trace
        if trace and trace.last_normalize_info:
            for table, count in trace.last_normalize_info.row_counts.items():
                if not table.startswith("_dlt"):
                    loaded[f"{source}.{table}"] = count
    return loaded


if __name__ == "__main__":
    counts = run()
    if not counts:
        print("No new partitions to load.")
    for table, count in sorted(counts.items()):
        print(f"  {table:45s} {count:7d} rows")
