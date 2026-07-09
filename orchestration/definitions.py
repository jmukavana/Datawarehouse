"""Dagster definitions: generate -> ingest -> dbt build, on a daily schedule.

Every dbt model/snapshot/seed appears as its own asset in the lineage graph;
dbt tests run as part of `dbt build`, so a failing test fails the run — the
data-quality gate.
"""

import subprocess
import sys
from pathlib import Path

import dagster as dg
from dagster_dbt import DagsterDbtTranslator, DbtCliResource, DbtProject, dbt_assets

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

dbt_project = DbtProject(project_dir=PROJECT_ROOT / "dbt_project")
dbt_project.prepare_if_dev()


@dg.asset(compute_kind="python", group_name="ingestion")
def raw_source_batch(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """Generate the next day's synthetic source extracts into data/raw/."""
    result = subprocess.run(
        [sys.executable, "-m", "generator", "--days", "1"],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
        check=True,
    )
    context.log.info(result.stdout)
    return dg.MaterializeResult(metadata={"generator_output": result.stdout})


# Must stay in sync with models/staging/sources.yml.
RAW_TABLES = [
    ("core_insurance", "agents"),
    ("core_insurance", "branches"),
    ("core_insurance", "claims"),
    ("core_insurance", "customers"),
    ("core_insurance", "policies"),
    ("core_insurance", "premium_schedules"),
    ("core_insurance", "products"),
    ("mpesa_gateway", "transactions"),
    ("bank_payments", "payments"),
]


@dg.multi_asset(
    specs=[
        dg.AssetSpec(
            key=dg.AssetKey(["raw", source, table]),
            deps=[raw_source_batch],
            group_name="ingestion",
            kinds={"duckdb"},
        )
        for source, table in RAW_TABLES
    ],
)
def ingested_raw(context: dg.AssetExecutionContext):
    """Load new raw partitions into DuckDB via dlt (incremental on load_date)."""
    result = subprocess.run(
        [sys.executable, "-m", "ingestion"],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
        check=True,
    )
    context.log.info(result.stdout)
    counts = {}
    for line in result.stdout.splitlines():
        parts = line.split()
        if len(parts) == 3 and parts[2] == "rows":
            counts[parts[0]] = int(parts[1])
    for source, table in RAW_TABLES:
        yield dg.MaterializeResult(
            asset_key=dg.AssetKey(["raw", source, table]),
            metadata={"rows_loaded": counts.get(f"{source}.{table}", 0)},
        )


class RawSourceTranslator(DagsterDbtTranslator):
    """Point every dbt source at its ingestion asset so lineage connects."""

    def get_asset_key(self, dbt_resource_props):
        if dbt_resource_props["resource_type"] == "source":
            return dg.AssetKey(["raw", dbt_resource_props["source_name"], dbt_resource_props["name"]])
        return super().get_asset_key(dbt_resource_props)


@dbt_assets(
    manifest=dbt_project.manifest_path,
    dagster_dbt_translator=RawSourceTranslator(),
    project=dbt_project,
)
def dbt_warehouse(context: dg.AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()


daily_refresh = dg.define_asset_job(
    "daily_warehouse_refresh",
    selection=dg.AssetSelection.all(),
)

daily_schedule = dg.ScheduleDefinition(
    job=daily_refresh,
    cron_schedule="0 3 * * *",
    execution_timezone="Africa/Nairobi",
)

defs = dg.Definitions(
    assets=[raw_source_batch, ingested_raw, dbt_warehouse],
    resources={"dbt": DbtCliResource(project_dir=dbt_project)},
    jobs=[daily_refresh],
    schedules=[daily_schedule],
)
