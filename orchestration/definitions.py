from pathlib import Path
import os

import dagster as dg
from dagster_dbt import DbtCliResource, DbtProject, dbt_assets
from dagster_dlt import DagsterDltResource, DagsterDltTranslator, dlt_assets

import dlt
from store_in_duckdb import build_pipeline, confluence_processed_source, configure_logging


DBT_PROJECT_DIR = Path(__file__).resolve().parents[1] / "dbt"
DBT_PROFILES_DIR = DBT_PROJECT_DIR

# Make dbt in Dagster work without requiring manual env exports each run.
if not os.getenv("MOTHERDUCK_CONNECTION_STRING"):
    os.environ["MOTHERDUCK_CONNECTION_STRING"] = dlt.secrets["destination.duckdb.credentials"]

dbt_project = DbtProject(project_dir=DBT_PROJECT_DIR, profiles_dir=DBT_PROFILES_DIR)
dbt_project.prepare_if_dev()


class ConfluenceDltTranslator(DagsterDltTranslator):
    """Map dlt resources to raw schema asset keys used by dbt sources."""

    def get_asset_spec(self, data):
        base_spec = super().get_asset_spec(data)
        return base_spec.replace_attributes(
            key=dg.AssetKey(["raw_confluence", data.resource.name]),
            kinds={"dlt", "duckdb"},
        )


@dlt_assets(
    dlt_source=confluence_processed_source(),
    dlt_pipeline=build_pipeline(),
    name="confluence_dlt_assets",
    group_name="ingestion",
    dagster_dlt_translator=ConfluenceDltTranslator(),
)
def confluence_dlt_assets(context: dg.AssetExecutionContext, dlt_resource: DagsterDltResource):
    """Native dlt Dagster assets for processed Confluence resources."""
    drop_existing = os.getenv("DAGSTER_DLT_DROP_EXISTING", "false").lower() == "true"
    configure_logging(log_level=os.getenv("DAGSTER_LOG_LEVEL", "INFO"))
    context.log.info("Starting dlt ingestion (drop_existing=%s)", drop_existing)
    refresh_mode = "drop_resources" if drop_existing else None
    yield from dlt_resource.run(context=context, refresh=refresh_mode)


@dbt_assets(
    manifest=dbt_project.manifest_path,
    project=dbt_project,
    name="confluence_dbt_assets",
)
def confluence_dbt_assets(context: dg.AssetExecutionContext, dbt: DbtCliResource):
    """Native dbt assets (models + tests) for Confluence analytics."""
    yield from dbt.cli(["build"], context=context).stream()


confluence_dlt_dbt_job = dg.define_asset_job(
    "confluence_dlt_dbt_job",
    selection=dg.AssetSelection.assets(confluence_dlt_assets).downstream(),
)


confluence_dbt_assets_grouped = dg.map_asset_specs(
    lambda spec: spec.replace_attributes(group_name="transformation"),
    [confluence_dbt_assets],
)


defs = dg.Definitions(
    assets=[confluence_dlt_assets, *confluence_dbt_assets_grouped],
    resources={
        "dbt": DbtCliResource(
            project_dir=DBT_PROJECT_DIR,
            profiles_dir=DBT_PROFILES_DIR,
        ),
        "dlt_resource": DagsterDltResource(),
    },
    jobs=[confluence_dlt_dbt_job],
)
