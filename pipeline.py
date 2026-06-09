"""
Single entrypoint for running Confluence and/or Jira ingestion
for one or all configured projects.

Usage:
    python pipeline.py --project orbis
    python pipeline.py --project orbis --source confluence
    python pipeline.py --project orbis --source jira
    python pipeline.py --all
    python pipeline.py --all --source confluence
"""
from __future__ import annotations

import argparse
import contextlib
import logging
import os
from io import StringIO

import dlt
from dlt.destinations import duckdb

from extract_confluence import atlassian_confluence_source, process_pages, process_hierarchy
from extract_jira import jira_source, process_issues
from extract_azure_devops_wiki import azure_devops_wiki_source
from project_config import ProjectConfig, load_project, load_all_projects

logger = logging.getLogger(__name__)


def configure_logging(log_level: str = "INFO", dlt_log_level: str = "WARNING") -> None:
    os.environ["DLT_LOG_LEVEL"] = dlt_log_level.upper()
    level = getattr(logging, log_level.upper(), logging.INFO)
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s [%(filename)s:%(lineno)d] %(message)s",
    )
    noisy_level = max(level, logging.WARNING)
    for name in ["dlt", "dlt.sources", "dlt.pipeline", "dlt.destinations", "urllib3", "requests"]:
        logging.getLogger(name).setLevel(noisy_level)


def _destination():
    """Return MotherDuck destination if configured, else local DuckDB."""
    try:
        credentials = dlt.secrets["destination.motherduck.credentials"]
        from dlt.destinations import motherduck
        logger.debug("Using MotherDuck destination")
        return motherduck(credentials)
    except KeyError:
        pass

    try:
        credentials = dlt.secrets["destination.duckdb.credentials"]
    except KeyError:
        credentials = "feather_ai.duckdb"
    logger.debug("Using local DuckDB: %s", credentials)
    return duckdb(credentials)


# ---------------------------------------------------------------------------
# Confluence
# ---------------------------------------------------------------------------

@dlt.source
def _confluence_processed(project: ProjectConfig):
    pages = atlassian_confluence_source(
        base_url=project.base_url,
        space_key=project.confluence_space_key,
        expand=project.confluence_expand,
    ).pages
    return pages | process_pages, pages | process_hierarchy


def run_confluence(project: ProjectConfig, drop_existing: bool = False) -> None:
    logger.info("[%s] Starting Confluence ingestion", project.name)
    # Inject per-project credentials as env vars so dlt secrets picks them up
    os.environ["SOURCES__ATLASSIAN_CONFLUENCE__USERNAME"] = project.username
    os.environ["SOURCES__ATLASSIAN_CONFLUENCE__PASSWORD"] = project.password
    os.environ["SOURCES__ATLASSIAN_CONFLUENCE__BASE_URL"] = project.base_url
    refresh = "drop_resources" if drop_existing else None
    pipeline = dlt.pipeline(
        pipeline_name=f"{project.name}_confluence",
        destination=_destination(),
        dataset_name=project.confluence_dataset,
        refresh=refresh,
    )
    with contextlib.redirect_stdout(StringIO()), contextlib.redirect_stderr(StringIO()):
        load_info = pipeline.run(_confluence_processed(project), refresh=refresh)
    logger.info("[%s] Confluence done: %s", project.name, load_info)


# ---------------------------------------------------------------------------
# Jira
# ---------------------------------------------------------------------------

@dlt.source
def _jira_processed(project: ProjectConfig):
    issues = jira_source(
        base_url=project.base_url,
        board_id=project.jira_board_id,
    ).issues
    return issues | process_issues


def run_jira(project: ProjectConfig, drop_existing: bool = False) -> None:
    logger.info("[%s] Starting Jira ingestion", project.name)
    # Inject per-project credentials as env vars so dlt secrets picks them up
    os.environ["SOURCES__ATLASSIAN_CONFLUENCE__USERNAME"] = project.username
    os.environ["SOURCES__ATLASSIAN_CONFLUENCE__PASSWORD"] = project.password
    os.environ["SOURCES__ATLASSIAN_CONFLUENCE__BASE_URL"] = project.base_url
    refresh = "drop_resources" if drop_existing else None
    pipeline = dlt.pipeline(
        pipeline_name=f"{project.name}_jira",
        destination=_destination(),
        dataset_name=project.jira_dataset,
        refresh=refresh,
    )
    with contextlib.redirect_stdout(StringIO()), contextlib.redirect_stderr(StringIO()):
        load_info = pipeline.run(_jira_processed(project), refresh=refresh)
    logger.info("[%s] Jira done: %s", project.name, load_info)


# ---------------------------------------------------------------------------
# Azure DevOps Wiki
# ---------------------------------------------------------------------------

def run_ado_wiki(project: ProjectConfig, drop_existing: bool = False) -> None:
    if not project.has_ado:
        logger.info("[%s] No ADO config — skipping Azure DevOps wiki ingestion", project.name)
        return
    logger.info("[%s] Starting Azure DevOps wiki ingestion", project.name)
    refresh = "drop_resources" if drop_existing else None
    source = azure_devops_wiki_source(
        org_url=project.ado_org_url,
        project=project.ado_project,
        wiki_name=project.ado_wiki_name,
        pat=project.ado_pat,
    )
    pipeline = dlt.pipeline(
        pipeline_name=f"{project.name}_ado_wiki",
        destination=_destination(),
        dataset_name=project.ado_dataset,
        refresh=refresh,
    )
    with contextlib.redirect_stdout(StringIO()), contextlib.redirect_stderr(StringIO()):
        load_info = pipeline.run(source, refresh=refresh)
    logger.info("[%s] ADO wiki done: %s", project.name, load_info)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _parse_args():
    parser = argparse.ArgumentParser(description="Feather-AI ingestion pipeline")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--project", metavar="NAME", help="Project name (matches projects/<name>.toml)")
    group.add_argument("--all", action="store_true", help="Run all configured projects")
    parser.add_argument(
        "--source",
        choices=["confluence", "jira", "ado_wiki"],
        default=None,
        help="Which source to ingest (default: all)",
    )
    parser.add_argument("--drop-existing", action="store_true")
    parser.add_argument("--log-level", default="INFO",
                        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"])
    parser.add_argument("--dlt-log-level", default="WARNING",
                        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"])
    return parser.parse_args()


def run_project(project: ProjectConfig, source: str | None, drop_existing: bool) -> None:
    if source in (None, "confluence"):
        run_confluence(project, drop_existing=drop_existing)
    if source in (None, "jira"):
        run_jira(project, drop_existing=drop_existing)
    if source in (None, "ado_wiki"):
        run_ado_wiki(project, drop_existing=drop_existing)


if __name__ == "__main__":
    args = _parse_args()
    configure_logging(log_level=args.log_level, dlt_log_level=args.dlt_log_level)

    projects = load_all_projects() if args.all else [load_project(args.project)]
    for project in projects:
        run_project(project, source=args.source, drop_existing=args.drop_existing)
