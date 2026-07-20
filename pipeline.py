"""
Single entrypoint for running Confluence, Jira and ADO wiki ingestion
for one or all configured projects.

Usage:
    python pipeline.py --project myproject
    python pipeline.py --project myproject --source confluence
    python pipeline.py --project myproject --source jira
    python pipeline.py --project myproject --source ado_wiki
    python pipeline.py --all
    python pipeline.py --all --source confluence
"""
from __future__ import annotations

import argparse
import contextlib
import logging
import os
import sys
from io import StringIO

import dlt
from dlt.destinations import duckdb

from extract_confluence import atlassian_confluence_source, process_pages, process_hierarchy, process_comments
from extract_jira import jira_source, process_issues
from extract_azure_devops_wiki import azure_devops_wiki_source
from extract_azure_devops_boards import azure_devops_boards_source
from project_config import ProjectConfig, load_project, load_all_projects

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Coloured logging
# ---------------------------------------------------------------------------

_RESET   = "\033[0m"
_BOLD    = "\033[1m"
_DIM     = "\033[2m"
_RED     = "\033[31m"
_GREEN   = "\033[32m"
_YELLOW  = "\033[33m"
_BLUE    = "\033[34m"
_MAGENTA = "\033[35m"
_CYAN    = "\033[36m"

_LEVEL_COLORS = {
    "DEBUG":    _BLUE,
    "INFO":     _GREEN,
    "WARNING":  _YELLOW,
    "ERROR":    _RED,
    "CRITICAL": _BOLD + _RED,
}

_SOURCE_COLORS = {
    "confluence": _BLUE,
    "jira":       _YELLOW,
    "ado_wiki":   _MAGENTA,
}


class _ColorFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        if not (hasattr(sys.stderr, "isatty") and sys.stderr.isatty()):
            self._style._fmt = "%(asctime)s %(levelname)-8s %(message)s"
            return super().format(record)

        level_color = _LEVEL_COLORS.get(record.levelname, "")
        asctime = self.formatTime(record, "%Y-%m-%d %H:%M:%S")
        msg = record.getMessage()

        # Highlight [project/source] tags
        import re
        msg = re.sub(
            r"\[([^\]/]+)/([^\]]+)\]",
            lambda m: f"{_CYAN}{_BOLD}[{m.group(1)}]{_RESET}{_SOURCE_COLORS.get(m.group(2), '')}/{m.group(2)}{_RESET}",
            msg,
        )
        msg = re.sub(r"\[([^\]]+)\]", f"{_CYAN}{_BOLD}[\\1]{_RESET}", msg)

        return (
            f"{_DIM}{asctime}{_RESET}  "
            f"{level_color}{record.levelname:<8}{_RESET}  "
            f"{msg}"
        )


def _banner(project_name: str, sources: list[str]) -> None:
    width = 52
    is_tty = hasattr(sys.stderr, "isatty") and sys.stderr.isatty()
    if is_tty:
        line  = f"{_CYAN}{'━' * width}{_RESET}"
        title = f"{_CYAN}{_BOLD}  {project_name.upper()}{_RESET}"
        src   = f"{_DIM}  sources: {', '.join(sources)}{_RESET}"
    else:
        line  = "─" * width
        title = f"  {project_name.upper()}"
        src   = f"  sources: {', '.join(sources)}"
    print(f"\n{line}", file=sys.stderr)
    print(title, file=sys.stderr)
    print(src, file=sys.stderr)
    print(f"{line}", file=sys.stderr)


def configure_logging(log_level: str = "INFO", dlt_log_level: str = "WARNING") -> None:
    os.environ["DLT_LOG_LEVEL"] = dlt_log_level.upper()
    level = getattr(logging, log_level.upper(), logging.INFO)
    handler = logging.StreamHandler()
    handler.setFormatter(_ColorFormatter())
    logging.basicConfig(level=level, handlers=[handler])
    noisy_level = max(level, logging.WARNING)
    for name in ["dlt", "dlt.sources", "dlt.pipeline", "dlt.destinations", "urllib3", "requests"]:
        logging.getLogger(name).setLevel(noisy_level)


def _destination(local: bool = False):
    """Return MotherDuck destination if configured, else local DuckDB."""
    if not local:
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
    return pages | process_pages, pages | process_hierarchy, pages | process_comments


def run_confluence(project: ProjectConfig, drop_existing: bool = False, local: bool = False) -> None:
    if not project.base_url or not project.confluence_space_key:
        logger.info("[%s/confluence] No config — skipping", project.name)
        return
    logger.info("[%s/confluence] Starting ingestion (space: %s)", project.name, project.confluence_space_key)
    os.environ["SOURCES__ATLASSIAN_CONFLUENCE__USERNAME"] = project.username
    os.environ["SOURCES__ATLASSIAN_CONFLUENCE__PASSWORD"] = project.password
    os.environ["SOURCES__ATLASSIAN_CONFLUENCE__BASE_URL"] = project.base_url
    refresh = "drop_resources" if drop_existing else None
    pipeline = dlt.pipeline(
        pipeline_name=f"{project.name}_confluence",
        destination=_destination(local=local),
        dataset_name=project.confluence_dataset,
        refresh=refresh,
    )
    with contextlib.redirect_stdout(StringIO()), contextlib.redirect_stderr(StringIO()):
        load_info = pipeline.run(_confluence_processed(project), refresh=refresh)
    logger.info("[%s/confluence] Done — %s", project.name, load_info)


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


def run_jira(project: ProjectConfig, drop_existing: bool = False, local: bool = False) -> None:
    if not project.base_url or not project.jira_board_id:
        logger.info("[%s/jira] No config — skipping", project.name)
        return
    logger.info("[%s/jira] Starting ingestion (board: %s)", project.name, project.jira_board_id)
    os.environ["SOURCES__ATLASSIAN_CONFLUENCE__USERNAME"] = project.username
    os.environ["SOURCES__ATLASSIAN_CONFLUENCE__PASSWORD"] = project.password
    os.environ["SOURCES__ATLASSIAN_CONFLUENCE__BASE_URL"] = project.base_url
    refresh = "drop_resources" if drop_existing else None
    pipeline = dlt.pipeline(
        pipeline_name=f"{project.name}_jira",
        destination=_destination(local=local),
        dataset_name=project.jira_dataset,
        refresh=refresh,
    )
    with contextlib.redirect_stdout(StringIO()), contextlib.redirect_stderr(StringIO()):
        load_info = pipeline.run(_jira_processed(project), refresh=refresh)
    logger.info("[%s/jira] Done — %s", project.name, load_info)


# ---------------------------------------------------------------------------
# Azure DevOps Wiki
# ---------------------------------------------------------------------------

def run_ado_wiki(project: ProjectConfig, drop_existing: bool = False, local: bool = False) -> None:
    if not project.has_ado:
        logger.info("[%s/ado_wiki] No config — skipping", project.name)
        return
    logger.info("[%s/ado_wiki] Starting ingestion (org: %s)", project.name, project.ado_org_url)
    refresh = "drop_resources" if drop_existing else None
    all_wiki_projects = [project.ado_project] + project.ado_extra_wiki_projects
    source = azure_devops_wiki_source(
        org_url=project.ado_org_url,
        project=all_wiki_projects,
        wiki_name=project.ado_wiki_name,
        pat=project.ado_pat,
    )
    pipeline = dlt.pipeline(
        pipeline_name=f"{project.name}_ado_wiki",
        destination=_destination(local=local),
        dataset_name=project.ado_dataset,
        refresh=refresh,
    )
    with contextlib.redirect_stdout(StringIO()), contextlib.redirect_stderr(StringIO()):
        load_info = pipeline.run(source, refresh=refresh)
    logger.info("[%s/ado_wiki] Done — %s", project.name, load_info)


# ---------------------------------------------------------------------------
# ADO Boards
# ---------------------------------------------------------------------------

def run_ado_boards(project: ProjectConfig, drop_existing: bool = False) -> None:
    if not project.has_ado_boards:
        logger.info("[%s] No ADO boards config — skipping", project.name)
        return
    logger.info("[%s] Starting ADO boards ingestion", project.name)
    refresh = "drop_resources" if drop_existing else None
    source = azure_devops_boards_source(
        org_url=project.ado_org_url,
        project=project.ado_boards_project,
        team=project.ado_boards_team,
        sprint_name=project.ado_boards_sprint,
        all_sprints=project.ado_boards_all_sprints,
        pat=project.ado_pat,
    )
    pipeline = dlt.pipeline(
        pipeline_name=f"{project.name}_ado_boards",
        destination=_destination(),
        dataset_name=project.ado_boards_dataset,
        refresh=refresh,
    )
    with contextlib.redirect_stdout(StringIO()), contextlib.redirect_stderr(StringIO()):
        load_info = pipeline.run(source, refresh=refresh)
    logger.info("[%s] ADO boards done: %s", project.name, load_info)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _parse_args():
    parser = argparse.ArgumentParser(description="Feather-AI ingestion pipeline")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--project", metavar="NAME", help="Project name from secrets.toml")
    group.add_argument("--all", action="store_true", help="Run all configured projects")
    parser.add_argument(
        "--source",
        choices=["confluence", "jira", "ado_wiki", "ado_boards"],
        default=None,
        help="Which source to ingest (default: all)",
    )
    parser.add_argument("--drop-existing", action="store_true")
    parser.add_argument("--local", action="store_true",
                        help="Force local DuckDB destination (skip MotherDuck)")
    parser.add_argument("--log-level", default="INFO",
                        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"])
    parser.add_argument("--dlt-log-level", default="WARNING",
                        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"])
    return parser.parse_args()


def run_project(project: ProjectConfig, source: str | None, drop_existing: bool, local: bool = False) -> None:
    sources = (
        [source] if source
        else [s for s in ("confluence", "jira", "ado_wiki")]
    )
    _banner(project.name, sources)
    if source in (None, "confluence"):
        run_confluence(project, drop_existing=drop_existing, local=local)
    if source in (None, "jira"):
        run_jira(project, drop_existing=drop_existing, local=local)
    if source in (None, "ado_wiki"):
        run_ado_wiki(project, drop_existing=drop_existing, local=local)
    if source in (None, "ado_boards"):
        run_ado_boards(project, drop_existing=drop_existing)


if __name__ == "__main__":
    args = _parse_args()
    configure_logging(log_level=args.log_level, dlt_log_level=args.dlt_log_level)

    projects = load_all_projects() if args.all else [load_project(args.project)]
    for project in projects:
        run_project(project, source=args.source, drop_existing=args.drop_existing, local=args.local)
