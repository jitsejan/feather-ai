"""
Load per-project configuration entirely from dlt secrets.

All project config (space_key, board_id, credentials) lives in
the gitignored secrets.toml under [projects.<name>]. Nothing
project-specific is committed to the repo.

secrets.toml structure:
    [projects.mycompany]
    base_url  = "https://mycompany.atlassian.net"
    username  = "me@mycompany.com"
    password  = "ATLASSIAN_API_TOKEN"
    confluence_space_key = "ENG"
    jira_board_id        = 42
    # optional — defaults shown below:
    # confluence_expand = "body.storage,space,metadata.labels,ancestors,version"
"""
from __future__ import annotations

from dataclasses import dataclass, field

import dlt

DEFAULT_EXPAND = "body.storage,space,metadata.labels,ancestors,version"


@dataclass
class ProjectConfig:
    name: str
    base_url: str
    username: str
    password: str
    confluence_space_key: str
    jira_board_id: int
    confluence_expand: str = field(default=DEFAULT_EXPAND)

    @property
    def confluence_dataset(self) -> str:
        return f"{self.name}_confluence"

    @property
    def jira_dataset(self) -> str:
        return f"{self.name}_jira"


def load_project(name: str) -> ProjectConfig:
    """Load a single project from secrets.toml [projects.<name>]."""
    secrets = dlt.secrets.get(f"projects.{name}")
    if not secrets:
        raise KeyError(
            f"No configuration found for project '{name}'. "
            f"Add a [projects.{name}] section to your .dlt/secrets.toml."
        )
    return ProjectConfig(
        name=name,
        base_url=secrets["base_url"],
        username=secrets["username"],
        password=secrets["password"],
        confluence_space_key=secrets["confluence_space_key"],
        jira_board_id=int(secrets["jira_board_id"]),
        confluence_expand=secrets.get("confluence_expand", DEFAULT_EXPAND),
    )


def list_projects() -> list[str]:
    """Return names of all projects defined in secrets.toml."""
    all_projects = dlt.secrets.get("projects") or {}
    # Filter out any non-dict entries (e.g. if someone puts a scalar under [projects])
    return sorted(k for k, v in all_projects.items() if isinstance(v, dict))


def load_all_projects() -> list[ProjectConfig]:
    return [load_project(name) for name in list_projects()]
