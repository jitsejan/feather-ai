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

    # Azure DevOps wiki (optional — omit if not used)
    # ado_org_url   = "https://dev.azure.com/your-org"
    # ado_project   = "your-project"
    # ado_wiki_name = "your-wiki"   # leave out to ingest all wikis
    # ado_pat       = "YOUR_PERSONAL_ACCESS_TOKEN"
"""
from __future__ import annotations

from dataclasses import dataclass, field

import dlt

DEFAULT_EXPAND = "body.storage,space,metadata.labels,ancestors,version"


@dataclass
class ProjectConfig:
    name: str
    # Atlassian (Confluence + Jira) — optional when using ADO-only projects
    base_url: str | None = field(default=None)
    username: str | None = field(default=None)
    password: str | None = field(default=None)
    confluence_space_key: str | None = field(default=None)
    jira_board_id: int | None = field(default=None)
    confluence_expand: str = field(default=DEFAULT_EXPAND)
    # Azure DevOps wiki — all optional; set to enable ADO ingestion
    ado_org_url: str | None = field(default=None)
    ado_project: str | None = field(default=None)
    ado_wiki_name: str | None = field(default=None)
    ado_pat: str | None = field(default=None)

    @property
    def has_ado(self) -> bool:
        """True when the minimum ADO config (org_url, project, pat) is present."""
        return bool(self.ado_org_url and self.ado_project and self.ado_pat)

    @property
    def confluence_dataset(self) -> str:
        return f"{self.name}_confluence"

    @property
    def jira_dataset(self) -> str:
        return f"{self.name}_jira"

    @property
    def ado_dataset(self) -> str:
        return f"{self.name}_ado_wiki"


def load_project(name: str) -> ProjectConfig:
    """Load a single project from secrets.toml [projects.<name>]."""
    secrets = dlt.secrets.get(f"projects.{name}")
    if not secrets:
        raise KeyError(
            f"No configuration found for project '{name}'. "
            f"Add a [projects.{name}] section to your .dlt/secrets.toml."
        )
    jira_board_id = secrets.get("jira_board_id")
    return ProjectConfig(
        name=name,
        base_url=secrets.get("base_url"),
        username=secrets.get("username"),
        password=secrets.get("password"),
        confluence_space_key=secrets.get("confluence_space_key"),
        jira_board_id=int(jira_board_id) if jira_board_id is not None else None,
        confluence_expand=secrets.get("confluence_expand", DEFAULT_EXPAND),
        ado_org_url=secrets.get("ado_org_url"),
        ado_project=secrets.get("ado_project"),
        ado_wiki_name=secrets.get("ado_wiki_name"),
        ado_pat=secrets.get("ado_pat"),
    )


def list_projects() -> list[str]:
    """Return names of all projects defined in secrets.toml."""
    all_projects = dlt.secrets.get("projects") or {}
    # Filter out any non-dict entries (e.g. if someone puts a scalar under [projects])
    return sorted(k for k, v in all_projects.items() if isinstance(v, dict))


def load_all_projects() -> list[ProjectConfig]:
    return [load_project(name) for name in list_projects()]
