"""
Load per-project configuration entirely from dlt secrets.

All project config (space_key, board_id, credentials) lives in
the gitignored secrets.toml under [projects.<name>]. Nothing
project-specific is committed to the repo.

secrets.toml structure:

    [projects.myproject]
    # Atlassian (optional — omit if not using Confluence/Jira)
    base_url             = "https://myorg.atlassian.net"
    username             = "me@example.com"
    password             = "ATLASSIAN_API_TOKEN"
    confluence_space_key = "ENG"
    jira_board_id        = 42
    # confluence_expand  = "body.storage,space,metadata.labels,ancestors,version"

    # Azure DevOps wiki (optional — omit if not using ADO)
    # ado_org_url   = "https://dev.azure.com/your-org"
    # ado_project   = "your-project"
    # ado_wiki_name = "your-wiki"   # leave out to ingest all wikis
    # ado_pat       = "YOUR_PERSONAL_ACCESS_TOKEN"  # Wiki (Read) scope

    # Obsidian export (optional)
    # obsidian_vault_path = "folder/inside/vault"
    # obsidian_label      = "Display Name"   # shown in weekly note

    [obsidian]
    vault   = "/path/to/your/ObsidianVault"
    my_name = "Your Name"
"""
from __future__ import annotations

from dataclasses import dataclass, field

import dlt

DEFAULT_EXPAND = "body.storage,space,metadata.labels,ancestors,version"


@dataclass
class ProjectConfig:
    name: str
    # Atlassian (Confluence + Jira) — optional for ADO-only projects
    base_url: str | None = field(default=None)
    username: str | None = field(default=None)
    password: str | None = field(default=None)
    confluence_space_key: str | None = field(default=None)
    jira_board_id: int | None = field(default=None)
    confluence_expand: str = field(default=DEFAULT_EXPAND)
    # Azure DevOps wiki — all optional
    ado_org_url: str | None = field(default=None)
    ado_project: str | None = field(default=None)
    ado_wiki_name: str | None = field(default=None)
    ado_pat: str | None = field(default=None)
    # Obsidian export — optional
    obsidian_vault_path: str | None = field(default=None)
    obsidian_label: str | None = field(default=None)

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
        obsidian_vault_path=secrets.get("obsidian_vault_path"),
        obsidian_label=secrets.get("obsidian_label"),
    )


def list_projects() -> list[str]:
    """Return names of all projects defined in secrets.toml."""
    all_projects = dlt.secrets.get("projects") or {}
    return sorted(k for k, v in all_projects.items() if isinstance(v, dict))


def load_all_projects() -> list[ProjectConfig]:
    return [load_project(name) for name in list_projects()]


def load_obsidian_config() -> dict:
    """Return global Obsidian settings from secrets.toml [obsidian]."""
    cfg = dlt.secrets.get("obsidian") or {}
    return {
        "vault": cfg.get("vault", ""),
        "my_name": cfg.get("my_name", ""),
    }
