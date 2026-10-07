"""Configuration loader for Jira, Azure DevOps, and GitHub integrations."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Mapping, Sequence
import os
import tomllib

CONFIG_ENV_VAR = "AGILE_AI_CONFIG"
DEFAULT_CONFIG_PATH = Path("config/integrations.toml")


@dataclass(slots=True)
class JiraInstance:
    name: str
    base_url: str
    project_key: str
    email_env: str
    token_env: str

    def creds(self) -> tuple[str, str]:
        email = os.environ.get(self.email_env)
        token = os.environ.get(self.token_env)
        if not email or not token:
            missing = []
            if not email:
                missing.append(self.email_env)
            if not token:
                missing.append(self.token_env)
            raise ValueError(
                f"Missing Jira credentials for instance '{self.name}'. Set: {', '.join(missing)}"
            )
        return email, token


@dataclass(slots=True)
class AzureDevOpsInstance:
    name: str
    org: str
    project: str
    repos: Sequence[str]
    token_env: str

    def token(self) -> str:
        value = os.environ.get(self.token_env)
        if not value:
            raise ValueError(f"Missing Azure DevOps PAT for '{self.name}'. Set {self.token_env}.")
        return value


@dataclass(slots=True)
class GithubInstance:
    name: str
    org: str
    repos: Sequence[str]
    token_env: str

    def token(self) -> str:
        value = os.environ.get(self.token_env)
        if not value:
            raise ValueError(f"Missing GitHub PAT for '{self.name}'. Set {self.token_env}.")
        return value


@dataclass(slots=True)
class Company:
    name: str
    jira_instances: Sequence[str]
    azure_devops_instances: Sequence[str]
    github_instances: Sequence[str]


@dataclass(slots=True)
class IntegrationConfig:
    jira: Mapping[str, JiraInstance]
    azure_devops: Mapping[str, AzureDevOpsInstance]
    github: Mapping[str, GithubInstance]
    companies: Mapping[str, Company]

    def company(self, name: str) -> Company:
        try:
            return self.companies[name]
        except KeyError as exc:
            raise KeyError(f"No company named '{name}' in config") from exc


def _load_raw_config(path: Path) -> Mapping[str, object]:
    with path.open("rb") as fh:
        return tomllib.load(fh)


def load_config(path: str | Path | None = None) -> IntegrationConfig:
    candidates: list[Path] = []
    if path:
        candidates.append(Path(path))
    env_override = os.environ.get(CONFIG_ENV_VAR)
    if env_override:
        candidates.append(Path(env_override))
    candidates.append(DEFAULT_CONFIG_PATH)

    for candidate in candidates:
        if candidate.exists():
            raw = _load_raw_config(candidate)
            return _parse_config(raw)

    searched = ", ".join(str(c) for c in candidates)
    raise FileNotFoundError(f"No integration config found. Looked in: {searched}")


def _parse_config(raw: Mapping[str, object]) -> IntegrationConfig:
    jira_instances = {
        name: JiraInstance(
            name=name,
            base_url=str(cfg.get("base_url")),
            project_key=str(cfg.get("project_key")),
            email_env=str(cfg.get("email_env")),
            token_env=str(cfg.get("token_env")),
        )
        for name, cfg in _iter_subsection(raw, "jira", "instances").items()
    }

    ado_instances = {
        name: AzureDevOpsInstance(
            name=name,
            org=str(cfg.get("org")),
            project=str(cfg.get("project")),
            repos=tuple(cfg.get("repos", [])),
            token_env=str(cfg.get("token_env")),
        )
        for name, cfg in _iter_subsection(raw, "azure_devops", "instances").items()
    }

    gh_instances = {
        name: GithubInstance(
            name=name,
            org=str(cfg.get("org")),
            repos=tuple(cfg.get("repos", [])),
            token_env=str(cfg.get("token_env")),
        )
        for name, cfg in _iter_subsection(raw, "github", "instances").items()
    }

    companies = {
        name: Company(
            name=name,
            jira_instances=tuple(cfg.get("jira_instances", [])),
            azure_devops_instances=tuple(cfg.get("azure_devops_instances", [])),
            github_instances=tuple(cfg.get("github_instances", [])),
        )
        for name, cfg in _iter_subsection(raw, "companies").items()
    }

    return IntegrationConfig(
        jira=jira_instances,
        azure_devops=ado_instances,
        github=gh_instances,
        companies=companies,
    )


def _iter_subsection(raw: Mapping[str, object], *path: str) -> Mapping[str, Mapping[str, object]]:
    section = raw
    for key in path:
        section = section.get(key, {})  # type: ignore[assignment]
        if not isinstance(section, Mapping):
            return {}
    return section  # type: ignore[return-value]
