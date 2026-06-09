"""
Azure DevOps Wiki extractor using dlt.

Fetches all wiki pages from an Azure DevOps project and yields them
in a normalised format compatible with the rest of the feather-ai pipeline.

Required secrets (.dlt/secrets.toml):
    [sources.azure_devops]
    pat = "YOUR_PERSONAL_ACCESS_TOKEN"   # Wiki (Read) scope

Required config (.dlt/config.toml):
    [sources.azure_devops]
    org_url  = "https://dev.azure.com/your-org"
    project  = "your-project"
    # wiki_name = "your-wiki"   # optional — auto-discovered when omitted
"""
from __future__ import annotations

import base64
import logging
from typing import Iterator

import dlt
import requests

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _auth_header(pat: str) -> dict[str, str]:
    """Return a Basic-Auth header for an Azure DevOps PAT."""
    encoded = base64.b64encode(f":{pat}".encode()).decode()
    return {"Authorization": f"Basic {encoded}"}


def _api_get(url: str, headers: dict, params: dict | None = None) -> dict:
    response = requests.get(url, headers=headers, params=params, timeout=30)
    response.raise_for_status()
    return response.json()


def _list_wikis(org_url: str, project: str, headers: dict) -> list[dict]:
    url = f"{org_url}/{project}/_apis/wiki/wikis"
    data = _api_get(url, headers, params={"api-version": "7.1"})
    return data.get("value", [])


def _list_wiki_pages(
    org_url: str,
    project: str,
    wiki_id: str,
    headers: dict,
) -> list[dict]:
    """Return a flat list of page stubs (id, path, gitItemPath, url)."""
    url = f"{org_url}/{project}/_apis/wiki/wikis/{wiki_id}/pages"
    data = _api_get(
        url,
        headers,
        params={
            "api-version": "7.1",
            "recursionLevel": "full",
            "includeContent": "false",
        },
    )
    pages: list[dict] = []
    _collect_pages(data, pages)
    return pages


def _collect_pages(node: dict, accumulator: list[dict]) -> None:
    """Recursively walk the page tree returned by the API."""
    if node.get("id") or node.get("path"):
        accumulator.append(node)
    for sub in node.get("subPages", []):
        _collect_pages(sub, accumulator)


def _get_page_content(
    org_url: str,
    project: str,
    wiki_id: str,
    page_path: str,
    headers: dict,
) -> str:
    """Fetch the raw markdown content of a single wiki page."""
    url = f"{org_url}/{project}/_apis/wiki/wikis/{wiki_id}/pages"
    try:
        response = requests.get(
            url,
            headers={**headers, "Accept": "text/plain"},
            params={
                "api-version": "7.1",
                "path": page_path,
                "includeContent": "true",
            },
            timeout=30,
        )
        response.raise_for_status()
        # The API may return JSON with a `content` field or raw text.
        content_type = response.headers.get("Content-Type", "")
        if "application/json" in content_type:
            return response.json().get("content", "")
        return response.text
    except requests.HTTPError as exc:
        logger.warning("Could not fetch content for %s: %s", page_path, exc)
        return ""


# ---------------------------------------------------------------------------
# dlt resource
# ---------------------------------------------------------------------------

@dlt.resource(name="pages", write_disposition="replace")
def _ado_wiki_pages(
    org_url: str,
    project: str,
    wiki_name: str | None,
    pat: str,
) -> Iterator[dict]:
    headers = _auth_header(pat)

    wikis = _list_wikis(org_url, project, headers)
    if not wikis:
        logger.warning("No wikis found in project '%s'", project)
        return

    if wiki_name:
        target_wikis = [w for w in wikis if w.get("name") == wiki_name]
        if not target_wikis:
            raise ValueError(
                f"Wiki '{wiki_name}' not found. "
                f"Available: {[w.get('name') for w in wikis]}"
            )
    else:
        target_wikis = wikis
        logger.info("Auto-discovered %d wiki(s): %s", len(wikis), [w.get("name") for w in wikis])

    for wiki in target_wikis:
        wiki_id = wiki["id"]
        wiki_display_name = wiki.get("name", wiki_id)
        logger.info("Processing wiki '%s' (%s)", wiki_display_name, wiki_id)

        page_stubs = _list_wiki_pages(org_url, project, wiki_id, headers)
        logger.info("Found %d page(s) in wiki '%s'", len(page_stubs), wiki_display_name)

        for stub in page_stubs:
            page_path = stub.get("path", "")
            content = _get_page_content(org_url, project, wiki_id, page_path, headers)
            yield {
                "id": f"{wiki_id}:{page_path}",
                "wiki_id": wiki_id,
                "wiki_name": wiki_display_name,
                "path": page_path,
                "title": page_path.rstrip("/").split("/")[-1] or wiki_display_name,
                "content": content,
                "order": stub.get("order", 0),
                "is_parent_page": bool(stub.get("subPages")),
                "remote_url": stub.get("remoteUrl", ""),
                "git_item_path": stub.get("gitItemPath", ""),
            }


# ---------------------------------------------------------------------------
# dlt source
# ---------------------------------------------------------------------------

@dlt.source
def azure_devops_wiki_source(
    org_url: str | None = None,
    project: str | None = None,
    wiki_name: str | None = None,
    pat: str | None = None,
):
    """dlt source for Azure DevOps wiki pages.

    Configuration is read from dlt config/secrets when not provided directly:
        config  -> sources.azure_devops.org_url / project / wiki_name
        secrets -> sources.azure_devops.pat
    """
    org_url = org_url or dlt.config["sources.azure_devops.org_url"]
    project = project or dlt.config["sources.azure_devops.project"]
    wiki_name = wiki_name or dlt.config.get("sources.azure_devops.wiki_name")
    pat = pat or dlt.secrets["sources.azure_devops.pat"]

    # Strip trailing slash for consistent URL construction.
    org_url = org_url.rstrip("/")

    yield _ado_wiki_pages(org_url, project, wiki_name, pat)


def ado_wiki_source():
    """Convenience wrapper returning only the pages resource."""
    return azure_devops_wiki_source().pages
