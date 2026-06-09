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
import os
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


def _list_attachments(
    org_url: str,
    project: str,
    wiki: dict,
    headers: dict,
) -> list[dict]:
    """Return a list of attachment metadata dicts from the wiki git repo."""
    repo_id = wiki.get("repositoryId")
    mapped_path = wiki.get("mappedPath", "/wiki").rstrip("/")
    if not repo_id:
        return []

    attachments_path = f"{mapped_path}/.attachments"
    url = f"{org_url}/{project}/_apis/git/repositories/{repo_id}/items"
    try:
        resp = requests.get(
            url,
            headers=headers,
            params={
                "api-version": "7.1",
                "scopePath": attachments_path,
                "recursionLevel": "full",
                "latestProcessedChange": "false",
            },
            timeout=30,
        )
        resp.raise_for_status()
        items = resp.json().get("value", [])
        # Skip the folder entry itself; keep only files
        return [
            {
                "repo_id": repo_id,
                "git_path": item["path"],
                # Normalise to just the filename relative to .attachments/
                "name": item["path"].replace(attachments_path + "/", ""),
                "url": item.get("url", ""),
                "is_folder": item.get("gitObjectType", "") == "tree",
            }
            for item in items
            if item.get("gitObjectType") != "tree"
        ]
    except requests.HTTPError as exc:
        logger.warning("Could not list attachments for wiki %s: %s", wiki.get("name"), exc)
        return []


def download_ado_attachment(
    org_url: str,
    project: str,
    repo_id: str,
    git_path: str,
    dest_path: str,
    headers: dict,
) -> bool:
    """Download a single attachment from the ADO git repo to dest_path. Returns True on success."""
    if os.path.exists(dest_path):
        return True
    os.makedirs(os.path.dirname(dest_path), exist_ok=True)
    url = f"{org_url}/{project}/_apis/git/repositories/{repo_id}/items"
    try:
        resp = requests.get(
            url,
            headers={**headers, "Accept": "application/octet-stream"},
            params={"api-version": "7.1", "path": git_path},
            timeout=60,
            stream=True,
        )
        resp.raise_for_status()
        with open(dest_path, "wb") as f:
            for chunk in resp.iter_content(chunk_size=8192):
                f.write(chunk)
        return True
    except Exception as exc:
        logger.warning("Could not download attachment %s: %s", git_path, exc)
        return False


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
# dlt resources
# ---------------------------------------------------------------------------

def _resolve_wikis(org_url: str, project: str, wiki_name: str | None, headers: dict) -> list[dict]:
    wikis = _list_wikis(org_url, project, headers)
    if not wikis:
        logger.warning("No wikis found in project '%s'", project)
        return []
    if wiki_name:
        target = [w for w in wikis if w.get("name") == wiki_name]
        if not target:
            raise ValueError(
                f"Wiki '{wiki_name}' not found. "
                f"Available: {[w.get('name') for w in wikis]}"
            )
        return target
    logger.info("Auto-discovered %d wiki(s): %s", len(wikis), [w.get("name") for w in wikis])
    return wikis


@dlt.resource(name="pages", write_disposition="replace")
def _ado_wiki_pages(
    org_url: str,
    project: str,
    wiki_name: str | None,
    pat: str,
) -> Iterator[dict]:
    headers = _auth_header(pat)
    for wiki in _resolve_wikis(org_url, project, wiki_name, headers):
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
                "repo_id": wiki.get("repositoryId", ""),
                "path": page_path,
                "title": page_path.rstrip("/").split("/")[-1] or wiki_display_name,
                "content": content,
                "order": stub.get("order", 0),
                "is_parent_page": bool(stub.get("subPages")),
                "remote_url": stub.get("remoteUrl", ""),
                "git_item_path": stub.get("gitItemPath", ""),
            }


@dlt.resource(name="attachments", write_disposition="replace")
def _ado_wiki_attachments(
    org_url: str,
    project: str,
    wiki_name: str | None,
    pat: str,
) -> Iterator[dict]:
    headers = _auth_header(pat)
    for wiki in _resolve_wikis(org_url, project, wiki_name, headers):
        wiki_display_name = wiki.get("name", wiki["id"])
        attachments = _list_attachments(org_url, project, wiki, headers)
        logger.info(
            "Found %d attachment(s) in wiki '%s'", len(attachments), wiki_display_name
        )
        for att in attachments:
            yield {
                "id": f"{wiki['id']}:{att['name']}",
                "wiki_id": wiki["id"],
                "wiki_name": wiki_display_name,
                "repo_id": att["repo_id"],
                "git_path": att["git_path"],
                "name": att["name"],
                "download_url": att["url"],
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
    yield _ado_wiki_attachments(org_url, project, wiki_name, pat)


def ado_wiki_source():
    """Convenience wrapper returning only the pages resource."""
    return azure_devops_wiki_source().pages
