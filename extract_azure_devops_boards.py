"""
Azure DevOps Boards extractor using dlt.

Fetches work items from Azure DevOps sprints/iterations and stores them
in a normalised format compatible with the feather-ai pipeline.

Required secrets (.dlt/secrets.toml):
    [projects.<name>]
    ado_org_url        = "https://dev.azure.com/your-org"
    ado_pat            = "YOUR_PERSONAL_ACCESS_TOKEN"   # Work Items (Read) scope
    ado_boards_project = "SCF"
    ado_boards_team    = "SCF Team"
    # ado_boards_sprint = "Sprint 4"  # optional — defaults to current sprint only
    # ado_boards_all_sprints = true   # optional — fetch all sprints
"""
from __future__ import annotations

import logging
from typing import Iterator

import dlt
import requests

from extract_azure_devops_wiki import _auth_header

logger = logging.getLogger(__name__)

WORK_ITEM_FIELDS = ",".join([
    "System.Id",
    "System.Title",
    "System.WorkItemType",
    "System.State",
    "System.AssignedTo",
    "System.Description",
    "System.Tags",
    "System.IterationPath",
    "System.AreaPath",
    "System.CreatedDate",
    "System.ChangedDate",
    "System.Parent",
    "Microsoft.VSTS.Common.Priority",
    "Microsoft.VSTS.Scheduling.StoryPoints",
    "Microsoft.VSTS.Common.StateChangeDate",
])


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _list_iterations(org_url: str, project: str, team: str, headers: dict) -> list[dict]:
    """Return all iterations (sprints) for a team."""
    url = f"{org_url}/{project}/{team}/_apis/work/teamsettings/iterations"
    resp = requests.get(url, headers=headers, params={"api-version": "7.1"}, timeout=30)
    resp.raise_for_status()
    return resp.json().get("value", [])


def _get_work_item_ids(org_url: str, project: str, iteration_path: str, headers: dict) -> list[int]:
    """Return all work item IDs in a given iteration path via WIQL."""
    wiql = {
        "query": (
            f"SELECT [System.Id] FROM WorkItems "
            f"WHERE [System.TeamProject] = '{project}' "
            f"AND [System.IterationPath] UNDER '{iteration_path}' "
            f"ORDER BY [System.Id]"
        )
    }
    url = f"{org_url}/{project}/_apis/wit/wiql"
    resp = requests.post(
        url,
        headers={**headers, "Content-Type": "application/json"},
        json=wiql,
        params={"api-version": "7.1"},
        timeout=30,
    )
    resp.raise_for_status()
    return [item["id"] for item in resp.json().get("workItems", [])]


def _get_work_items_batch(
    org_url: str, project: str, ids: list[int], headers: dict
) -> list[dict]:
    """Fetch work item details in batches of 200 (API limit)."""
    results = []
    for i in range(0, len(ids), 200):
        batch = ids[i : i + 200]
        resp = requests.get(
            f"{org_url}/{project}/_apis/wit/workitems",
            headers=headers,
            params={
                "api-version": "7.1",
                "ids": ",".join(str(id) for id in batch),
                "fields": WORK_ITEM_FIELDS,
            },
            timeout=30,
        )
        resp.raise_for_status()
        results.extend(resp.json().get("value", []))
    return results


def _normalise_work_item(item: dict, sprint_name: str, sprint_time_frame: str) -> dict:
    """Flatten a work item's fields into a clean record."""
    f = item.get("fields", {})
    assigned_to = f.get("System.AssignedTo") or {}
    return {
        "id": f["System.Id"],
        "title": f.get("System.Title", ""),
        "work_item_type": f.get("System.WorkItemType", ""),
        "state": f.get("System.State", ""),
        "assigned_to": assigned_to.get("displayName", "") if isinstance(assigned_to, dict) else str(assigned_to),
        "description": f.get("System.Description", "") or "",
        "tags": f.get("System.Tags", "") or "",
        "iteration_path": f.get("System.IterationPath", ""),
        "sprint_name": sprint_name,
        "sprint_time_frame": sprint_time_frame,
        "area_path": f.get("System.AreaPath", ""),
        "parent_id": f.get("System.Parent"),
        "priority": f.get("Microsoft.VSTS.Common.Priority"),
        "story_points": f.get("Microsoft.VSTS.Scheduling.StoryPoints"),
        "created": f.get("System.CreatedDate", ""),
        "updated": f.get("System.ChangedDate", ""),
        "state_changed": f.get("Microsoft.VSTS.Common.StateChangeDate", ""),
        "url": item.get("url", ""),
    }


# ---------------------------------------------------------------------------
# dlt resources
# ---------------------------------------------------------------------------

@dlt.resource(name="work_items", write_disposition="replace")
def _ado_work_items(
    org_url: str,
    project: str,
    team: str,
    sprint_name: str | None,
    all_sprints: bool,
    pat: str,
) -> Iterator[dict]:
    headers = _auth_header(pat)

    iterations = _list_iterations(org_url, project, team, headers)
    if not iterations:
        logger.warning("No iterations found for team '%s' in project '%s'", team, project)
        return

    # Decide which iterations to fetch
    if sprint_name:
        target = [i for i in iterations if i["name"] == sprint_name]
        if not target:
            available = [i["name"] for i in iterations]
            raise ValueError(f"Sprint '{sprint_name}' not found. Available: {available}")
    elif all_sprints:
        target = iterations
    else:
        # Default: current sprint only
        target = [i for i in iterations if i.get("attributes", {}).get("timeFrame") == "current"]
        if not target:
            logger.warning("No current sprint found — falling back to all sprints")
            target = iterations

    logger.info(
        "Fetching work items for %d sprint(s): %s",
        len(target),
        [i["name"] for i in target],
    )

    for iteration in target:
        sprint = iteration["name"]
        time_frame = iteration.get("attributes", {}).get("timeFrame", "")
        # Iteration path uses backslash: "SCF\Sprint 4"
        iteration_path = iteration.get("path", f"{project}\\{sprint}")

        ids = _get_work_item_ids(org_url, project, iteration_path, headers)
        logger.info("Found %d work item(s) in '%s'", len(ids), sprint)
        if not ids:
            continue

        items = _get_work_items_batch(org_url, project, ids, headers)
        for item in items:
            yield _normalise_work_item(item, sprint, time_frame)


@dlt.resource(name="iterations", write_disposition="replace")
def _ado_iterations(
    org_url: str,
    project: str,
    team: str,
    pat: str,
) -> Iterator[dict]:
    headers = _auth_header(pat)
    for iteration in _list_iterations(org_url, project, team, headers):
        attrs = iteration.get("attributes", {})
        yield {
            "id": iteration["id"],
            "name": iteration["name"],
            "path": iteration.get("path", ""),
            "time_frame": attrs.get("timeFrame", ""),
            "start_date": attrs.get("startDate", ""),
            "finish_date": attrs.get("finishDate", ""),
        }


# ---------------------------------------------------------------------------
# dlt source
# ---------------------------------------------------------------------------

@dlt.source
def azure_devops_boards_source(
    org_url: str | None = None,
    project: str | None = None,
    team: str | None = None,
    sprint_name: str | None = None,
    all_sprints: bool = False,
    pat: str | None = None,
):
    """dlt source for Azure DevOps work items.

    All parameters read from per-project secrets when not passed directly.
    """
    org_url = (org_url or "").rstrip("/")
    yield _ado_work_items(org_url, project, team, sprint_name, all_sprints, pat)
    yield _ado_iterations(org_url, project, team, pat)
