import dlt
import logging
import re
from html import unescape

from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.auth import HttpBasicAuth
from dlt.sources.helpers.rest_client.paginators import OffsetPaginator

logger = logging.getLogger(__name__)

ISSUE_LIMIT = 100
ISSUE_FIELDS = (
    "summary,description,status,issuetype,priority,assignee,reporter,"
    "created,updated,labels,components,parent,subtasks,comment,sprint,fixVersions"
)


def _jira_text(value: str | None) -> str:
    """Strip Jira wiki markup / HTML tags and normalise whitespace."""
    if not value:
        return ""
    text = re.sub(r"<[^>]+>", " ", value)
    text = unescape(text)
    text = text.replace("\xa0", " ")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\s*\n\s*", "\n", text)
    text = re.sub(r"\n{2,}", "\n", text)
    text = text.replace("\n", " ")
    return re.sub(r"\s{2,}", " ", text).strip()


def _user(field: dict | None) -> str:
    if not field:
        return ""
    return field.get("displayName") or field.get("emailAddress", "")


def _name(field: dict | None) -> str:
    return field.get("name", "") if field else ""


def build_issue_record(issue: dict) -> dict:
    f = issue.get("fields", {})
    parent = f.get("parent") or {}
    parent_fields = parent.get("fields", {})
    sprint = f.get("sprint") or {}
    comments = [
        {
            "id": c.get("id"),
            "author": _user(c.get("author")),
            "body": _jira_text(c.get("body")),
            "created": c.get("created"),
            "updated": c.get("updated"),
        }
        for c in (f.get("comment") or {}).get("comments", [])
    ]
    return {
        "id": issue["id"],
        "key": issue["key"],
        "summary": f.get("summary", ""),
        "description": _jira_text(f.get("description")),
        "status": _name(f.get("status")),
        "status_category": (f.get("status") or {}).get("statusCategory", {}).get("name", ""),
        "issue_type": _name(f.get("issuetype")),
        "priority": _name(f.get("priority")),
        "assignee": _user(f.get("assignee")),
        "reporter": _user(f.get("reporter")),
        "created": f.get("created"),
        "updated": f.get("updated"),
        "labels": f.get("labels", []),
        "components": [_name(c) for c in f.get("components", [])],
        "parent_key": parent.get("key"),
        "parent_summary": parent_fields.get("summary", ""),
        "parent_type": _name(parent_fields.get("issuetype")),
        "sprint_name": sprint.get("name"),
        "sprint_state": sprint.get("state"),
        "fix_versions": [_name(v) for v in f.get("fixVersions", [])],
        "subtask_keys": [s.get("key") for s in f.get("subtasks", [])],
        "comments": comments,
        "comment_count": len(comments),
    }


@dlt.resource(name="issues", write_disposition="replace", primary_key="id")
def _jira_issues(base_url: str, board_id: int, fields: str):
    username = dlt.secrets["sources.atlassian_confluence.username"]
    password = dlt.secrets["sources.atlassian_confluence.password"]
    client = RESTClient(
        base_url=f"{base_url}/rest/agile/1.0",
        auth=HttpBasicAuth(username, password),
        paginator=OffsetPaginator(
            limit=ISSUE_LIMIT,
            offset_param="startAt",
            limit_param="maxResults",
            total_path="total",
        ),
    )
    for page in client.paginate(
        f"board/{board_id}/issue",
        params={"fields": fields},
        data_selector="issues",
    ):
        yield from page


@dlt.transformer(
    primary_key="id",
    write_disposition="merge",
    columns={"sprint_name": {"data_type": "text"}, "sprint_state": {"data_type": "text"}},
)
def process_issues(issue: dict):
    record = build_issue_record(issue)
    if record:
        yield record


@dlt.source
def jira_source(base_url=None, board_id=None, fields=None):
    base_url = base_url or dlt.secrets["sources.atlassian_confluence.base_url"]
    board_id = board_id or dlt.config["sources.jira.board_id"]
    fields = fields or dlt.config.get("sources.jira.fields", ISSUE_FIELDS)
    yield _jira_issues(base_url, board_id, fields)
