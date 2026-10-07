"""dlt source for Azure DevOps repositories (pull requests, commits, comments, releases)."""

from __future__ import annotations

import base64
import logging
from typing import Iterable, Iterator, Mapping

import dlt
import requests

API_VERSION = "7.1-preview.1"
LOGGER = logging.getLogger(__name__)


def _build_session(token: str) -> requests.Session:
    session = requests.Session()
    encoded = base64.b64encode(f":{token}".encode("utf-8")).decode("ascii")
    session.headers.update({
        "Accept": "application/json",
        "Authorization": f"Basic {encoded}",
    })
    return session


def _paged_get(session: requests.Session, url: str, params: Mapping[str, object] | None = None) -> Iterator[dict]:
    continuation = None
    params = dict(params or {})
    while True:
        query = dict(params)
        if continuation:
            query["continuationToken"] = continuation
        resp = session.get(url, params=query)
        resp.raise_for_status()
        yield resp.json()
        continuation = resp.headers.get("x-ms-continuationtoken")
        if not continuation:
            break


@dlt.source(max_table_nesting=1)
def azure_devops(
    org: str,
    project: str,
    token: str,
    repos: Iterable[str],
) -> Iterable[dlt.resources.DltResource]:
    session = _build_session(token)
    base = f"https://dev.azure.com/{org}/{project}/_apis/git/repositories"

    resources = []
    for repo in repos:
        repo_base = f"{base}/{repo}".rstrip("/")

        pr_resource = dlt.resource(
            name=f"ado_{repo}_pull_requests",
            primary_key="pullRequestId",
            write_disposition="merge",
        )(_pull_requests(session, f"{repo_base}/pullRequests"))
        resources.append(pr_resource)

        pr_threads = dlt.resource(
            name=f"ado_{repo}_pull_request_threads",
            primary_key="threadId",
            write_disposition="append",
        )(_pull_request_threads(session, f"{repo_base}/pullRequests"))
        resources.append(pr_threads)

        commits = dlt.resource(
            name=f"ado_{repo}_commits",
            primary_key="commitId",
            write_disposition="append",
        )(_commits(session, f"{repo_base}/commits"))
        resources.append(commits)

        tags = dlt.resource(
            name=f"ado_{repo}_tags",
            primary_key="name",
            write_disposition="merge",
        )(_tags(session, f"{repo_base}/refs"))
        resources.append(tags)

    return resources


def _pull_requests(session: requests.Session, url: str):
    def iterator() -> Iterator[dict]:
        params = {
            "searchCriteria.includeLinks": True,
            "$top": 100,
            "api-version": API_VERSION,
        }
        for page in _paged_get(session, url, params):
            for pr in page.get("value", []):
                yield pr

    return iterator


def _pull_request_threads(session: requests.Session, url: str):
    def iterator() -> Iterator[dict]:
        pr_url = url
        params = {
            "searchCriteria.includeLinks": False,
            "$top": 100,
            "api-version": API_VERSION,
        }
        for page in _paged_get(session, pr_url, params):
            for pr in page.get("value", []):
                pr_id = pr.get("pullRequestId")
                if pr_id is None:
                    continue
                threads_url = f"{pr_url}/{pr_id}/threads"
                thread_params = {"api-version": API_VERSION}
                resp = session.get(threads_url, params=thread_params)
                resp.raise_for_status()
                data = resp.json()
                for thread in data.get("value", []):
                    thread["pullRequestId"] = pr_id
                    yield thread

    return iterator


def _commits(session: requests.Session, url: str):
    def iterator() -> Iterator[dict]:
        params = {"api-version": API_VERSION, "$top": 250}
        for page in _paged_get(session, url, params):
            for commit in page.get("value", []):
                yield commit

    return iterator


def _tags(session: requests.Session, url: str):
    def iterator() -> Iterator[dict]:
        params = {"filter": "tags/", "api-version": API_VERSION}
        for page in _paged_get(session, url, params):
            for ref in page.get("value", []):
                yield ref

    return iterator
