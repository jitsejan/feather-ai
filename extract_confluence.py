import dlt
from dlt.sources.rest_api import rest_api_source
from html import unescape
import logging
import re

logger = logging.getLogger(__name__)

PAGE_LIMIT = 100


def confluence_storage_to_text(value):
    if not value:
        return ""

    text = value
    # Preserve some structure before stripping tags.
    text = re.sub(r"(?i)<br\s*/?>", "\n", text)
    text = re.sub(r"(?i)</(p|div|h1|h2|h3|h4|h5|h6|li|tr|table|blockquote)>", "\n", text)
    text = re.sub(r"(?i)</(td|th)>", "\t", text)
    text = re.sub(r"(?i)<li[^>]*>", "- ", text)

    # Remove all remaining tags, including Confluence namespaced tags.
    text = re.sub(r"<[^>]+>", " ", text)
    text = unescape(text)

    # Normalize whitespace without collapsing line boundaries entirely.
    text = text.replace("\xa0", " ")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n\s*\n\s*\n+", "\n\n", text)

    return text.strip()


def extract_ancestors(page):
    return [
        {"id": ancestor["id"], "title": ancestor["title"]}
        for ancestor in page.get("ancestors", [])
        if isinstance(ancestor, dict) and ancestor.get("id") and ancestor.get("title")
    ]


def extract_labels(page):
    results = page.get("metadata", {}).get("labels", {}).get("results", [])
    return [
        label["name"]
        for label in results
        if isinstance(label, dict) and label.get("name")
    ]


def build_page_record(page):
    ancestors = extract_ancestors(page)
    content_html = page.get("body", {}).get("storage", {}).get("value", "")
    version = page.get("version", {})

    return {
        "id": page["id"],
        "title": page["title"],
        "content": confluence_storage_to_text(content_html),
        "content_html": content_html,
        "ancestors": ancestors,
        "parent_id": ancestors[-1]["id"] if ancestors else None,
        "space_key": dlt.config["sources.confluence.space_key"],
        "space_name": page.get("space", {}).get("name", ""),
        "labels": extract_labels(page),
        "version": version.get("number", 0),
        "created": version.get("when", ""),
        "updated": version.get("when", ""),
    }


@dlt.source
def confluence_source():
    return rest_api_source({
        "client": {
            "base_url": dlt.config["sources.confluence.base_url"],
            "auth": {
                "type": "http_basic",
                "username": dlt.secrets["sources.confluence.username"],
                "password": dlt.secrets["sources.confluence.password"]
            }
        },
        "resources": [
            {
                "name": "pages",
                "endpoint": {
                    "path": "/wiki/rest/api/content",
                    "params": {
                        "spaceKey": dlt.config["sources.confluence.space_key"],
                        "expand": dlt.config["sources.confluence.expand"],
                        "limit": PAGE_LIMIT
                    },
                    "paginator": {
                        "type": "offset",
                        "limit": PAGE_LIMIT,
                        "offset_param": "start",
                        "limit_param": "limit",
                        "total_path": None
                    },
                    "data_selector": "results"
                },
                "write_disposition": "replace"
            }
        ]
    }).pages


@dlt.transformer(primary_key="id", write_disposition="merge")
def process_pages(page):
    try:
        yield build_page_record(dict(page))
    except Exception as exc:
        logger.error("Error processing page: %s", exc)


@dlt.transformer(primary_key=("page_id", "ancestor_id"), write_disposition="merge")
def process_hierarchy(page):
    try:
        page = dict(page)
        page_id = page["id"]

        for level, ancestor in enumerate(page.get("ancestors", []), start=1):
            if not isinstance(ancestor, dict) or not ancestor.get("id"):
                continue

            yield {
                "page_id": page_id,
                "ancestor_id": ancestor["id"],
                "level": level,
            }
    except Exception as exc:
        logger.error("Error processing hierarchy: %s", exc)
