import dlt
from dlt.sources.rest_api import RESTAPIConfig, rest_api_resources
from collections.abc import Mapping
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

    # Normalize whitespace for embedding/query friendliness.
    text = text.replace("\xa0", " ")
    text = re.sub(r"[ \t]+", " ", text)
    # Collapse excessive line breaks and trim around newlines.
    text = re.sub(r"\s*\n\s*", "\n", text)
    text = re.sub(r"\n{2,}", "\n", text)
    # Store mostly single-line text to avoid noisy '\n' in downstream tables.
    text = text.replace("\n", " ")
    text = re.sub(r"\s{2,}", " ", text)

    return text.strip()


def _normalize_page_record(record):
    """Normalize incoming transformer items to a plain dict."""
    if isinstance(record, Mapping):
        return dict(record)

    # Some wrappers may be namedtuple-like.
    asdict = getattr(record, "_asdict", None)
    if callable(asdict):
        try:
            asdict_value = asdict()
            if isinstance(asdict_value, Mapping):
                return dict(asdict_value)
        except Exception:
            pass

    # Mapping-like objects that do not subclass Mapping.
    keys = getattr(record, "keys", None)
    getitem = getattr(record, "__getitem__", None)
    if callable(keys) and callable(getitem):
        try:
            return {k: record[k] for k in record.keys()}
        except Exception:
            pass

    # Some resource wrappers may pass a (name, payload) tuple.
    if isinstance(record, tuple) and len(record) == 2 and isinstance(record[1], Mapping):
        return dict(record[1])

    # dlt rest_api wrappers (e.g. PageData) may store payload in attributes.
    for attr_name in ("data", "item", "record", "value", "payload", "page"):
        attr_value = getattr(record, attr_name, None)
        if isinstance(attr_value, Mapping):
            return dict(attr_value)

    # Last resort: scan object attributes for a mapping payload.
    try:
        for attr_value in vars(record).values():
            if isinstance(attr_value, Mapping):
                return dict(attr_value)
    except Exception:
        pass

    raise TypeError(f"Unsupported page record type: {type(record).__name__}")


def _extract_page_payload(record):
    """Return a Confluence page dict (must contain id) or None."""
    page = _normalize_page_record(record)
    if "id" in page:
        return page

    # Some wrappers normalize to envelope dicts; unwrap likely payload fields.
    for key in ("data", "item", "record", "value", "payload", "page", "result"):
        nested = page.get(key)
        if isinstance(nested, Mapping) and nested.get("id"):
            return dict(nested)

    # Generic one-level scan for any nested mapping/list item with an id.
    for value in page.values():
        if isinstance(value, Mapping) and value.get("id"):
            return dict(value)
        if isinstance(value, list):
            for item in value:
                if isinstance(item, Mapping) and item.get("id"):
                    return dict(item)

    return None


def _iter_page_payloads(record):
    """Yield zero or more page dicts from single records or paged batches."""
    single_page = _extract_page_payload(record)
    if single_page is not None:
        yield single_page
        return

    if isinstance(record, Mapping):
        return

    try:
        for item in record:
            page = _extract_page_payload(item)
            if page is not None:
                yield page
    except TypeError:
        return


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
        "space_key": dlt.config["sources.atlassian_confluence.space_key"],
        "space_name": page.get("space", {}).get("name", ""),
        "labels": extract_labels(page),
        "version": version.get("number", 0),
        "created": version.get("when", ""),
        "updated": version.get("when", ""),
    }


@dlt.source
def atlassian_confluence_source(
    base_url=None,
    space_key=None,
    expand=None,
    username=None,
    password=None,
):
    """dlt-style Confluence source using RESTAPIConfig/rest_api_resources."""
    base_url = base_url or dlt.config["sources.atlassian_confluence.base_url"]
    space_key = space_key or dlt.config["sources.atlassian_confluence.space_key"]
    expand = expand or dlt.config["sources.atlassian_confluence.expand"]
    username = username or dlt.secrets["sources.atlassian_confluence.username"]
    password = password or dlt.secrets["sources.atlassian_confluence.password"]

    config: RESTAPIConfig = {
        "client": {
            "base_url": base_url,
            "auth": {
                "type": "http_basic",
                "username": username,
                "password": password,
            },
        },
        "resources": [
            {
                "name": "pages",
                "endpoint": {
                    "path": "/wiki/rest/api/content",
                    "params": {
                        "spaceKey": space_key,
                        "expand": expand,
                        "limit": PAGE_LIMIT,
                    },
                    "paginator": {
                        "type": "offset",
                        "limit": PAGE_LIMIT,
                        "offset_param": "start",
                        "limit_param": "limit",
                        "total_path": None,
                    },
                    "data_selector": "results",
                },
                "write_disposition": "replace",
            }
        ],
    }
    yield from rest_api_resources(config)


def confluence_source():
    """Backward-compatible helper returning only the pages resource."""
    return atlassian_confluence_source().pages


@dlt.transformer(primary_key="id", write_disposition="merge")
def process_pages(page):
    yielded = False
    for page_payload in _iter_page_payloads(page):
        yielded = True
        yield build_page_record(page_payload)
    if not yielded:
        logger.debug("Skipping non-page item in process_pages")


@dlt.transformer(primary_key=("page_id", "ancestor_id"), write_disposition="merge")
def process_hierarchy(page):
    yielded = False
    for page_payload in _iter_page_payloads(page):
        yielded = True
        page_id = page_payload["id"]
        for level, ancestor in enumerate(page_payload.get("ancestors", []), start=1):
            if not isinstance(ancestor, dict) or not ancestor.get("id"):
                continue
            yield {
                "page_id": page_id,
                "ancestor_id": ancestor["id"],
                "level": level,
            }
    if not yielded:
        logger.debug("Skipping non-page item in process_hierarchy")
