import dlt
from dlt.sources.rest_api import rest_api_source
from html import unescape
import logging
import re
import sys

# Suppress verbose logging from dlt and HTTP libraries
logging.basicConfig(
    level=logging.ERROR,
    format='%(levelname)s [%(filename)s:%(lineno)d] %(message)s'
)

# Suppress specific loggers
logging.getLogger('dlt').setLevel(logging.ERROR)
logging.getLogger('dlt.sources').setLevel(logging.ERROR)
logging.getLogger('urllib3').setLevel(logging.ERROR)
logging.getLogger('requests').setLevel(logging.ERROR)
logging.getLogger('httpx').setLevel(logging.ERROR)

logger = logging.getLogger(__name__)
logger.setLevel(logging.ERROR)


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

@dlt.source
def confluence_source():
    # Define the pages resource with minimal verbosity
    pages_resource = rest_api_source({
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
                        "expand": "body.storage,space,metadata.labels,ancestors,version",
                        "limit": 100
                    },
                    "paginator": {
                        "type": "offset",
                        "limit": 100,
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

    return pages_resource


# Define transformers as separate functions
@dlt.transformer(primary_key="id", write_disposition="merge")
def process_pages(page):
    # Process page data silently
    try:
        # Convert PageData to dict if needed
        if hasattr(page, '__dict__'):
            page_dict = vars(page)
        elif hasattr(page, 'items'):
            page_dict = dict(page)
        elif isinstance(page, str):
            # If it's a string, try to parse it as JSON
            import json
            page_dict = json.loads(page)
        else:
            logger.error(f"Unexpected page type: {type(page)}")
            return
        
        # Access fields using dict
        ancestors = []
        if 'ancestors' in page_dict:
            for a in page_dict['ancestors']:
                if isinstance(a, dict) and 'id' in a and 'title' in a:
                    ancestors.append({'id': a['id'], 'title': a['title']})
        
        label_names = []
        if 'metadata' in page_dict and 'labels' in page_dict['metadata']:
            labels_obj = page_dict['metadata']['labels']
            if isinstance(labels_obj, dict) and 'results' in labels_obj:
                for label in labels_obj['results']:
                    if isinstance(label, dict) and 'name' in label:
                        label_names.append(label['name'])
        
        content_html = ''
        if 'body' in page_dict and 'storage' in page_dict['body'] and 'value' in page_dict['body']['storage']:
            content_html = page_dict['body']['storage']['value']
        
        space_name = ''
        if 'space' in page_dict and 'name' in page_dict['space']:
            space_name = page_dict['space']['name']
        
        version_number = 0
        version_when = ''
        if 'version' in page_dict:
            if 'number' in page_dict['version']:
                version_number = page_dict['version']['number']
            if 'when' in page_dict['version']:
                version_when = page_dict['version']['when']
        
        yield {
            'id': page_dict['id'],
            'title': page_dict['title'],
            'content': confluence_storage_to_text(content_html),
            'content_html': content_html,
            'ancestors': ancestors,
            'parent_id': ancestors[-1]['id'] if ancestors else None,
            'space_key': dlt.config["sources.confluence.space_key"],
            'space_name': space_name,
            'labels': label_names,
            'version': version_number,
            'created': version_when,
            'updated': version_when
        }
    except Exception as e:
        logger.error(f"Error processing page: {e}")
        pass


@dlt.transformer(primary_key=("page_id", "ancestor_id"), write_disposition="merge")
def process_hierarchy(page):
    # Now receiving individual page objects, not a list
    try:
        # Convert PageData to dict if needed
        if hasattr(page, '__dict__'):
            page_dict = vars(page)
        elif hasattr(page, 'items'):
            page_dict = dict(page)
        elif isinstance(page, str):
            # If it's a string, try to parse it as JSON
            import json
            page_dict = json.loads(page)
        else:
            return
        
        ancestors = page_dict.get('ancestors', [])
        page_id = page_dict['id']
        
        for level, ancestor in enumerate(ancestors):
            if isinstance(ancestor, dict) and 'id' in ancestor:
                yield {
                    "page_id": page_id,
                    "ancestor_id": ancestor['id'],
                    "level": level + 1
                }
    except Exception as e:
        logger.error(f"Error processing hierarchy: {e}")
        pass
