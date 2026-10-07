import dlt
from typing import Iterable, Optional, List
import requests
import time
import logging

logger = logging.getLogger(__name__)

def fetch_issue_comments(subdomain: str, email: str, api_token: str, issue_id_or_key: str, page_size: int = 1000) -> Iterable[dict]:
    """Fetch all comments for a given issue via Jira API with retry logic for rate limiting."""
    url = f"https://{subdomain}.atlassian.net/rest/api/3/issue/{issue_id_or_key}/comment"
    auth = (email, api_token)
    headers = {"Accept": "application/json"}
    start_at = 0
    max_retries = 5
    base_delay = 1  # Start with 1 second delay
    
    while True:
        params = {"startAt": start_at, "maxResults": page_size}
        
        # Retry logic for rate limiting (429 errors)
        for attempt in range(max_retries):
            try:
                resp = requests.get(url, auth=auth, headers=headers, params=params)
                
                # Handle rate limiting
                if resp.status_code == 429:
                    retry_after = int(resp.headers.get("Retry-After", base_delay * (2 ** attempt)))
                    logger.warning(f"⚠️  Rate limited (429) for issue {issue_id_or_key}, retrying after {retry_after}s (attempt {attempt + 1}/{max_retries})")
                    time.sleep(retry_after)
                    continue
                
                resp.raise_for_status()
                data = resp.json()
                comments = data.get("comments", [])
                
                # Small delay between requests to avoid hitting rate limits
                time.sleep(0.1)
                
                for comment in comments:
                    yield comment
                    
                if start_at + len(comments) >= data.get("total", 0):
                    return
                start_at += len(comments)
                break  # Success, exit retry loop
                
            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 429 and attempt < max_retries - 1:
                    retry_after = int(e.response.headers.get("Retry-After", base_delay * (2 ** attempt)))
                    logger.warning(f"⚠️  Rate limited (429) for issue {issue_id_or_key}, retrying after {retry_after}s (attempt {attempt + 1}/{max_retries})")
                    time.sleep(retry_after)
                    continue
                else:
                    raise  # Re-raise if not rate limit or out of retries
