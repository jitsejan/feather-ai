"""
Export Confluence and Jira data from MotherDuck to Obsidian vault.

Usage:
    uv run export_to_obsidian.py

Confluence pages are written to:
    <OBSIDIAN_VAULT>/current-work/<project_path>/confluence/<space_key>/<title>.md

Jira issues are written to:
    <OBSIDIAN_VAULT>/current-work/<project_path>/jira/<issue_key>.md

Project-to-vault-path mapping is defined in VAULT_PATHS below.
"""
import os
import re
import dlt
import duckdb
import requests
from markdownify import markdownify
from requests.auth import HTTPBasicAuth

from project_config import load_all_projects, ProjectConfig

MOTHERDUCK_DB = "feather_ai"
OBSIDIAN_VAULT = "/Users/jitsejan/Documents/VAULT"

VAULT_PATHS = {
    "nged": "current-work/REDACTED/REDACTED",
    REDACTED,
}


def safe_filename(title: str) -> str:
    return re.sub(r'[<>:"/\\|?*]', "-", title).strip()


def download_attachment(
    base_url: str,
    auth: HTTPBasicAuth,
    page_id: str,
    filename: str,
    attachments_dir: str,
) -> bool:
    """Download a Confluence attachment to attachments_dir. Returns True on success."""
    dest = os.path.join(attachments_dir, filename)
    if os.path.exists(dest):
        return True

    # Find the attachment download URL
    resp = requests.get(
        f"{base_url}/wiki/rest/api/content/{page_id}/child/attachment",
        params={"filename": filename, "expand": "version"},
        auth=auth,
        timeout=30,
    )
    if not resp.ok:
        return False

    results = resp.json().get("results", [])
    if not results:
        return False

    attachment_id = results[0].get("id", "")
    if not attachment_id:
        return False

    file_resp = requests.get(
        f"{base_url}/wiki/rest/api/content/{page_id}/child/attachment/{attachment_id}/download",
        auth=auth,
        timeout=60,
        stream=True,
    )
    if not file_resp.ok:
        return False

    os.makedirs(attachments_dir, exist_ok=True)
    with open(dest, "wb") as f:
        for chunk in file_resp.iter_content(chunk_size=8192):
            f.write(chunk)
    return True


def confluence_html_to_markdown(
    html: str,
    page_id: str,
    attachments_dir: str,
    base_url: str,
    auth: HTTPBasicAuth,
) -> str:
    """Convert Confluence storage-format HTML to Obsidian markdown with wikilinks and embedded images."""
    if not html:
        return ""

    # Replace ac:image blocks with Obsidian embed syntax
    def replace_ac_image(m):
        block = m.group(0)
        filename_m = re.search(r'ri:filename="([^"]+)"', block)
        if not filename_m:
            return ""
        filename = filename_m.group(1)
        caption_m = re.search(r'<ac:caption[^>]*>(.*?)</ac:caption>', block, re.DOTALL)
        caption = re.sub(r'<[^>]+>', '', caption_m.group(1)).strip() if caption_m else ""
        download_attachment(base_url, auth, page_id, filename, attachments_dir)
        embed = f"![[attachments/{filename}]]"
        if caption:
            embed += f"\n*{caption}*"
        return embed

    html = re.sub(r'<ac:image\b[^>]*>.*?</ac:image>', replace_ac_image, html, flags=re.DOTALL)

    # Replace ac:link blocks with Obsidian wikilinks.
    # Only link to pages in the same space (no ri:space-key attribute).
    def replace_ac_link(m):
        block = m.group(0)
        if 'ri:space-key' in block:
            title_m = re.search(r'ri:content-title="([^"]+)"', block)
            return title_m.group(1) if title_m else ""
        title_m = re.search(r'ri:content-title="([^"]+)"', block)
        if not title_m:
            return ""
        title = title_m.group(1)
        label_m = re.search(r'<ac:link-body>(.*?)</ac:link-body>', block, re.DOTALL)
        label = label_m.group(1).strip() if label_m else ""
        if label and label != title:
            return f"[[{title}|{label}]]"
        return f"[[{title}]]"

    html = re.sub(r'<ac:link\b[^>]*>.*?</ac:link>', replace_ac_link, html, flags=re.DOTALL)

    md = markdownify(html, heading_style="ATX", strip=["ac:structured-macro"])
    md = re.sub(r'\n{3,}', '\n\n', md)
    return md.strip()


def export_confluence(con, project: ProjectConfig, vault_path: str):
    auth = HTTPBasicAuth(project.username, project.password)
    rows = con.execute(f"""
        SELECT id, title, content_html, space_key, updated
        FROM {project.confluence_dataset}.process_pages
    """).fetchall()

    for page_id, title, content_html, space_key, updated in rows:
        folder = os.path.join(OBSIDIAN_VAULT, vault_path, "confluence", space_key)
        attachments_dir = os.path.join(folder, "attachments")
        os.makedirs(folder, exist_ok=True)

        filepath = os.path.join(folder, f"{safe_filename(title)}.md")
        with open(filepath, "w") as f:
            f.write(f"---\n")
            f.write(f"title: \"{title}\"\n")
            f.write(f"source: confluence\n")
            f.write(f"project: {project.name}\n")
            f.write(f"space: {space_key}\n")
            f.write(f"updated: {updated}\n")
            f.write(f"---\n\n")
            f.write(confluence_html_to_markdown(
                content_html, page_id, attachments_dir, project.base_url, auth
            ))

    print(f"  confluence: {len(rows)} pages written")


def build_confluence_page_map(con, dataset: str) -> dict:
    """Return a dict of {page_id: title} for wikilink resolution."""
    try:
        rows = con.execute(f"SELECT id, title FROM {dataset}.process_pages").fetchall()
        return {row[0]: row[1] for row in rows}
    except Exception:
        return {}


def clean_jira_body(text: str, confluence_page_map: dict, jira_base_url: str, confluence_base_url: str) -> str:
    """Convert Jira wiki markup in comment/description bodies to clean markdown."""
    if not text:
        return ""

    # Remove user mentions [~accountid:xxx]
    text = re.sub(r'\[~accountid:[^\]]+\]', '', text)

    # Remove inline Jira images !filename|...!
    text = re.sub(r'!\S+\|[^!]*!', '', text)
    text = re.sub(r'!\S+\.(?:png|jpg|gif|jpeg)!', '', text)

    # Convert smart-links [label|url|smart-link] or [url|url|smart-link]
    def replace_smart_link(m):
        label, url = m.group(1).strip(), m.group(2).strip()

        # Jira issue link — check if it's in our project
        jira_issue_m = re.search(r'/browse/([A-Z]+-\d+)', url)
        if jira_issue_m:
            issue_key = jira_issue_m.group(1)
            return f"[[{issue_key}]]"

        # Confluence page link — try to resolve to a wikilink by page ID
        confluence_page_m = re.search(r'/pages/(\d+)', url)
        if confluence_page_m:
            page_id = confluence_page_m.group(1)
            page_title = confluence_page_map.get(page_id)
            if page_title:
                return f"[[{page_title}]]"
            # Fall back to markdown link with decoded label
            display = label if label != url else "Confluence page"
            return f"[{display}]({url})"

        # Generic external link
        display = label if label != url else url
        return f"[{display}]({url})"

    text = re.sub(r'\[([^\]]+)\|([^\]]+)\|smart-link\]', replace_smart_link, text)

    # Also handle plain [label|url] Jira wiki links (no smart-link suffix)
    def replace_wiki_link(m):
        label, url = m.group(1).strip(), m.group(2).strip()
        if not url.startswith('http'):
            return label
        jira_issue_m = re.search(r'/browse/([A-Z]+-\d+)', url)
        if jira_issue_m:
            return f"[[{jira_issue_m.group(1)}]]"
        confluence_page_m = re.search(r'/pages/(\d+)', url)
        if confluence_page_m:
            page_title = confluence_page_map.get(confluence_page_m.group(1))
            if page_title:
                return f"[[{page_title}]]"
        return f"[{label}]({url})"

    text = re.sub(r'\[([^\]|]+)\|([^\]]+)\]', replace_wiki_link, text)

    # Clean up extra whitespace
    text = re.sub(r'\n{3,}', '\n\n', text)
    return text.strip()


def export_jira(con, project: ProjectConfig, vault_path: str):
    dataset = project.jira_dataset
    folder = os.path.join(OBSIDIAN_VAULT, vault_path, "jira")
    os.makedirs(folder, exist_ok=True)

    # Build Confluence page ID → title map for wikilink resolution
    confluence_page_map = build_confluence_page_map(con, project.confluence_dataset)

    # Load labels, subtasks and components keyed by _dlt_root_id
    def load_child_table(table: str) -> dict:
        try:
            rows = con.execute(f"SELECT _dlt_root_id, value FROM {dataset}.{table}").fetchall()
            result = {}
            for root_id, value in rows:
                result.setdefault(root_id, []).append(value)
            return result
        except Exception:
            return {}

    labels_map = load_child_table("process_issues__labels")
    subtasks_map = load_child_table("process_issues__subtask_keys")
    components_map = load_child_table("process_issues__components")

    # Load comments keyed by _dlt_root_id, sorted by created
    comments_rows = con.execute(f"""
        SELECT _dlt_root_id, author, body, created
        FROM {dataset}.process_issues__comments
        ORDER BY created ASC
    """).fetchall()
    comments_map = {}
    for root_id, author, body, created in comments_rows:
        comments_map.setdefault(root_id, []).append((author, body, created))

    # Load all issues
    issues = con.execute(f"""
        SELECT _dlt_id, id, key, summary, description, status, issue_type, priority,
               assignee, reporter, created, updated, sprint_name, sprint_state,
               parent_key, parent_summary
        FROM {dataset}.process_issues
        ORDER BY key
    """).fetchall()

    for (dlt_id, issue_id, key, summary, description, status, issue_type, priority,
         assignee, reporter, created, updated, sprint_name, sprint_state,
         parent_key, parent_summary) in issues:

        labels = labels_map.get(dlt_id, [])
        subtasks = subtasks_map.get(dlt_id, [])
        components = components_map.get(dlt_id, [])
        comments = comments_map.get(dlt_id, [])

        filepath = os.path.join(folder, f"{key}.md")
        with open(filepath, "w") as f:
            # Frontmatter
            f.write("---\n")
            f.write(f"key: {key}\n")
            f.write(f"summary: \"{summary}\"\n")
            f.write(f"source: jira\n")
            f.write(f"project: {project.name}\n")
            f.write(f"type: {issue_type}\n")
            f.write(f"status: {status}\n")
            f.write(f"priority: {priority}\n")
            f.write(f"assignee: {assignee or ''}\n")
            f.write(f"reporter: {reporter or ''}\n")
            if sprint_name:
                f.write(f"sprint: \"{sprint_name}\"\n")
            if parent_key:
                f.write(f"epic: \"[[{parent_key}]]\"\n")
            if labels:
                f.write(f"labels: [{', '.join(labels)}]\n")
            if components:
                f.write(f"components: [{', '.join(components)}]\n")
            f.write(f"created: {created}\n")
            f.write(f"updated: {updated}\n")
            f.write("---\n\n")

            # Title
            f.write(f"# {key} — {summary}\n\n")

            # Parent link
            if parent_key:
                f.write(f"**{issue_type} of:** [[{parent_key}]] — {parent_summary}\n\n")

            # Subtasks
            if subtasks:
                f.write("## Subtasks\n\n")
                for st in subtasks:
                    f.write(f"- [[{st}]]\n")
                f.write("\n")

            # Description
            if description:
                f.write("## Description\n\n")
                f.write(clean_jira_body(description, confluence_page_map, project.base_url, project.base_url))
                f.write("\n\n")

            # Comments (full history)
            if comments:
                f.write("## Comments\n\n")
                for author, body, created_dt in comments:
                    f.write(f"### {author} — {created_dt.strftime('%Y-%m-%d %H:%M') if hasattr(created_dt, 'strftime') else created_dt}\n\n")
                    f.write(clean_jira_body(body, confluence_page_map, project.base_url, project.base_url))
                    f.write("\n\n")

    print(f"  jira: {len(issues)} issues written")


def main():
    credentials = dlt.secrets["destination.motherduck.credentials"]
    con = duckdb.connect(credentials)
    projects = load_all_projects()

    for project in projects:
        vault_path = VAULT_PATHS.get(project.name)
        if not vault_path:
            print(f"Skipping '{project.name}' — no vault path configured in VAULT_PATHS")
            continue

        print(f"Exporting {project.name}...")
        export_confluence(con, project, vault_path)
        export_jira(con, project, vault_path)

    print("Done.")


if __name__ == "__main__":
    main()
