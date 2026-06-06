"""
Export Confluence (and optionally Jira) pages from MotherDuck to Obsidian vault.

Usage:
    uv run export_to_obsidian.py

Each project defined in secrets.toml gets its Confluence pages written to:
    <OBSIDIAN_VAULT>/current-work/<project_path>/confluence/<space_key>/<title>.md

Attachments are downloaded to:
    <OBSIDIAN_VAULT>/current-work/<project_path>/confluence/<space_key>/attachments/

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
OBSIDIAN_VAULT = "/Users/jitsejan/Documents/ObsidiJan"

VAULT_PATHS = {
    "nged": "current-work/vivanti/national-grid",
    "orbis": "current-work/orbis",
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

    print("Done.")


if __name__ == "__main__":
    main()
