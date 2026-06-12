"""
Export Confluence, Jira and ADO wiki data from MotherDuck to an Obsidian vault.

Usage:
    uv run export_to_obsidian.py

Per-project vault paths and labels are configured in .dlt/secrets.toml:

    [projects.myproject]
    obsidian_vault_path = "folder/inside/vault"
    obsidian_label      = "Display Name"   # used in weekly note

    [obsidian]
    vault   = "/path/to/ObsidianVault"
    my_name = "Your Name"

Projects without obsidian_vault_path are skipped.
"""
import os
import re
import dlt
import duckdb
import requests
from datetime import date
from markdownify import markdownify
from requests.auth import HTTPBasicAuth

from project_config import load_all_projects, load_obsidian_config, ProjectConfig
from extract_azure_devops_wiki import download_ado_attachment, _auth_header

MOTHERDUCK_DB = "feather_ai"


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


def ado_wiki_md_to_obsidian(content: str) -> str:
    """Rewrite ADO wiki markdown to Obsidian syntax:
    - Image attachments  → ![[attachments/filename]]
    - Internal page links → [[Page Title|label]]
    - External links are left untouched.
    """
    if not content:
        return ""

    def replace_image(m):
        alt = m.group(1)
        src = m.group(2)
        att_match = re.search(r'\.attachments/(.+)', src)
        if not att_match:
            return m.group(0)
        filename = att_match.group(1)
        embed = f"![[attachments/{filename}]]"
        if alt:
            embed += f"\n*{alt}*"
        return embed

    content = re.sub(r'!\[([^\]]*)\]\(([^\)]+)\)', replace_image, content)

    def replace_page_link(m):
        label = m.group(1)
        target = m.group(2)
        if target.startswith("http") or ".attachments" in target:
            return m.group(0)
        page_title = target.rstrip("/").split("/")[-1].replace("-", " ")
        if label == page_title:
            return f"[[{page_title}]]"
        return f"[[{page_title}|{label}]]"

    content = re.sub(r'\[([^\]]+)\]\((/[^\)]+)\)', replace_page_link, content)
    return content


def export_ado_wiki(con, project: ProjectConfig, vault_path: str, obsidian_vault: str) -> None:
    if not project.has_ado:
        return

    dataset = project.ado_dataset
    try:
        rows = con.execute(
            f"SELECT id, wiki_name, path, title, content FROM {dataset}.pages"
        ).fetchall()
    except Exception as e:
        print(f"  ado_wiki: could not read pages — {e}")
        return

    headers = _auth_header(project.ado_pat)
    org_url = project.ado_org_url.rstrip("/")
    ado_project = project.ado_project

    try:
        att_rows = con.execute(
            f"SELECT repo_id, git_path, name FROM {dataset}.attachments"
        ).fetchall()
    except Exception:
        att_rows = []

    for _id, wiki_name, page_path, title, content in rows:
        page_parts = [p for p in page_path.strip("/").split("/") if p]
        if not page_parts:
            page_parts = [safe_filename(wiki_name)]

        folder = os.path.join(
            obsidian_vault, vault_path, "ado", safe_filename(wiki_name),
            *[safe_filename(p) for p in page_parts[:-1]]
        )
        attachments_dir = os.path.join(
            obsidian_vault, vault_path, "ado", safe_filename(wiki_name), "attachments"
        )
        os.makedirs(folder, exist_ok=True)

        for repo_id, git_path, att_name in att_rows:
            dest = os.path.join(attachments_dir, att_name)
            if not os.path.exists(dest):
                os.makedirs(os.path.dirname(dest), exist_ok=True)
                download_ado_attachment(org_url, ado_project, repo_id, git_path, dest, headers)

        filepath = os.path.join(folder, safe_filename(page_parts[-1]) + ".md")
        with open(filepath, "w") as f:
            f.write("---\n")
            f.write(f"title: \"{title}\"\n")
            f.write(f"source: ado_wiki\n")
            f.write(f"project: {project.name}\n")
            f.write(f"wiki: {wiki_name}\n")
            f.write(f"path: {page_path}\n")
            f.write("---\n\n")
            f.write(ado_wiki_md_to_obsidian(content or ""))

    print(f"  ado_wiki: {len(rows)} pages written")


def export_confluence(con, project: ProjectConfig, vault_path: str, obsidian_vault: str):
    if not project.base_url or not project.confluence_space_key:
        return
    auth = HTTPBasicAuth(project.username, project.password)
    try:
        rows = con.execute(f"""
            SELECT id, title, content_html, space_key, updated
            FROM {project.confluence_dataset}.process_pages
        """).fetchall()
    except Exception as e:
        print(f"  confluence: could not read pages — {e}")
        return

    for page_id, title, content_html, space_key, updated in rows:
        folder = os.path.join(obsidian_vault, vault_path, "confluence", space_key)
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


def export_jira(con, project: ProjectConfig, vault_path: str, obsidian_vault: str, my_name: str):
    if not project.base_url or not project.jira_board_id:
        return
    dataset = project.jira_dataset
    folder = os.path.join(obsidian_vault, vault_path, "jira")
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


def _parse_checked_tickets(filepath: str) -> set:
    """Read an existing weekly note and return the set of ticket IDs that are checked off.

    Handles both formats:
      - [x] [HMP-187 — …](url)          ← link-style
      - [x] 25116 — …                   ← plain text
    """
    checked = set()
    if not os.path.exists(filepath):
        return checked
    # Match:  - [x] followed by optional [  then the ticket id
    pattern = re.compile(r'- \[x\].*?[\[\s](\w+-\d+|\d{4,})\b', re.IGNORECASE)
    with open(filepath) as fh:
        for line in fh:
            m = pattern.search(line)
            if m:
                checked.add(m.group(1))
    return checked


def generate_weekly_note(con, projects: list, obsidian_vault: str, my_name: str, my_name_ado: str):
    today = date.today()
    week_str = today.strftime("%Y-W%V")  # e.g. 2026-W24
    folder = os.path.join(obsidian_vault, "current-work", "weekly")
    os.makedirs(folder, exist_ok=True)
    filepath = os.path.join(folder, f"{week_str}.md")

    # Read previously ticked checkboxes so we don't lose manual state
    checked_tickets = _parse_checked_tickets(filepath)

    # Collect data per project — Jira
    project_data = []
    for project in projects:
        dataset = project.jira_dataset
        label = project.obsidian_label or project.name

        try:
            active_issues = con.execute(f"""
                SELECT key, summary, status, assignee, priority, parent_key, sprint_name
                FROM {dataset}.process_issues
                WHERE (sprint_state = 'active'
                   OR (sprint_name IS NULL AND status_category = 'In Progress'))
                  AND status_category != 'Done'
                ORDER BY assignee, status
            """).fetchall()

            sprint_name = next(
                (r[6] for r in active_issues if r[6]),
                "Kanban"
            )

            active_keys = {r[0] for r in active_issues}
            my_all_issues = con.execute(f"""
                SELECT key, summary, status, assignee, priority, parent_key, sprint_name
                FROM {dataset}.process_issues
                WHERE assignee = '{my_name}'
                  AND status_category != 'Done'
                ORDER BY status_category, status
            """).fetchall()
            my_extra_issues = [r for r in my_all_issues if r[0] not in active_keys]

            project_data.append({
                "label": label,
                "name": project.name,
                "sprint": sprint_name,
                "issues": active_issues,
                "my_extra_issues": my_extra_issues,
            })
        except Exception as e:
            project_data.append({
                "label": label,
                "name": project.name,
                "sprint": None,
                "issues": [],
                "my_extra_issues": [],
                "error": str(e),
            })

    # Collect ADO boards data per project
    ado_board_data = []
    for project in projects:
        if not project.has_ado_boards:
            continue
        dataset = project.ado_boards_dataset
        label = project.obsidian_label or project.name
        org_url = (project.ado_org_url or "").rstrip("/")
        ado_project = project.ado_boards_project or ""
        try:
            sprint_row = con.execute(f"""
                SELECT name FROM {dataset}.iterations WHERE time_frame = 'current' LIMIT 1
            """).fetchone()
            current_sprint = sprint_row[0] if sprint_row else None

            all_items = con.execute(f"""
                SELECT id, title, work_item_type, state, assigned_to, parent_id, sprint_name
                FROM {dataset}.work_items
                WHERE state != 'Closed'
                ORDER BY assigned_to, state
            """).fetchall()

            my_items = [r for r in all_items if r[4] == my_name_ado]

            ado_board_data.append({
                "label": label,
                "name": project.name,
                "sprint": current_sprint,
                "issues": all_items,
                "my_issues": my_items,
                "org_url": org_url,
                "ado_project": ado_project,
            })
        except Exception as e:
            ado_board_data.append({
                "label": label,
                "name": project.name,
                "sprint": None,
                "issues": [],
                "my_issues": [],
                "org_url": org_url,
                "ado_project": ado_project,
                "error": str(e),
            })

    with open(filepath, "w") as f:
        f.write(f"---\n")
        f.write(f"date: {today.isoformat()}\n")
        f.write(f"week: {week_str}\n")
        f.write(f"---\n\n")
        f.write(f"# Week {week_str}\n\n")

        # Section 1: My active tickets — checkbox list, state preserved across re-exports
        f.write(f"## 🔴 My tickets\n\n")
        has_my_tickets = False

        # Jira — active sprint tickets assigned to me (exclude Done)
        for pd in project_data:
            base_url = next((p.base_url for p in projects if p.name == pd["name"]), "") or ""
            my_active = [
                r for r in pd["issues"]
                if r[3] == my_name and r[2] not in ("Done", "Closed", "Resolved")
            ]
            if not my_active:
                continue
            f.write(f"### {pd['label']}\n\n")
            for key, summary, status, assignee, priority, parent_key, sprint_name in my_active:
                tick = "x" if key in checked_tickets else " "
                label_text = f"{key} — {summary}"
                link = f"[{label_text}]({base_url}/browse/{key})" if base_url else label_text
                f.write(f"- [{tick}] {link} `{status}`\n")
                has_my_tickets = True
            f.write("\n")

        # ADO boards — active items assigned to me (exclude Closed/Resolved)
        for pd in ado_board_data:
            org_url = pd.get("org_url", "")
            ado_project = pd.get("ado_project", "")
            my_active_ado = [
                r for r in pd["my_issues"]
                if r[3] not in ("Closed", "Resolved", "Done")
            ]
            if not my_active_ado:
                continue
            f.write(f"### {pd['label']}\n\n")
            for item_id, title, wtype, state, assignee, parent_id, sprint in my_active_ado:
                tick = "x" if str(item_id) in checked_tickets else " "
                label_text = f"{item_id} — {title}"
                link = f"[{label_text}]({org_url}/{ado_project}/_workitems/edit/{item_id})" if org_url else label_text
                f.write(f"- [{tick}] {link} `{state}`\n")
                has_my_tickets = True
            f.write("\n")

        if not has_my_tickets:
            f.write("_No active tickets assigned to you this week._\n\n")

        # Section 2: Full active sprint per project
        f.write(f"## 📋 Active sprints\n\n")
        for pd in project_data:
            sprint_label = pd["sprint"] or "Kanban"
            total = len(pd["issues"])
            f.write(f"### {pd['label']} — {sprint_label} ({total} issues)\n\n")

            if not pd["issues"]:
                f.write("_No active issues._\n\n")
                continue

            f.write("| Key | Summary | Assignee | Status |\n")
            f.write("|-----|---------|----------|--------|\n")
            for key, summary, status, assignee, priority, parent_key, sprint_name in pd["issues"]:
                assignee_short = assignee.split()[0] if assignee else "—"
                bold_open = "**" if assignee == my_name else ""
                bold_close = "**" if assignee == my_name else ""
                f.write(f"| {bold_open}[[{key}]]{bold_close} | {bold_open}{summary}{bold_close} | {assignee_short} | {status} |\n")
            f.write("\n")

        # ADO boards — full sprint
        for pd in ado_board_data:
            sprint_label = pd["sprint"] or "Current Sprint"
            total = len(pd["issues"])
            f.write(f"### {pd['label']} — {sprint_label} ({total} items, ADO)\n\n")
            if not pd["issues"]:
                f.write("_No active items._\n\n")
                continue
            f.write("| ID | Summary | Assignee | Type | Status |\n")
            f.write("|----|---------|----------|------|--------|\n")
            for item_id, title, wtype, state, assignee, parent_id, sprint in pd["issues"]:
                assignee_short = assignee.split()[0] if assignee else "—"
                bold_open = "**" if assignee == my_name_ado else ""
                bold_close = "**" if assignee == my_name_ado else ""
                f.write(f"| {bold_open}{item_id}{bold_close} | {bold_open}{title}{bold_close} | {assignee_short} | {wtype} | {state} |\n")
            f.write("\n")

    print(f"  weekly note: {filepath}")


def main():
    obs = load_obsidian_config()
    obsidian_vault = obs["vault"]
    my_name = obs["my_name"]

    if not obsidian_vault:
        raise RuntimeError(
            "No Obsidian vault configured. "
            "Add [obsidian] vault = '...' to .dlt/secrets.toml."
        )

    credentials = dlt.secrets["destination.motherduck.credentials"]
    con = duckdb.connect(credentials)
    projects = load_all_projects()

    for project in projects:
        vault_path = project.obsidian_vault_path
        if not vault_path:
            print(f"Skipping '{project.name}' — no obsidian_vault_path configured")
            continue

        print(f"Exporting {project.name}...")
        export_confluence(con, project, vault_path, obsidian_vault)
        export_jira(con, project, vault_path, obsidian_vault, my_name)
        export_ado_wiki(con, project, vault_path, obsidian_vault)

    print("Generating weekly note...")
    generate_weekly_note(con, projects, obsidian_vault, my_name, obs["my_name_ado"])

    print("Done.")


if __name__ == "__main__":
    main()
