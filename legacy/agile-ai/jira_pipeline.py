import dlt
import tomli
import os
from pathlib import Path

from jira import jira, jira_issues


def load_jira_instances():
    """Load Jira instance configurations from config/integrations.toml"""
    config_path = Path(__file__).parent / "config" / "integrations.toml"
    with open(config_path, "rb") as f:
        config = tomli.load(f)

    instances = []
    jira_instances = config.get("jira", {}).get("instances", {})

    for instance_name, instance_config in jira_instances.items():
        base_url = instance_config["base_url"]
        # Extract subdomain from base_url (e.g., "https://company-a.atlassian.net" -> "company-a")
        subdomain = base_url.split("//")[1].split(".")[0]

        email_env = instance_config["email_env"]
        token_env = instance_config["token_env"]
        project_key = instance_config["project_key"]

        # Get credentials from environment variables
        email = os.environ.get(email_env)
        api_token = os.environ.get(token_env)

        if not email or not api_token:
            print(f"⚠️  Skipping {instance_name}: Missing {email_env} or {token_env}")
            continue

        instances.append({
            "name": instance_name,
            "subdomain": subdomain,
            "email": email,
            "api_token": api_token,
            "project_key": project_key,
            "board_ids": instance_config.get("board_ids", []),
        })

    return instances


def main() -> None:
    """Run the jira pipeline.

    This function is exported as an entry point so tooling like `uv run`
    can execute the pipeline by the name `jira_pipeline`.
    """
    instances = load_jira_instances()

    if not instances:
        print("❌ No Jira instances configured with valid credentials")
        return

    # Get MotherDuck token from environment
    motherduck_token = os.environ.get("MOTHERDUCK_TOKEN")
    if not motherduck_token:
        print("❌ MOTHERDUCK_TOKEN environment variable not set")
        return

    pipeline = dlt.pipeline(
        pipeline_name="jira_pipeline",
        destination="motherduck",
        dataset_name="raw"
    )

    # Fetch data from all configured Jira instances
    for instance in instances:
        print(f"\n{'='*80}")
        print(f"📥 Starting fetch for {instance['name']} (project: {instance['project_key']})")
        print(f"{'='*80}\n")

        # Use larger page size to reduce API calls (1000 is Jira's max)
        load_info = pipeline.run(
            jira(
                subdomain=instance["subdomain"],
                email=instance["email"],
                api_token=instance["api_token"],
                project_key=instance["project_key"],
                instance_name=instance["name"],
                board_ids=instance.get("board_ids", []),
                page_size=1000  # Max page size to minimize API calls
            ).with_resources("issues", "issue_histories", "issue_comments", "issue_sprints", "all_sprints")
        )

        print(f"\n{'='*80}")
        print(f"✅ Completed {instance['name']} - Summary:")
        print(f"{'='*80}")
        print(load_info)
        print()


if __name__ == "__main__":
    main()
