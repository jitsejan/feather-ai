# Configuration Setup

## Jira Integration Configuration

The `integrations.toml` file contains sensitive configuration including company names, project keys, and Jira instance URLs.

### Setup Instructions

1. Copy the example configuration:
   ```bash
   cp integrations.example.toml integrations.toml
   ```

2. Edit `integrations.toml` with your actual Jira instances:
   - Replace `company_a` and `company_b` with your actual instance names
   - Update `base_url` with your actual Atlassian URLs
   - Update `project_key` with your Jira project keys
   - Configure environment variable names for credentials

3. Set up environment variables with your Jira credentials:
   ```bash
   export COMPANY_A_JIRA_EMAIL="your-email@example.com"
   export COMPANY_A_JIRA_TOKEN="your-api-token"
   export COMPANY_B_JIRA_EMAIL="your-email@example.com"
   export COMPANY_B_JIRA_TOKEN="your-api-token"
   ```

4. Update the DBT seed file `transform/seeds/jira_config.csv` to match your instances:
   ```csv
   instance_name,project_key,jira_base_url
   company_a,PROJECT_A,https://company-a.atlassian.net
   company_b,PROJECT_B,https://company-b.atlassian.net
   ```

### Security Notes

- `integrations.toml` is gitignored and should never be committed
- Only `integrations.example.toml` should be committed
- Keep actual company names and URLs out of the repository
- The DBT seed approach allows the dashboard to generate correct issue links without exposing configuration in the codebase
