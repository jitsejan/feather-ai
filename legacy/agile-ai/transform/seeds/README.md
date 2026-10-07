# DBT Seeds Configuration

## Jira Configuration

The `jira_config.csv` file contains the mapping of Jira project keys to their instance base URLs.

### Setup

1. Copy the example file:
   ```bash
   cp jira_config.example.csv jira_config.csv
   ```

2. Edit `jira_config.csv` with your actual Jira instances:
   ```csv
   instance_name,project_key,jira_base_url
   company_a_primary,DT,https://your-company.atlassian.net
   company_b_primary,CORE,https://other-company.atlassian.net
   ```

3. The `jira_config.csv` file is gitignored and will not be committed.

### How It Works

- The dashboard extracts the project key from issue keys (e.g., "DT-123" → "DT")
- It joins with this config table to get the correct Jira base URL
- This allows the dashboard to support multiple Jira instances seamlessly

### Example

If you have issues:
- `DT-123` → links to `https://your-company.atlassian.net/browse/DT-123`
- `CORE-456` → links to `https://other-company.atlassian.net/browse/CORE-456`
