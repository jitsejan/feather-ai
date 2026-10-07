# Apache Superset Setup for Agile AI Dashboard

This directory contains the configuration and setup for Apache Superset to replicate and extend the Evidence dashboards.

## Quick Start

See [SETUP.md](./SETUP.md) for detailed setup instructions.

**TL;DR:**
```bash
export MOTHERDUCK_TOKEN="your-token"
make superset-up
make superset-init
# Then go to http://localhost:8088 (admin/admin)
```

## Database Structure

Data is stored in Motherduck with the following structure:
- **Database:** `agile_ai_db`
- **Schema:** `gold`
- **Tables:** `user_insights`, `sprint_velocity`, `sprint_performance`, etc.

In Superset queries, reference tables as: `gold.table_name` (e.g., `gold.user_insights`)

Test your connection using queries in [test_connection.sql](./test_connection.sql)

## Creating Dashboards

The SQL queries for all charts are available in `superset/queries.sql`. You can use these to create charts in Superset:

1. **Create a Dataset:**
   - Go to **Data** → **Datasets** → **+ Dataset**
   - Select your Motherduck connection
   - Choose a table (e.g., `gold.sprint_velocity`)
   - Click **Create Dataset**

2. **Create Charts:**
   - Go to **Charts** → **+ Chart**
   - Select your dataset
   - Choose a visualization type
   - Use queries from `queries.sql` as reference
   - Save the chart

3. **Create Dashboards:**
   - Go to **Dashboards** → **+ Dashboard**
   - Add your charts
   - Organize them similar to the Evidence dashboard pages

## Dashboard Structure

The Evidence dashboards are organized into 4 main pages:

1. **Executive Overview** (`index.md`)
   - KPIs: Total Issues, Completed, Completion Rate, Team Members
   - Sprint Velocity Trend (Line Chart)
   - Committed vs Completed (Bar Chart)
   - Team Performance (Bar Chart + Table)
   - Ticket Aging Analysis

2. **Sprint Analytics** (`sprints.md`)
   - Sprint KPIs
   - Velocity Trends
   - Completion Rate by Sprint
   - Sprint Carryover Analysis

3. **Team Performance** (`team.md`)
   - Team Overview KPIs
   - Individual Performance Charts
   - Workload Distribution
   - Top Performers & Attention Needed

4. **Ticket Analysis** (`tickets.md`)
   - Aging Overview KPIs
   - Age Distribution
   - Status Breakdown
   - Aging by Assignee
   - Critical Tickets

## Advanced Features

### Drill-Down Capabilities

Superset supports drill-down features that can be enabled:

1. **Drill to Detail:** Click on a chart element to see underlying data
2. **Drill by:** Filter other charts based on selection
3. **Cross-filters:** Enable filtering across charts

### Native Filters

Add filters to dashboards:
- Sprint selector
- Date range
- Team member selector
- Status filter

### Scheduled Reports

Set up email reports:
- Go to **Settings** → **Alerts & Reports**
- Create scheduled reports for stakeholders

## Troubleshooting

### Connection Issues

If you can't connect to Motherduck:
1. Verify `MOTHERDUCK_TOKEN` is set correctly
2. Check that the token has read access to `agile_ai_db`
3. Test the connection string in DuckDB CLI first

### Chart Not Loading

1. Check the SQL query syntax
2. Verify the table exists in the `gold` schema
3. Check Superset logs: `docker-compose logs superset`

### Performance Issues

1. Consider materializing views for complex queries
2. Add indexes in Motherduck if possible
3. Use query caching in Superset

## Next Steps

Once the basic dashboards are set up:

1. **Add Drill-Down Features:**
   - Click on a sprint → see sprint details
   - Click on a team member → see individual performance
   - Click on a status → see tickets in that status

2. **Create User-Level Dashboards:**
   - Personal dashboard for each team member
   - Filter by assignee

3. **Create Sprint-Level Dashboards:**
   - Detailed sprint analysis
   - Sprint comparison views

4. **Add More Gold Models:**
   - Review `transform/models/gold/` for available models
   - Create additional visualizations as needed

## Maintenance

### Updating Data

Data is automatically updated when you run:
```bash
make pipeline  # Runs jira_pipeline.py
make transform  # Runs dbt to update gold models
```

### Updating Superset

```bash
docker-compose pull
docker-compose up -d
```

### Backup

Superset metadata is stored in the `superset_db` volume. To backup:
```bash
docker-compose exec superset superset export-dashboards -f /tmp/dashboards.json
```

