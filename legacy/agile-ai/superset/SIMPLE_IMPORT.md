# Simple Dashboard Import Guide

Since the API authentication is complex, here's the simplest approach:

## Step 1: Create Datasets Manually (One Time)

1. Go to **Data** → **Datasets** → **+ Dataset**
2. For each table, create a dataset:
   - Select **Motherduck - Agile AI** connection
   - Schema: `gold`
   - Table: (select one)
   - Click **Create Dataset**

**Tables to create:**
- `user_insights`
- `sprint_velocity`
- `sprint_performance`
- `team_member_performance`
- `ticket_aging`
- `sprint_carryover`
- `personal_activity`
- `personal_ticket_status`
- `jira_config`

## Step 2: Create Charts

1. Go to **Charts** → **+ Chart**
2. Select a dataset
3. Choose visualization type
4. Use SQL queries from `superset/queries.sql` as reference
5. Save the chart

## Step 3: Create Dashboards

1. Go to **Dashboards** → **+ Dashboard**
2. Name it (e.g., "Executive Overview")
3. Click **Edit Dashboard**
4. Drag & drop your charts
5. Arrange and resize
6. Save

## Step 4: Export for Version Control

Once your dashboard is complete:

1. Go to **Dashboards** → Select your dashboard
2. Click **...** (menu) → **Export Dashboard**
3. Save the JSON file to `superset/dashboards/executive_overview.json`

## Step 5: Import Later

To restore a dashboard:

1. Go to **Dashboards** → **Import Dashboards**
2. Select the JSON file from `superset/dashboards/`
3. Click **Import**

## Chart Reference

See `chart_definitions.yaml` for:
- All chart SQL queries
- Dashboard structure
- Chart positions

Use this as a reference when creating charts manually.

