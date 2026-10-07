# Apache Superset Setup Guide

This guide will help you set up Apache Superset with Docker and connect it to your Motherduck database.

## Prerequisites

- Docker and Docker Compose installed
- `MOTHERDUCK_TOKEN` environment variable set
- Access to the `agile_ai_db` database in Motherduck

## Quick Start

### 1. Set Environment Variables

```bash
export MOTHERDUCK_TOKEN="your-motherduck-token"
export SUPERSET_SECRET_KEY="your-secret-key-for-production"  # Optional, but recommended
```

### 2. Start Superset

```bash
make superset-up
```

Wait about 60 seconds for Superset to initialize.

### 3. Initialize Superset (First Time Only)

```bash
make superset-init
```

This will:
- Upgrade the database schema
- Create an admin user (username: `admin`, password: `admin`)
- Initialize Superset with default roles and permissions

### 4. Access Superset

Open http://localhost:8088 and login with:
- Username: `admin`
- Password: `admin`

**Important:** Change the admin password after first login!

## Setting Up the Motherduck Connection

### Option 1: Manual Setup (Recommended)

1. **Login to Superset** at http://localhost:8088

2. **Go to Database Connections:**
   - Click on **Settings** (gear icon) → **Database Connections**
   - Click **+ Database**

3. **Configure Connection:**
   - **Supported Databases:** Select **Other** (or search for DuckDB if available)
   - **Display Name:** `Motherduck - Agile AI`
   - **SQLAlchemy URI:** 
     ```
     duckdb:///agile_ai_db?motherduck_token=YOUR_MOTHERDUCK_TOKEN
     ```
     Replace `YOUR_MOTHERDUCK_TOKEN` with your actual token
   - **Database Name:** `Motherduck - Agile AI`
   
   **Note:** When connected, tables are accessed as `gold.table_name` (e.g., `gold.user_insights`, `gold.sprint_velocity`)

4. **Advanced Settings:**
   - Check **Expose in SQL Lab**
   - Check **Allow CREATE TABLE AS (CTAS)**
   - Check **Allow CREATE VIEW AS (CVAS)**
   - Uncheck **Allow DML** (we're read-only)

5. **Test Connection:**
   - Click **Test Connection**
   - If successful, click **Connect**

### Option 2: Using SQL Lab (Alternative)

If the DuckDB connection doesn't work directly, you can use SQL Lab with a custom connection:

1. Go to **SQL Lab** → **SQL Editor**
2. Use the connection string format that works with your setup
3. Test queries directly in SQL Lab

### Option 3: Using Python Script

```bash
# Make sure Superset is running first
python superset/setup_connection.py
```

**Note:** This script requires the Superset API to be accessible and may need adjustments based on your Superset version.

## Creating Your First Dashboard

### Step 1: Create Datasets

For each gold table you want to visualize:

1. Go to **Data** → **Datasets** → **+ Dataset**
2. Select your **Motherduck - Agile AI** connection
3. Choose a schema: `gold`
4. Select a table (e.g., `sprint_velocity`)
5. Click **Create Dataset**

Repeat for all tables you need (all in the `gold` schema):
- `sprint_velocity` (will be accessed as `gold.sprint_velocity`)
- `sprint_performance`
- `team_member_performance`
- `user_insights`
- `ticket_aging`
- `sprint_carryover`
- `personal_activity`
- `personal_ticket_status`
- `jira_config`

**Note:** In Superset, when creating a dataset, you'll select the schema `gold` and then the table name. The full path is `agile_ai_db.gold.table_name`, but in queries you'll use `gold.table_name`.

### Step 2: Create Charts

1. Go to **Charts** → **+ Chart**
2. Select your dataset
3. Choose a visualization type
4. Configure the chart:
   - **Query:** Use SQL queries from `superset/queries.sql` as reference
   - **Metrics:** Select or create metrics
   - **Dimensions:** Select grouping columns
   - **Filters:** Add any filters needed

5. Click **Run Query** to preview
6. Click **Save** and give it a name

### Step 3: Create Dashboards

1. Go to **Dashboards** → **+ Dashboard**
2. Give it a name (e.g., "Executive Overview")
3. Click **Save**
4. Click **Edit Dashboard**
5. Add your charts by clicking **+ Add Chart**
6. Arrange and resize charts as needed
7. Click **Save**

## Replicating Evidence Dashboards

The Evidence dashboards are organized into 4 main pages. Create corresponding Superset dashboards:

### 1. Executive Overview Dashboard

**Charts to create:**
- **KPIs:** 4 Big Number charts
  - Total Issues
  - Completed Issues
  - Completion Rate
  - Active Team Members
- **Sprint Velocity Trend:** Line Chart
- **Committed vs Completed:** Bar Chart
- **Team Performance:** Bar Chart + Table
- **Ticket Aging:** Bar Chart + Table

**SQL Queries:** See `superset/queries.sql` section "EXECUTIVE OVERVIEW"

### 2. Sprint Analytics Dashboard

**Charts to create:**
- **Sprint KPIs:** 4 Big Number charts
- **Velocity Trends:** Line Chart
- **Completion Rate:** Bar Chart
- **Carryover Analysis:** Bar Chart + Table

**SQL Queries:** See `superset/queries.sql` section "SPRINT ANALYTICS"

### 3. Team Performance Dashboard

**Charts to create:**
- **Team KPIs:** 4 Big Number charts
- **Individual Performance:** Bar Charts
- **Workload Distribution:** Bar Chart
- **Top Performers:** Table
- **Needs Attention:** Table

**SQL Queries:** See `superset/queries.sql` section "TEAM PERFORMANCE"

### 4. Ticket Analysis Dashboard

**Charts to create:**
- **Aging KPIs:** 4 Big Number charts
- **Age Distribution:** Bar Chart
- **Status Breakdown:** Bar Chart
- **Aging by Assignee:** Bar Chart
- **Critical Tickets:** Table

**SQL Queries:** See `superset/queries.sql` section "TICKET ANALYSIS"

## Adding Drill-Down Features

Superset supports drill-down capabilities:

### Enable Drill-Down

1. Go to **Settings** → **Feature Flags**
2. Enable:
   - `ENABLE_DRILL_TO_DETAIL`
   - `ENABLE_DRILL_BY`
   - `DASHBOARD_CROSS_FILTERS`

### Create Drill-Down Charts

1. **Sprint → Details:**
   - Create a chart showing sprint summary
   - Add drill-down to show sprint details
   - Link to a detailed sprint dashboard

2. **Team Member → Personal View:**
   - Create a team member chart
   - Add drill-down to personal activity
   - Use `personal_activity` and `personal_ticket_status` tables

3. **Status → Tickets:**
   - Create a status breakdown chart
   - Add drill-down to show tickets in that status
   - Use `ticket_aging` table

## Troubleshooting

### Connection Issues

**Problem:** Can't connect to Motherduck

**Solutions:**
1. Verify `MOTHERDUCK_TOKEN` is correct
2. Test connection in DuckDB CLI:
   ```bash
   duckdb "md:agile_ai_db?motherduck_token=YOUR_TOKEN"
   ```
   Then test a query:
   ```sql
   SELECT * FROM gold.user_insights LIMIT 10;
   ```
3. Check Superset logs: `make superset-logs`
4. Try using SQL Lab with a direct query first:
   ```sql
   SELECT * FROM gold.user_insights LIMIT 10;
   ```
5. Verify the database name is `agile_ai_db` and schema is `gold`

### Chart Not Loading

**Problem:** Chart shows error or no data

**Solutions:**
1. Check the SQL query syntax
2. Verify the table exists: `SELECT * FROM gold.sprint_velocity LIMIT 1`
3. Check Superset logs for detailed error messages
4. Try simplifying the query first

### Performance Issues

**Problem:** Queries are slow

**Solutions:**
1. Add filters to limit data
2. Use materialized views in Motherduck
3. Enable query caching in Superset
4. Consider creating summary tables in dbt

### DuckDB Driver Not Found

**Problem:** "No module named 'duckdb_engine'"

**Solutions:**
1. Restart Superset: `make superset-restart`
2. Check if the package was installed:
   ```bash
   docker-compose exec superset pip list | grep duckdb
   ```
3. If missing, install manually:
   ```bash
   docker-compose exec superset pip install duckdb-engine sqlalchemy-duckdb
   ```

## Next Steps

Once basic dashboards are working:

1. **Add Native Filters:**
   - Sprint selector
   - Date range picker
   - Team member filter
   - Status filter

2. **Create User-Level Dashboards:**
   - Personal dashboard for each team member
   - Filter by assignee using native filters

3. **Create Sprint-Level Dashboards:**
   - Detailed sprint analysis
   - Sprint comparison views
   - Use `sprint_performance` and `team_member_performance` tables

4. **Set Up Scheduled Reports:**
   - Go to **Settings** → **Alerts & Reports**
   - Create email reports for stakeholders

5. **Enable Cross-Filtering:**
   - Enable dashboard cross-filters
   - Click on one chart to filter others

## Maintenance

### Updating Data

Data is automatically updated when you run:
```bash
make refresh  # Runs jira_pipeline + dbt
```

Superset will use the latest data on next query execution.

### Updating Superset

```bash
docker-compose pull
docker-compose down
docker-compose up -d
```

### Backup Dashboards

```bash
docker-compose exec superset superset export-dashboards -f /tmp/dashboards.json
```

### View Logs

```bash
make superset-logs
```

## Additional Resources

- [Superset Documentation](https://superset.apache.org/docs/intro)
- [Superset SQL Lab](https://superset.apache.org/docs/intro#sql-lab)
- [Superset Charts](https://superset.apache.org/docs/intro#charts)
- [Motherduck Documentation](https://motherduck.com/docs/)

