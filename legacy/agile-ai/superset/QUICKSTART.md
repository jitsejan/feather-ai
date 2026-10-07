# Quick Start Guide - Get Superset Running Now

Follow these steps in order:

## Step 1: Set Environment Variable

```bash
export MOTHERDUCK_TOKEN="your-motherduck-token-here"
```

**To get your token:**
```bash
duckdb "md:"
# Then in DuckDB shell:
PRAGMA PRINT_MD_TOKEN;
```

## Step 2: Start Superset

```bash
make superset-up
```

Wait about 60 seconds for Superset to initialize. You'll see logs indicating it's starting up.

## Step 3: Initialize Superset (First Time Only)

```bash
make superset-init
```

This creates the admin user:
- Username: `admin`
- Password: `admin`

**⚠️ Change the password after first login!**

## Step 4: Access Superset

Open your browser: **http://localhost:8088**

Login with:
- Username: `admin`
- Password: `admin`

## Step 5: Connect to Motherduck

1. In Superset, click **Settings** (gear icon) → **Database Connections**
2. Click **+ Database** (green button)
3. Fill in:
   - **Display Name:** `Motherduck - Agile AI`
   - **Supported Databases:** Select **Other** (or search for "DuckDB")
   - **SQLAlchemy URI:** 
     ```
     duckdb:///agile_ai_db?motherduck_token=YOUR_MOTHERDUCK_TOKEN
     ```
     (Replace `YOUR_MOTHERDUCK_TOKEN` with your actual token)
4. Click **Test Connection**
5. If successful, click **Connect**

## Step 6: Test the Connection

1. Go to **SQL Lab** → **SQL Editor**
2. Select your **Motherduck - Agile AI** connection
3. Run this test query:
   ```sql
   SELECT 
     assignee,
     issues_assigned,
     issues_completed
   FROM gold.user_insights
   LIMIT 100;
   ```
4. If you see data, you're connected! ✅

## Step 7: Create Your First Dataset

1. Go to **Data** → **Datasets** → **+ Dataset**
2. Select **Motherduck - Agile AI** connection
3. Select schema: `gold`
4. Select table: `user_insights`
5. Click **Create Dataset**

## Step 8: Create Your First Chart

1. Go to **Charts** → **+ Chart**
2. Select the `user_insights` dataset you just created
3. Choose chart type: **Bar Chart**
4. Configure:
   - **Query Mode:** Choose "Raw SQL" or use the dataset
   - **Metrics:** `SUM(issues_completed)`
   - **Group by:** `assignee`
5. Click **Run Query** to preview
6. Click **Save** and name it "Team Member Completion"

## Step 9: Create Your First Dashboard

1. Go to **Dashboards** → **+ Dashboard**
2. Name it: "Executive Overview"
3. Click **Save**
4. Click **Edit Dashboard**
5. Click **+ Add Chart** and add your chart
6. Click **Save**

## Troubleshooting

### Can't connect to Motherduck?
- Verify your token is correct
- Test in DuckDB CLI first:
  ```bash
  duckdb "md:agile_ai_db?motherduck_token=YOUR_TOKEN"
  ```
  Then run: `SELECT * FROM gold.user_insights LIMIT 10;`

### Superset won't start?
- Check logs: `make superset-logs`
- Make sure Docker is running
- Try restarting: `make superset-restart`

### Need help?
- See `superset/SETUP.md` for detailed documentation
- Check `superset/test_connection.sql` for test queries
- Review `superset/queries.sql` for all available queries

## Next Steps

Once you have the basics working:

1. **Create all datasets** for the gold tables:
   - `sprint_velocity`
   - `sprint_performance`
   - `team_member_performance`
   - `ticket_aging`
   - `sprint_carryover`
   - `personal_activity`
   - `personal_ticket_status`
   - `jira_config`

2. **Replicate Evidence dashboards** using queries from `superset/queries.sql`

3. **Add drill-down features** for user/sprint level analysis

