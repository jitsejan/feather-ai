# Superset Dashboard Definitions

This directory contains dashboard and chart definitions that can be imported into Superset.

## Importing Dashboards

### Option 1: Via Superset UI (Recommended)

1. Go to **Dashboards** → **Import Dashboards**
2. Select the JSON file from this directory
3. Click **Import**

### Option 2: Via API Script

```bash
# Set credentials
export SUPERSET_USERNAME=admin
export SUPERSET_PASSWORD=admin

# Run import script
python superset/import_dashboards.py
```

### Option 3: Via Superset CLI

```bash
docker-compose exec superset superset import-dashboards -p /app/superset/dashboards/dashboard.json
```

## Dashboard Files

- `executive_overview.json` - Main dashboard with KPIs and overview charts
- `sprint_analytics.json` - Sprint performance and velocity analysis
- `team_performance.json` - Team member metrics and insights
- `ticket_analysis.json` - Ticket aging and status analysis

## Creating Dashboard JSON

You can export existing dashboards from Superset UI:
1. Go to **Dashboards** → Select a dashboard
2. Click **...** (menu) → **Export Dashboard**
3. Save the JSON file here

## Notes

- Dashboards reference datasets by name - make sure datasets are created first
- Chart IDs and dashboard IDs will be auto-generated on import
- You may need to adjust dataset references after import

