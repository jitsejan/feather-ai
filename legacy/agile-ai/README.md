# 🚀 Agile AI - Jira Analytics

AI-powered Agile & DevOps insights with dbt + Evidence.dev + DuckDB.

## Quick Start

```bash
# Start the dashboard
make dashboard
```

Opens **http://localhost:3000** with your Jira analytics!

## Complete Workflow

```bash
# 1. Fetch data from Jira
uv run jira_pipeline

# 2. Transform with dbt  
make dbt-run

# 3. View dashboard
make dashboard

# Or refresh everything at once:
make refresh
```

## What You Get

Beautiful dashboards with:
- ✅ Sprint velocity trends
- ✅ Team member performance
- ✅ Personal activity tracking
- ✅ Ticket health alerts
- ✅ Carryover analysis

## Available Commands

```bash
make help              # Show all commands
make dashboard         # Start Evidence dev server
make dbt-run          # Run dbt transformations
make refresh          # Fetch Jira data + run dbt
```

See `make help` for full command list.

## Tech Stack

- **Storage**: Motherduck (DuckDB in the cloud)
- **Transformation**: dbt (10 gold models)
- **Visualization**: 
  - Evidence.dev (markdown + SQL) - See `dashboards/`
  - Apache Superset (Docker) - See `superset/` for setup

## Dashboards

### Evidence.dev Dashboard
- Quick start: `make dashboard` → http://localhost:3000
- See `dashboards/SETUP.md` for details

### Apache Superset Dashboard
- Quick start: `make superset-up && make superset-init` → http://localhost:8088
- See `superset/SETUP.md` for detailed setup
- Replicates Evidence dashboards with drill-down capabilities

## Documentation

- **FINAL_SOLUTION.md** - Complete architecture
- **dashboards/SETUP.md** - Evidence dashboard guide
- **superset/SETUP.md** - Superset setup guide
- **Makefile** - All commands (run `make help`)

---

**Get started**: `make dashboard` → http://localhost:3000 🎉
