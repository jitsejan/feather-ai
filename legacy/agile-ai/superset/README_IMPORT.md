# Importing Dashboards into Superset

**Note:** The automated import script currently has authentication issues. Use the manual approach below, or see `SIMPLE_IMPORT.md` for step-by-step instructions.

There are several ways to get dashboards into Superset:

## Option 1: Automated Import Script (Recommended)

The script creates all datasets automatically:

```bash
make superset-import
```

Or directly:
```bash
python superset/import_dashboards.py
```

This will:
1. ✅ Create all datasets for gold tables
2. ✅ Create empty dashboards (you add charts via UI)

## Option 2: Export/Import Dashboard JSON

Once you've created a dashboard in Superset UI:

1. **Export:**
   - Go to **Dashboards** → Select your dashboard
   - Click **...** (menu) → **Export Dashboard**
   - Save the JSON file to `superset/dashboards/`

2. **Import:**
   - Go to **Dashboards** → **Import Dashboards**
   - Select the JSON file
   - Click **Import**

## Option 3: Manual Creation

1. Create datasets (via script or UI)
2. Create charts from datasets
3. Create dashboards and add charts

## Chart Definitions

See `chart_definitions.yaml` for the structure of all charts. This file defines:
- Dashboard names
- Chart configurations
- SQL queries
- Layout positions

You can use this as a reference when creating charts manually, or enhance the import script to create charts programmatically.

## Notes

- Datasets must be created before charts
- Charts reference datasets by name
- Dashboard JSON includes chart IDs which are auto-generated
- After importing, you may need to verify dataset references

