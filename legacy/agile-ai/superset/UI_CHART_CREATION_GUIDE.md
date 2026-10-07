# Step-by-Step Guide: Creating a Chart in Superset UI

This guide will walk you through creating a chart in Superset to visualize data from your `gold` tables.

## Prerequisites

1. Superset is running at http://localhost:8088
2. You're logged in (default: admin/admin)
3. Database connection to Motherduck is configured

## Step 1: Create a Dataset (if not already created)

1. **Navigate to Datasets:**
   - Click on **"Data"** in the top menu
   - Click **"Datasets"** in the dropdown
   - Or go directly to: http://localhost:8088/dataset/list/

2. **Create New Dataset:**
   - Click the **"+ Dataset"** button (top right)
   - Or click **"Add Dataset"** button

3. **Select Your Data Source:**
   - **Database:** Select "DuckDB" (or your Motherduck connection name)
   - **Schema:** Select `gold`
   - **Table:** Select a table (e.g., `user_insights`, `sprint_velocity`, `ticket_aging`)
   - Click **"Create Dataset"**

4. **Configure Dataset (optional):**
   - You can add a description
   - Review the columns
   - Click **"Save"**

## Step 2: Create a Chart

1. **Navigate to Charts:**
   - Click on **"Charts"** in the top menu
   - Click **"+ Chart"** button (top right)
   - Or go directly to: http://localhost:8088/chart/add

2. **Select Dataset:**
   - In the **"Choose a dataset"** dropdown, select the dataset you just created
   - Click **"Create new chart"**

3. **Choose Visualization Type:**
   - You'll see a list of visualization types
   - For this example, let's create a **"Big Number"** chart:
     - Scroll down or search for **"Big Number"**
     - Click on it

4. **Configure the Chart:**

   ### For a Big Number Chart:
   
   **Metrics:**
   - Click **"+ Add metric"**
   - You can either:
     - Select a column from the dropdown (e.g., `issues_assigned`)
     - Or create a custom SQL expression:
       - Click **"Custom SQL"** tab
       - Enter: `SUM(issues_assigned)`
       - Click **"Save"**
   
   **Filters (optional):**
   - Click **"+ Add filter"** if you want to filter data
   - Example: `assignee IS NOT NULL`
   
   **Preview:**
   - Click **"Run Query"** to see a preview
   - The big number should appear

5. **Save the Chart:**
   - Click **"Save"** button (top right)
   - Enter a name: e.g., "Total Issues"
   - Optionally add a description
   - Click **"Save"**

## Step 3: Create a More Complex Chart (Line Chart Example)

Let's create a line chart showing sprint velocity over time:

1. **Create New Chart:**
   - Go to **Charts** → **"+ Chart"**
   - Select your `sprint_velocity` dataset
   - Choose **"Line Chart"** visualization type

2. **Configure Metrics:**
   - Click **"+ Add metric"**
   - Select or create: `SUM(issues_completed)` or `issues_completed`
   - This will be the Y-axis value

3. **Configure Time Column:**
   - In **"Time"** section:
     - **Time column:** Select `start_date` or `sprint_name`
     - **Time grain:** Select appropriate grain (e.g., "Day", "Week", or "None" if using sprint_name)

4. **Configure Group By (optional):**
   - If you want to group by sprint:
     - Click **"+ Add group by"**
     - Select `sprint_name`

5. **Run Query:**
   - Click **"Run Query"** to preview
   - You should see a line chart

6. **Save:**
   - Click **"Save"**
   - Name it: "Sprint Velocity Trend"

## Step 4: Create a Bar Chart

For a bar chart comparing values:

1. **Create New Chart:**
   - Select dataset (e.g., `user_insights`)
   - Choose **"Bar Chart"** visualization type

2. **Configure:**
   - **Metrics:** Add metric (e.g., `SUM(issues_completed)`)
   - **Group by:** Add dimension (e.g., `assignee`)
   - **Sort by:** Choose how to sort (e.g., by metric value descending)

3. **Run Query and Save**

## Step 5: Add Chart to Dashboard

1. **Go to Dashboards:**
   - Click **"Dashboards"** in top menu
   - Select an existing dashboard (e.g., "Executive Overview")
   - Or create a new dashboard: Click **"+ Dashboard"**

2. **Edit Dashboard:**
   - Click **"Edit Dashboard"** button
   - Click **"+ Add Chart"** button

3. **Select Chart:**
   - Find your chart in the list
   - Click on it to add to dashboard

4. **Arrange:**
   - Drag the chart to position it
   - Resize by dragging corners
   - Arrange multiple charts as needed

5. **Save Dashboard:**
   - Click **"Save"** button
   - Your chart is now on the dashboard!

## Example SQL Queries for Reference

Here are some example queries you can use as custom SQL metrics:

### Big Number - Total Issues:
```sql
SELECT SUM(issues_assigned) 
FROM gold.user_insights 
WHERE assignee IS NOT NULL
```

### Big Number - Completion Rate:
```sql
SELECT ROUND(100.0 * SUM(issues_completed) / NULLIF(SUM(issues_assigned), 0), 1)
FROM gold.user_insights
WHERE assignee IS NOT NULL
```

### Line Chart - Sprint Velocity:
```sql
SELECT 
  sprint_name,
  SUM(issues_completed) as completed
FROM gold.sprint_velocity
GROUP BY sprint_name
ORDER BY start_date ASC
```

## Tips

1. **Use Custom SQL for Complex Metrics:**
   - When the built-in aggregations aren't enough, use "Custom SQL" in the metric
   - You can write full SQL expressions

2. **Test with Run Query:**
   - Always click "Run Query" before saving to verify it works

3. **Start Simple:**
   - Begin with simple charts (Big Number) before moving to complex visualizations

4. **Check Data Types:**
   - Make sure your columns have the right data types (numbers for metrics, dates for time series)

5. **Use Filters:**
   - Add filters to focus on specific data subsets

## Troubleshooting

- **"No data":** Check your filters and make sure the dataset has data
- **"Error loading data":** Verify your SQL syntax and column names
- **"Visualization type not supported":** Make sure you selected a valid viz type
- **Chart not appearing:** Check that the chart is saved and the dataset connection is working

## Next Steps

Once you've created a chart successfully:
1. Note the exact configuration (metrics, filters, etc.)
2. We can use this as a template to automate chart creation
3. Export the dashboard JSON to see the exact format Superset uses



