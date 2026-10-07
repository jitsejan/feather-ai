#!/usr/bin/env python3
"""
Script to import dashboards and charts into Superset via API.
This creates all the dashboards from the Evidence setup automatically.

Usage:
    python superset/import_dashboards.py

Or via Makefile:
    make superset-import
"""
import os
import json
import yaml
import requests
from typing import Dict, List, Optional

SUPERSET_URL = os.environ.get("SUPERSET_URL", "http://localhost:8088")
SUPERSET_USERNAME = os.environ.get("SUPERSET_USERNAME", "admin")
SUPERSET_PASSWORD = os.environ.get("SUPERSET_PASSWORD", "admin")


class SupersetClient:
    def __init__(self, base_url: str, username: str, password: str):
        self.base_url = base_url.rstrip("/")
        self.session = requests.Session()
        self.csrf_token = None
        self.login(username, password)

    def login(self, username: str, password: str) -> bool:
        """Login to Superset using access token API."""
        # Use Superset's access token endpoint for authentication
        auth_data = {
            "username": username,
            "password": password,
            "provider": "db",
            "refresh": True
        }
        
        response = self.session.post(
            f"{self.base_url}/api/v1/security/login",
            json=auth_data,
            headers={"Content-Type": "application/json"}
        )

        if response.status_code == 200:
            data = response.json()
            # Token is directly in response, not nested
            access_token = data.get("access_token") or data.get("accessToken")
            
            if access_token:
                # Use Bearer token for API calls
                self.session.headers.update({
                    "Authorization": f"Bearer {access_token}",
                    "Content-Type": "application/json"
                })
                
                # Get CSRF token - required even with Bearer auth
                try:
                    csrf_response = self.session.get(f"{self.base_url}/api/v1/security/csrf_token/")
                    if csrf_response.status_code == 200:
                        csrf_data = csrf_response.json()
                        # CSRF token is in "result" key
                        if isinstance(csrf_data, dict):
                            self.csrf_token = csrf_data.get("result")
                            if not self.csrf_token:
                                # Try alternative keys
                                self.csrf_token = csrf_data.get("csrf_token")
                        elif isinstance(csrf_data, str):
                            self.csrf_token = csrf_data
                        
                        if self.csrf_token:
                            self.session.headers["X-CSRFToken"] = self.csrf_token
                            self.session.headers["Referer"] = self.base_url
                            print(f"✅ Got CSRF token")
                except Exception as e:
                    print(f"⚠️  Could not get CSRF token: {e}")
                    # Continue anyway - some endpoints might work without it
                
                print("✅ Logged in to Superset (Bearer token)")
                return True
            else:
                # Debug: print response structure
                print(f"⚠️  No access token found. Response keys: {list(data.keys()) if isinstance(data, dict) else 'not a dict'}")
                # Fall through to session login
        else:
            print(f"❌ Login failed: {response.status_code}")
            print(f"   Response: {response.text[:300]}")
            # Fallback to session-based login
            return self._session_login(username, password)
    
    def _session_login(self, username: str, password: str) -> bool:
        """Fallback session-based login."""
        # Get login page
        response = self.session.get(f"{self.base_url}/login/")
        if response.status_code != 200:
            return False

        # Login with session
        login_data = {
            "username": username,
            "password": password,
        }
        response = self.session.post(
            f"{self.base_url}/login/",
            data=login_data,
            allow_redirects=True
        )

        if response.status_code in [200, 302]:
            # Get CSRF token
            csrf_response = self.session.get(f"{self.base_url}/api/v1/security/csrf_token/")
            if csrf_response.status_code == 200:
                self.csrf_token = csrf_response.json().get("result", {}).get("csrf_token")
                self.session.headers.update({
                    "X-CSRFToken": self.csrf_token,
                    "Referer": self.base_url,
                    "Content-Type": "application/json"
                })
            print("✅ Logged in to Superset (session)")
            return True
        return False

    def get_database_id(self, database_name: str) -> Optional[int]:
        """Get database ID by name."""
        response = self.session.get(f"{self.base_url}/api/v1/database/")
        if response.status_code == 200:
            databases = response.json().get("result", [])
            for db in databases:
                if db.get("database_name") == database_name:
                    return db.get("id")
        return None

    def create_dataset(self, database_id: int, schema: str, table_name: str) -> Optional[int]:
        """Create a dataset if it doesn't exist."""
        # Check if dataset exists
        response = self.session.get(
            f"{self.base_url}/api/v1/dataset/",
            params={"q": json.dumps({"filters": [{"col": "table_name", "opr": "eq", "value": table_name}]})}
        )
        if response.status_code == 200:
            datasets = response.json().get("result", [])
            for ds in datasets:
                if ds.get("table_name") == table_name and ds.get("schema") == schema:
                    print(f"  ✅ Dataset {table_name} already exists (ID: {ds.get('id')})")
                    return ds.get("id")

        # Create new dataset
        dataset_data = {
            "database": database_id,
            "schema": schema,
            "table_name": table_name,
        }
        response = self.session.post(
            f"{self.base_url}/api/v1/dataset/",
            json=dataset_data
        )
        if response.status_code in [200, 201]:
            data = response.json()
            # Try different response structures
            result = data.get("result", data)
            if isinstance(result, dict):
                dataset_id = result.get("id")
            else:
                dataset_id = data.get("id")
            
            if dataset_id:
                print(f"  ✅ Created dataset {table_name} (ID: {dataset_id})")
            else:
                print(f"  ✅ Created dataset {table_name} (response: {response.status_code})")
            return dataset_id
        else:
            print(f"  ❌ Failed to create dataset {table_name}: {response.status_code}")
            print(f"     {response.text[:200]}")
            return None

    def create_chart_from_yaml(self, chart_def: Dict, dataset_id: int, dataset_name: str) -> Optional[int]:
        """Create a chart from YAML definition."""
        chart_name = chart_def.get("name", "Untitled Chart")
        chart_type = chart_def.get("type", "table")
        
        # Map chart types to Superset viz types
        # Note: Superset uses specific viz type names
        viz_type_map = {
            "big_number": "big_number_total",  # Full name for big number
            "line": "line",
            "bar": "bar",  # Use 'bar' not 'dist_bar'
            "table": "table",
        }
        viz_type = viz_type_map.get(chart_type, "table")
        
        # Check if chart already exists
        response = self.session.get(
            f"{self.base_url}/api/v1/chart/",
            params={"q": json.dumps({"filters": [{"col": "slice_name", "opr": "eq", "value": chart_name}]})}
        )
        if response.status_code == 200:
            charts = response.json().get("result", [])
            for chart in charts:
                if chart.get("slice_name") == chart_name:
                    chart_id = chart.get("id")
                    print(f"  ✅ Chart '{chart_name}' already exists (ID: {chart_id})")
                    return chart_id
        
        # Build chart configuration
        # For SQL-based charts, we'll use a virtual dataset approach
        # or create the chart with raw SQL query
        
        # Get the query
        query = chart_def.get("query", "").strip()
        
        # Create chart config
        # Extract metrics and dimensions from query or definition
        metrics_list = chart_def.get("metrics", [])
        group_by_list = chart_def.get("group_by", [])
        
        # For big_number charts, extract metric from query if not specified
        if viz_type == "big_number" and not metrics_list and query:
            # Try to extract the metric expression
            try:
                select_part = query.split("SELECT")[1].split("FROM")[0].strip()
                # Remove "as value" or similar aliases
                metric_expr = select_part.split(" AS ")[0].strip()
                metrics_list = [metric_expr]
            except:
                # Fallback to a simple count
                metrics_list = ["COUNT(*)"]
        
        # Build params based on chart type
        params = {
            "adhoc_filters": [],
            "row_limit": 10000,
        }
        
        # Add metrics
        if metrics_list:
            if viz_type == "big_number":
                # For big_number, use first metric as string or dict
                if isinstance(metrics_list[0], str):
                    params["metric"] = metrics_list[0]
                else:
                    params["metric"] = metrics_list[0]
            else:
                # For other chart types, use array
                params["metrics"] = metrics_list
        
        # Add group by
        if group_by_list:
            params["groupby"] = group_by_list
        
        # Ensure params has proper structure for the chart type
        if viz_type == "big_number":
            # For big_number, we need a metric
            if not params.get("metric"):
                # Try to extract from query or use a default
                if query and "SUM" in query:
                    try:
                        metric_expr = query.split("SELECT")[1].split("FROM")[0].strip()
                        metric_expr = metric_expr.split(" AS ")[0].strip()
                        params["metric"] = metric_expr
                    except:
                        params["metric"] = "COUNT(*)"
                else:
                    params["metric"] = "COUNT(*)"
        
        chart_config = {
            "slice_name": chart_name,
            "viz_type": viz_type,
            "datasource_id": dataset_id,
            "datasource_type": "table",
            "params": json.dumps(params),
        }
        
        # Create the chart
        import time
        max_retries = 3
        for attempt in range(max_retries):
            response = self.session.post(
                f"{self.base_url}/api/v1/chart/",
                json=chart_config
            )
            
            if response.status_code in [200, 201]:
                data = response.json()
                # Response structure: {"id": X, "result": {...}}
                chart_id = data.get("id")
                if not chart_id:
                    # Try result.id as fallback
                    result = data.get("result", {})
                    if isinstance(result, dict):
                        chart_id = result.get("id")
                
                if chart_id:
                    # Verify chart was created with correct datasource
                    # If not, update it
                    verify_resp = self.session.get(f"{self.base_url}/api/v1/chart/{chart_id}")
                    if verify_resp.status_code == 200:
                        chart_data = verify_resp.json().get("result", {})
                        if chart_data.get("datasource_id") != dataset_id:
                            # Update datasource
                            update_data = {
                                "datasource_id": dataset_id,
                                "datasource_type": "table"
                            }
                            self.session.put(f"{self.base_url}/api/v1/chart/{chart_id}", json=update_data)
                    
                    print(f"  ✅ Created chart: {chart_name} (ID: {chart_id})")
                    print(f"     Type: {viz_type}, Dataset: {dataset_name}")
                    return chart_id
                else:
                    # Try to get chart by name as fallback
                    chart_id = self._get_chart_id_by_name(chart_name)
                    if chart_id:
                        print(f"  ✅ Found existing chart: {chart_name} (ID: {chart_id})")
                        return chart_id
                    return None
            elif response.status_code == 429:
                # Rate limited - wait and retry
                wait_time = (attempt + 1) * 2
                print(f"  ⏳ Rate limited, waiting {wait_time} seconds...")
                time.sleep(wait_time)
                continue
            else:
                if attempt == max_retries - 1:
                    print(f"  ❌ Failed to create chart '{chart_name}': {response.status_code}")
                    if response.status_code != 429:
                        print(f"     {response.text[:300]}")
                else:
                    time.sleep(1)
                    continue
        
        return None
    
    def _get_chart_id_by_name(self, chart_name: str) -> Optional[int]:
        """Get chart ID by name as fallback."""
        response = self.session.get(
            f"{self.base_url}/api/v1/chart/",
            params={"q": json.dumps({"filters": [{"col": "slice_name", "opr": "eq", "value": chart_name}]})}
        )
        if response.status_code == 200:
            charts = response.json().get("result", [])
            for chart in charts:
                if chart.get("slice_name") == chart_name:
                    return chart.get("id")
        return None
    
    def add_charts_to_dashboard(self, dashboard_id: int, chart_ids: List[int], chart_positions: Dict[int, Dict]) -> bool:
        """Add multiple charts to a dashboard with positions."""
        # Get current dashboard
        response = self.session.get(f"{self.base_url}/api/v1/dashboard/{dashboard_id}")
        if response.status_code != 200:
            return False
        
        dashboard = response.json().get("result", {})
        position_json = dashboard.get("position_json")
        
        # Parse position_json if it's a string
        if isinstance(position_json, str):
            try:
                position_json = json.loads(position_json)
            except:
                position_json = {}
        
        # Initialize position_json if empty
        if not position_json or not isinstance(position_json, dict):
            position_json = {
                "ROOT_ID": {
                    "type": "ROOT",
                    "id": "ROOT_ID",
                    "children": ["GRID_ID"]
                },
                "GRID_ID": {
                    "type": "GRID",
                    "id": "GRID_ID",
                    "children": []
                }
            }
        
        # Add all charts to position
        grid_children = position_json.get("GRID_ID", {}).get("children", [])
        for chart_id in chart_ids:
            chart_key = f"CHART-{chart_id}"
            if chart_key not in grid_children:
                grid_children.append(chart_key)
            
            # Add/update chart position
            # Superset expects specific structure with meta containing width/height
            position = chart_positions.get(chart_id, {"x": 0, "y": 0, "w": 4, "h": 4})
            w = position.get("w", 4)
            h = position.get("h", 4)
            position_json[chart_key] = {
                "type": "CHART",
                "id": chart_key,
                "meta": {
                    "chartId": chart_id,
                    "width": w,
                    "height": h,
                    "sliceName": "",  # Will be populated by Superset
                },
                "x": position.get("x", 0),
                "y": position.get("y", 0),
                "w": w,
                "h": h,
            }
        
        position_json["GRID_ID"]["children"] = grid_children
        
        # Update dashboard - position_json must be a JSON string
        update_data = {
            "position_json": json.dumps(position_json)
        }
        
        response = self.session.put(
            f"{self.base_url}/api/v1/dashboard/{dashboard_id}",
            json=update_data
        )
        
        if response.status_code in [200, 201]:
            return True
        else:
            print(f"     ⚠️  Failed to add charts to dashboard: {response.status_code}")
            if response.status_code != 429:
                print(f"     {response.text[:200]}")
            return False
    
    def add_chart_to_dashboard(self, dashboard_id: int, chart_id: int, position: Dict) -> bool:
        """Add a chart to a dashboard with position."""
        import time
        # Get current dashboard
        response = self.session.get(f"{self.base_url}/api/v1/dashboard/{dashboard_id}")
        if response.status_code != 200:
            return False
        
        dashboard = response.json().get("result", {})
        current_slices = dashboard.get("slices", [])
        position_json = dashboard.get("position_json") or {}
        
        # Add chart to slices array if not already there
        if chart_id not in current_slices:
            current_slices.append(chart_id)
        
        # Initialize position_json if empty
        if not position_json or not isinstance(position_json, dict):
            position_json = {
                "ROOT_ID": {
                    "type": "ROOT",
                    "id": "ROOT_ID",
                    "children": ["GRID_ID"]
                },
                "GRID_ID": {
                    "type": "GRID",
                    "id": "GRID_ID",
                    "children": []
                }
            }
        
        # Add chart to position
        chart_key = f"CHART-{chart_id}"
        grid_children = position_json.get("GRID_ID", {}).get("children", [])
        if chart_key not in grid_children:
            grid_children.append(chart_key)
            position_json["GRID_ID"]["children"] = grid_children
        
        # Add/update chart position
        # Superset expects specific structure with meta containing width/height
        w = position.get("w", 4)
        h = position.get("h", 4)
        position_json[chart_key] = {
            "type": "CHART",
            "id": chart_key,
            "meta": {
                "chartId": chart_id,
                "width": w,
                "height": h,
                "sliceName": "",  # Will be populated by Superset
            },
            "x": position.get("x", 0),
            "y": position.get("y", 0),
            "w": w,
            "h": h,
        }
        
        # Update dashboard - position_json must be a JSON string
        # Note: slices array is managed separately, we'll update position_json only
        update_data = {
            "position_json": json.dumps(position_json)
        }
        
        response = self.session.put(
            f"{self.base_url}/api/v1/dashboard/{dashboard_id}",
            json=update_data
        )
        
        if response.status_code in [200, 201]:
            return True
        else:
            # Debug
            print(f"     ⚠️  Failed to add chart {chart_id} to dashboard: {response.status_code}")
            if response.status_code != 429:
                print(f"     {response.text[:200]}")
            return False

    def create_dashboard(self, dashboard_name: str, chart_ids: List[int]) -> Optional[int]:
        """Create a dashboard with charts."""
        # Check if dashboard exists
        response = self.session.get(
            f"{self.base_url}/api/v1/dashboard/",
            params={"q": json.dumps({"filters": [{"col": "dashboard_title", "opr": "eq", "value": dashboard_name}]})}
        )
        if response.status_code == 200:
            dashboards = response.json().get("result", [])
            for dash in dashboards:
                if dash.get("dashboard_title") == dashboard_name:
                    print(f"✅ Dashboard {dashboard_name} already exists (ID: {dash.get('id')})")
                    return dash.get("id")

        # Create dashboard
        dashboard_data = {
            "dashboard_title": dashboard_name,
            "slug": dashboard_name.lower().replace(" ", "-"),
            "published": True,
        }
        response = self.session.post(
            f"{self.base_url}/api/v1/dashboard/",
            json=dashboard_data
        )
        if response.status_code in [200, 201]:
            dashboard_id = response.json().get("result", {}).get("id")
            print(f"✅ Created dashboard: {dashboard_name} (ID: {dashboard_id})")
            
            # Add charts to dashboard (this requires updating the dashboard JSON)
            # For now, we'll create the dashboard and user can add charts via UI
            # Or we can use the dashboard JSON API to update it
            return dashboard_id
        else:
            print(f"❌ Failed to create dashboard: {response.status_code}")
            return None


def main():
    """Main function to import dashboards."""
    print("🚀 Importing Superset dashboards...")
    
    client = SupersetClient(SUPERSET_URL, SUPERSET_USERNAME, SUPERSET_PASSWORD)
    
    # Get database ID - try different possible names
    db_id = None
    possible_names = [
        "Motherduck - Agile AI",
        "Motherduck",
        "Agile AI",
        "agile_ai_db",
        "DuckDB",  # Default name Superset might use
    ]
    
    # First, list all databases to help debug
    response = client.session.get(f"{client.base_url}/api/v1/database/")
    if response.status_code == 200:
        databases = response.json().get("result", [])
        print(f"\n📋 Available databases:")
        for db in databases:
            db_name = db.get('database_name', 'Unknown')
            db_id_val = db.get('id')
            print(f"   - {db_name} (ID: {db_id_val})")
            if not db_id:
                # Try to find a match
                for name in possible_names:
                    if name.lower() in db_name.lower():
                        db_id = db_id_val
                        print(f"   ✅ Using: {db_name}")
                        break
    else:
        print(f"⚠️  Could not list databases: {response.status_code}")
        if response.status_code == 401:
            print("   Authentication issue - check login credentials")
        print(f"   Response: {response.text[:200]}")
    
    if not db_id:
        # Try exact matches
        for name in possible_names:
            db_id = client.get_database_id(name)
            if db_id:
                print(f"✅ Found database: {name} (ID: {db_id})")
                break
    
    if not db_id and response.status_code == 200 and databases:
        # Use first available database if no match found
        db_id = databases[0].get('id')
        db_name = databases[0].get('database_name')
        print(f"⚠️  No exact match found, using first database: {db_name} (ID: {db_id})")
    
    if not db_id:
        print("\n❌ Could not find database connection")
        print("   Please create the database connection in Superset UI first")
        print("   Or specify the exact name if it's different")
        return
    
    print(f"✅ Using database (ID: {db_id})")
    
    # Create datasets
    print("\n📊 Creating datasets...")
    datasets = {}
    tables = [
        "user_insights",
        "sprint_velocity",
        "sprint_performance",
        "team_member_performance",
        "ticket_aging",
        "sprint_carryover",
        "personal_activity",
        "personal_ticket_status",
        "jira_config",
    ]
    
    for table in tables:
        dataset_id = client.create_dataset(db_id, "gold", table)
        if dataset_id:
            datasets[table] = dataset_id
    
    print(f"\n✅ Created {len(datasets)} datasets")
    
    # Load chart definitions and create charts + dashboards
    chart_defs_path = os.path.join(os.path.dirname(__file__), "chart_definitions.yaml")
    if not os.path.exists(chart_defs_path):
        print(f"\n⚠️  Chart definitions file not found: {chart_defs_path}")
        print("   Creating empty dashboards only...")
        dashboard_names = ["Executive Overview", "Sprint Analytics", "Team Performance", "Ticket Analysis"]
        for name in dashboard_names:
            client.create_dashboard(name, [])
        return
    
    try:
        import yaml
    except ImportError:
        print("\n⚠️  PyYAML not installed. Install with: uv pip install pyyaml")
        print("   Creating empty dashboards only...")
        return
    
    print("\n📊 Creating charts and dashboards from definitions...")
    with open(chart_defs_path, 'r') as f:
        definitions = yaml.safe_load(f)
    
    created_dashboards = {}
    total_charts_created = 0
    
    for dashboard_def in definitions.get("dashboards", []):
        dashboard_name = dashboard_def["name"]
        print(f"\n📈 Dashboard: {dashboard_name}")
        
        # Create or get dashboard
        dashboard_id = client.create_dashboard(dashboard_name, [])
        if not dashboard_id:
            print(f"  ⚠️  Skipping charts - dashboard creation failed")
            continue
        
        created_dashboards[dashboard_name] = dashboard_id
        chart_ids = []
        
        # Create charts for this dashboard
        import time
        for idx, chart_def in enumerate(dashboard_def.get("charts", [])):
            chart_name = chart_def["name"]
            dataset_name = chart_def.get("dataset")
            position = chart_def.get("position", {})
            
            if dataset_name not in datasets:
                print(f"  ⚠️  Skipping '{chart_name}': dataset '{dataset_name}' not found")
                continue
            
            # Add small delay to avoid rate limiting
            if idx > 0:
                time.sleep(0.5)
            
            dataset_id = datasets[dataset_name]
            chart_id = client.create_chart_from_yaml(chart_def, dataset_id, dataset_name)
            
            if chart_id:
                chart_ids.append(chart_id)
                total_charts_created += 1
        
        # Add all charts to dashboard at once (more efficient)
        if chart_ids:
            print(f"  📌 Adding {len(chart_ids)} charts to dashboard...")
            time.sleep(0.5)
            # Map chart names to IDs and positions
            chart_positions = {}
            for chart_def in dashboard_def.get("charts", []):
                chart_name = chart_def["name"]
                chart_id = client._get_chart_id_by_name(chart_name)
                if chart_id and chart_id in chart_ids:
                    chart_positions[chart_id] = chart_def.get("position", {})
            
            # Add all charts to dashboard
            success = client.add_charts_to_dashboard(dashboard_id, chart_ids, chart_positions)
            if success:
                print(f"  ✅ Successfully added charts to dashboard")
        
        print(f"  ✅ Dashboard '{dashboard_name}' has {len(chart_ids)} charts")
    
    print(f"\n✅✅✅ AUTOMATION COMPLETE! ✅✅✅")
    print(f"\n📊 Summary:")
    print(f"   - Datasets: {len(datasets)}")
    print(f"   - Dashboards: {len(created_dashboards)}")
    print(f"   - Charts: {total_charts_created}")
    print(f"\n📝 Next steps:")
    print(f"   1. Go to Superset UI: http://localhost:8088")
    print(f"   2. Review and customize charts in each dashboard:")
    for name, dash_id in created_dashboards.items():
        print(f"      - {name} (ID: {dash_id})")
    print(f"   3. Charts may need SQL queries updated via UI")
    print(f"   4. Use queries from superset/queries.sql as reference")
    print(f"   5. Export completed dashboards to superset/dashboards/ for version control")


if __name__ == "__main__":
    main()

