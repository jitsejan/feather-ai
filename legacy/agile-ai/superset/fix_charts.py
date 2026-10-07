#!/usr/bin/env python3
"""Fix existing charts by linking them to datasets and configuring metrics."""

import requests
import json
import os
from typing import Dict, Optional

SUPERSET_URL = os.environ.get("SUPERSET_URL", "http://localhost:8088")
SUPERSET_USERNAME = os.environ.get("SUPERSET_USERNAME", "admin")
SUPERSET_PASSWORD = os.environ.get("SUPERSET_PASSWORD", "admin")


def login(session: requests.Session) -> bool:
    """Login to Superset."""
    auth_data = {
        "username": SUPERSET_USERNAME,
        "password": SUPERSET_PASSWORD,
        "provider": "db",
        "refresh": True
    }
    
    response = session.post(
        f"{SUPERSET_URL}/api/v1/security/login",
        json=auth_data,
        headers={"Content-Type": "application/json"}
    )

    if response.status_code == 200:
        data = response.json()
        access_token = data.get("access_token")
        
        if access_token:
            session.headers.update({
                "Authorization": f"Bearer {access_token}",
                "Content-Type": "application/json"
            })
            
            # Get CSRF token
            csrf_resp = session.get(f"{SUPERSET_URL}/api/v1/security/csrf_token/")
            if csrf_resp.status_code == 200:
                csrf_data = csrf_resp.json()
                csrf_token = csrf_data.get("result")
                if csrf_token:
                    session.headers["X-CSRFToken"] = csrf_token
                    session.headers["Referer"] = SUPERSET_URL
            
            return True
    return False


def main():
    """Fix all charts."""
    print("🔧 Fixing charts...")
    
    session = requests.Session()
    if not login(session):
        print("❌ Failed to login")
        return
    
    # Get all charts
    chart_resp = session.get(f"{SUPERSET_URL}/api/v1/chart/")
    charts = chart_resp.json().get("result", [])
    
    # Get all datasets
    dataset_resp = session.get(f"{SUPERSET_URL}/api/v1/dataset/")
    datasets = {ds.get("table_name"): ds.get("id") for ds in dataset_resp.json().get("result", [])}
    
    print(f"Found {len(charts)} charts and {len(datasets)} datasets\n")
    
    # Map chart names to datasets
    chart_to_dataset = {
        "user_insights": ["Total Issues", "Completed Issues", "Completion Rate", "Active Team Members", 
                          "Team Performance", "Team Size", "Total Workload", "Avg per Person", 
                          "Team Completion", "Workload vs Completion"],
        "sprint_velocity": ["Sprint Velocity Trend", "Committed vs Completed", "Total Sprints",
                           "Avg Completion Rate", "Avg Velocity", "Best Velocity", "Velocity Trends",
                           "Completion Rate by Sprint"],
        "ticket_aging": ["Ticket Aging", "Total Tickets", "Avg Age (days)", "Oldest Ticket",
                        "Critical (>90d)", "Tickets by Age Category", "Tickets by Status",
                        "Aging by Assignee"],
        "sprint_performance": ["Sprint Performance"],
        "team_member_performance": ["Team Member Performance"],
    }
    
    # Reverse mapping
    name_to_dataset = {}
    for dataset, names in chart_to_dataset.items():
        for name in names:
            name_to_dataset[name] = dataset
    
    fixed = 0
    for chart in charts:
        cid = chart.get("id")
        name = chart.get("slice_name", "")
        current_ds_id = chart.get("datasource_id")
        
        # Find dataset for this chart
        dataset_name = name_to_dataset.get(name)
        if not dataset_name:
            # Try fuzzy matching
            name_lower = name.lower()
            if any(word in name_lower for word in ["issue", "team", "member", "workload", "completion"]):
                dataset_name = "user_insights"
            elif any(word in name_lower for word in ["sprint", "velocity", "committed"]):
                dataset_name = "sprint_velocity"
            elif any(word in name_lower for word in ["ticket", "aging", "age", "oldest"]):
                dataset_name = "ticket_aging"
        
        if dataset_name and dataset_name in datasets:
            target_ds_id = datasets[dataset_name]
            
            # Get current params
            current_params = chart.get("params", "{}")
            if isinstance(current_params, str):
                try:
                    current_params = json.loads(current_params)
                except:
                    current_params = {}
            
            # Update chart
            update_data = {
                "datasource_id": target_ds_id,
                "datasource_type": "table",
            }
            
            # Add metric if missing and it's a big_number chart
            if chart.get("viz_type") == "big_number" and not current_params.get("metric"):
                # Try to infer metric from chart name
                if "total" in name.lower() or "sum" in name.lower():
                    update_data["params"] = json.dumps({"metric": "COUNT(*)", "adhoc_filters": []})
                elif "avg" in name.lower() or "average" in name.lower():
                    update_data["params"] = json.dumps({"metric": "AVG(*)", "adhoc_filters": []})
                else:
                    update_data["params"] = json.dumps({"metric": "COUNT(*)", "adhoc_filters": []})
            
            resp = session.put(f"{SUPERSET_URL}/api/v1/chart/{cid}", json=update_data)
            if resp.status_code == 200:
                print(f"  ✅ Fixed: {name} -> {dataset_name} (dataset ID: {target_ds_id})")
                fixed += 1
            else:
                print(f"  ❌ Failed: {name} - {resp.status_code}: {resp.text[:100]}")
        else:
            print(f"  ⚠️  Skipped: {name} (no dataset mapping found)")
    
    print(f"\n✅ Fixed {fixed} charts")


if __name__ == "__main__":
    main()



