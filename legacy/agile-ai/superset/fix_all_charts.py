#!/usr/bin/env python3
"""Comprehensive fix: Delete and recreate all charts with proper datasource_id and viz_type."""

import requests
import json
import os
import time
from typing import Dict, List

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
    """Fix all charts by recreating them."""
    print("⚠️  This will DELETE and RECREATE all charts!")
    print("   Charts will be recreated with proper datasource_id and viz_type.")
    print("   Press Ctrl+C to cancel, or wait 3 seconds...")
    time.sleep(3)
    
    print("\n🔄 Fixing all charts...")
    
    session = requests.Session()
    if not login(session):
        print("❌ Failed to login")
        return
    
    # Get datasets
    dataset_resp = session.get(f"{SUPERSET_URL}/api/v1/dataset/")
    datasets = {ds.get("table_name"): ds.get("id") for ds in dataset_resp.json().get("result", [])}
    print(f"Found {len(datasets)} datasets")
    
    # Chart configurations with correct viz types
    chart_configs = [
        {"name": "Total Issues", "dataset": "user_insights", "viz_type": "big_number_total"},
        {"name": "Completed Issues", "dataset": "user_insights", "viz_type": "big_number_total"},
        {"name": "Completion Rate", "dataset": "user_insights", "viz_type": "big_number_total"},
        {"name": "Active Team Members", "dataset": "user_insights", "viz_type": "big_number_total"},
        {"name": "Sprint Velocity Trend", "dataset": "sprint_velocity", "viz_type": "line"},
        {"name": "Committed vs Completed", "dataset": "sprint_velocity", "viz_type": "bar"},
        {"name": "Team Performance", "dataset": "user_insights", "viz_type": "bar"},
        {"name": "Ticket Aging", "dataset": "ticket_aging", "viz_type": "bar"},
        {"name": "Total Sprints", "dataset": "sprint_velocity", "viz_type": "big_number_total"},
        {"name": "Avg Completion Rate", "dataset": "sprint_velocity", "viz_type": "big_number_total"},
        {"name": "Avg Velocity", "dataset": "sprint_velocity", "viz_type": "big_number_total"},
        {"name": "Best Velocity", "dataset": "sprint_velocity", "viz_type": "big_number_total"},
        {"name": "Velocity Trends", "dataset": "sprint_velocity", "viz_type": "line"},
        {"name": "Completion Rate by Sprint", "dataset": "sprint_velocity", "viz_type": "bar"},
        {"name": "Team Size", "dataset": "user_insights", "viz_type": "big_number_total"},
        {"name": "Total Workload", "dataset": "user_insights", "viz_type": "big_number_total"},
        {"name": "Avg per Person", "dataset": "user_insights", "viz_type": "big_number_total"},
        {"name": "Team Completion", "dataset": "user_insights", "viz_type": "big_number_total"},
        {"name": "Workload vs Completion", "dataset": "user_insights", "viz_type": "bar"},
        {"name": "Total Tickets", "dataset": "ticket_aging", "viz_type": "big_number_total"},
        {"name": "Avg Age (days)", "dataset": "ticket_aging", "viz_type": "big_number_total"},
        {"name": "Oldest Ticket", "dataset": "ticket_aging", "viz_type": "big_number_total"},
        {"name": "Critical (>90d)", "dataset": "ticket_aging", "viz_type": "big_number_total"},
        {"name": "Tickets by Age Category", "dataset": "ticket_aging", "viz_type": "bar"},
        {"name": "Tickets by Status", "dataset": "ticket_aging", "viz_type": "bar"},
        {"name": "Aging by Assignee", "dataset": "ticket_aging", "viz_type": "bar"},
    ]
    
    # Step 1: Delete existing charts
    print("\n🗑️  Step 1: Deleting existing charts...")
    deleted = 0
    for config in chart_configs:
        chart_name = config["name"]
        chart_resp = session.get(
            f"{SUPERSET_URL}/api/v1/chart/",
            params={"q": json.dumps({"filters": [{"col": "slice_name", "opr": "eq", "value": chart_name}]})}
        )
        if chart_resp.status_code == 200:
            charts = chart_resp.json().get("result", [])
            for chart in charts:
                if chart.get("slice_name") == chart_name:
                    chart_id = chart.get("id")
                    del_resp = session.delete(f"{SUPERSET_URL}/api/v1/chart/{chart_id}")
                    if del_resp.status_code in [200, 204]:
                        deleted += 1
                        print(f"  ✅ Deleted: {chart_name}")
                    time.sleep(0.2)
    
    print(f"Deleted {deleted} charts")
    
    # Step 2: Recreate charts with proper config
    print("\n📊 Step 2: Creating charts with proper datasource and viz_type...")
    created = 0
    chart_id_map = {}  # Map chart name to new ID
    
    for config in chart_configs:
        dataset_name = config["dataset"]
        dataset_id = datasets.get(dataset_name)
        if not dataset_id:
            print(f"  ⚠️  Skipping {config['name']}: dataset {dataset_name} not found")
            continue
        
        # Build params based on viz type
        if config["viz_type"] == "big_number_total":
            params = {"metric": "COUNT(*)", "adhoc_filters": []}
        elif config["viz_type"] == "line":
            params = {"metrics": ["COUNT(*)"], "groupby": [], "adhoc_filters": []}
        elif config["viz_type"] == "bar":
            params = {"metrics": ["COUNT(*)"], "groupby": [], "adhoc_filters": []}
        else:
            params = {"adhoc_filters": []}
        
        chart_data = {
            "slice_name": config["name"],
            "viz_type": config["viz_type"],
            "datasource_id": dataset_id,
            "datasource_type": "table",
            "params": json.dumps(params)
        }
        
        resp = session.post(f"{SUPERSET_URL}/api/v1/chart/", json=chart_data)
        if resp.status_code in [200, 201]:
            data = resp.json()
            chart_id = data.get("id") or data.get("result", {}).get("id")
            if chart_id:
                chart_id_map[config["name"]] = chart_id
                created += 1
                print(f"  ✅ Created: {config['name']} (ID: {chart_id}, Dataset: {dataset_id}, Viz: {config['viz_type']})")
            else:
                print(f"  ⚠️  Created but no ID: {config['name']}")
        else:
            print(f"  ❌ Failed: {config['name']} - {resp.status_code}: {resp.text[:100]}")
        
        time.sleep(0.5)  # Rate limiting
    
    print(f"\n✅ Created {created} charts")
    print("\n📝 Next step: Run 'make superset-rebuild-dashboards' to rebuild dashboards with new chart IDs")


if __name__ == "__main__":
    main()



