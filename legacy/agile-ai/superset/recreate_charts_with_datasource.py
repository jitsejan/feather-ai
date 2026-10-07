#!/usr/bin/env python3
"""Recreate charts with proper datasource_id from the start."""

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
    """Recreate charts with datasource."""
    print("⚠️  WARNING: This will delete and recreate charts!")
    print("   This is needed because charts can't have datasource_id updated after creation.")
    print("   Press Ctrl+C to cancel, or wait 5 seconds...")
    import time
    time.sleep(5)
    
    print("\n🔄 Recreating charts...")
    
    session = requests.Session()
    if not login(session):
        print("❌ Failed to login")
        return
    
    # Get datasets
    dataset_resp = session.get(f"{SUPERSET_URL}/api/v1/dataset/")
    datasets = {ds.get("table_name"): ds.get("id") for ds in dataset_resp.json().get("result", [])}
    
    # Chart mapping
    chart_configs = [
        {"name": "Total Issues", "dataset": "user_insights", "viz_type": "big_number"},
        {"name": "Completed Issues", "dataset": "user_insights", "viz_type": "big_number"},
        {"name": "Completion Rate", "dataset": "user_insights", "viz_type": "big_number"},
        {"name": "Active Team Members", "dataset": "user_insights", "viz_type": "big_number"},
        {"name": "Sprint Velocity Trend", "dataset": "sprint_velocity", "viz_type": "line"},
        {"name": "Committed vs Completed", "dataset": "sprint_velocity", "viz_type": "dist_bar"},
        {"name": "Team Performance", "dataset": "user_insights", "viz_type": "dist_bar"},
        {"name": "Ticket Aging", "dataset": "ticket_aging", "viz_type": "dist_bar"},
    ]
    
    # Delete existing charts first
    print("\n🗑️  Deleting existing charts...")
    for config in chart_configs:
        chart_name = config["name"]
        # Find chart by name
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
                        print(f"  ✅ Deleted: {chart_name}")
    
    # Recreate charts with datasource
    print("\n📊 Creating charts with datasource...")
    import time
    for config in chart_configs:
        dataset_name = config["dataset"]
        dataset_id = datasets.get(dataset_name)
        if not dataset_id:
            print(f"  ⚠️  Skipping {config['name']}: dataset {dataset_name} not found")
            continue
        
        chart_data = {
            "slice_name": config["name"],
            "viz_type": config["viz_type"],
            "datasource_id": dataset_id,
            "datasource_type": "table",
            "params": json.dumps({
                "metric": "COUNT(*)",
                "adhoc_filters": []
            })
        }
        
        resp = session.post(f"{SUPERSET_URL}/api/v1/chart/", json=chart_data)
        if resp.status_code in [200, 201]:
            chart_id = resp.json().get("id")
            print(f"  ✅ Created: {config['name']} (ID: {chart_id}, Dataset: {dataset_id})")
        else:
            print(f"  ❌ Failed: {config['name']} - {resp.status_code}")
        
        time.sleep(0.5)  # Rate limiting
    
    print("\n✅ Charts recreated! Now run: make superset-rebuild-dashboards")


if __name__ == "__main__":
    main()



