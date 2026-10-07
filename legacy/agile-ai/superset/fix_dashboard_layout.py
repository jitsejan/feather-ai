#!/usr/bin/env python3
"""Fix dashboard position_json structure to match Superset's expected format."""

import requests
import json
import os
from typing import Dict

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


def fix_dashboard_position_json(session: requests.Session, dashboard_id: int) -> bool:
    """Fix position_json structure for a dashboard."""
    # Get dashboard
    resp = session.get(f"{SUPERSET_URL}/api/v1/dashboard/{dashboard_id}")
    if resp.status_code != 200:
        return False
    
    dashboard = resp.json().get("result", {})
    position_json = dashboard.get("position_json")
    
    # Parse if string
    if isinstance(position_json, str):
        try:
            position_json = json.loads(position_json)
        except:
            position_json = {}
    
    if not position_json or not isinstance(position_json, dict):
        return False
    
    # Get all charts to fetch slice names
    chart_resp = session.get(f"{SUPERSET_URL}/api/v1/chart/")
    charts = {}
    if chart_resp.status_code == 200:
        for chart in chart_resp.json().get("result", []):
            charts[chart.get("id")] = chart.get("slice_name", "")
    
    # Fix all entries that have a meta property
    fixed = False
    for key, value in position_json.items():
        if isinstance(value, dict):
            # Check if this entry has a meta property
            if "meta" in value:
                meta = value.get("meta", {})
                if not isinstance(meta, dict):
                    meta = {}
                
                # Ensure meta has width and height for all types
                if "width" not in meta:
                    meta["width"] = value.get("w", 4)
                if "height" not in meta:
                    meta["height"] = value.get("h", 4)
                
                # For CHART type, also ensure chartId and sliceName
                if value.get("type") == "CHART":
                    if "chartId" not in meta:
                        # Extract from key like "CHART-123"
                        try:
                            chart_id = int(key.split("-")[1])
                            meta["chartId"] = chart_id
                            # Also set sliceName if we have it
                            if chart_id in charts:
                                meta["sliceName"] = charts[chart_id]
                        except:
                            pass
                    elif "sliceName" not in meta and "chartId" in meta:
                        # Add sliceName if missing
                        chart_id = meta.get("chartId")
                        if chart_id in charts:
                            meta["sliceName"] = charts[chart_id]
                
                value["meta"] = meta
                fixed = True
            # Also check if entry has w/h but no meta - create meta
            elif value.get("w") is not None or value.get("h") is not None:
                meta = {
                    "width": value.get("w", 4),
                    "height": value.get("h", 4),
                }
                # For CHART type, add chartId
                if value.get("type") == "CHART":
                    try:
                        chart_id = int(key.split("-")[1])
                        meta["chartId"] = chart_id
                        if chart_id in charts:
                            meta["sliceName"] = charts[chart_id]
                    except:
                        pass
                value["meta"] = meta
                fixed = True
    
    if fixed:
        # Update dashboard
        update_data = {
            "position_json": json.dumps(position_json)
        }
        resp = session.put(
            f"{SUPERSET_URL}/api/v1/dashboard/{dashboard_id}",
            json=update_data
        )
        return resp.status_code in [200, 201]
    
    return False


def main():
    """Fix all dashboards."""
    print("🔧 Fixing dashboard layouts...")
    
    session = requests.Session()
    if not login(session):
        print("❌ Failed to login")
        return
    
    # Get all dashboards
    resp = session.get(f"{SUPERSET_URL}/api/v1/dashboard/")
    dashboards = resp.json().get("result", [])
    
    print(f"Found {len(dashboards)} dashboards\n")
    
    fixed = 0
    for dash in dashboards:
        dash_id = dash.get("id")
        dash_name = dash.get("dashboard_title")
        
        if fix_dashboard_position_json(session, dash_id):
            print(f"  ✅ Fixed: {dash_name} (ID: {dash_id})")
            fixed += 1
        else:
            print(f"  ⚠️  Skipped: {dash_name} (ID: {dash_id})")
    
    print(f"\n✅ Fixed {fixed} dashboards")


if __name__ == "__main__":
    main()

