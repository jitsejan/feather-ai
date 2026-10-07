#!/usr/bin/env python3
"""Rebuild all dashboards with proper position_json structure."""

import requests
import json
import os
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


def rebuild_dashboard(session: requests.Session, dashboard_id: int, chart_ids: List[int], positions: List[Dict]) -> bool:
    """Rebuild a dashboard's position_json from scratch."""
    # Get charts
    chart_resp = session.get(f"{SUPERSET_URL}/api/v1/chart/")
    charts = {}
    if chart_resp.status_code == 200:
        for chart in chart_resp.json().get("result", []):
            charts[chart.get("id")] = chart.get("slice_name", "")
    
    # Build new position_json
    new_pos_json = {
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
    
    # Add all charts with proper structure
    for idx, chart_id in enumerate(chart_ids):
        chart_key = f"CHART-{chart_id}"
        pos = positions[idx] if idx < len(positions) else {"x": 0, "y": 0, "w": 4, "h": 4}
        
        new_pos_json["GRID_ID"]["children"].append(chart_key)
        new_pos_json[chart_key] = {
            "type": "CHART",
            "id": chart_key,
            "meta": {
                "chartId": chart_id,
                "width": pos["w"],
                "height": pos["h"],
                "sliceName": charts.get(chart_id, "")
            },
            "x": pos["x"],
            "y": pos["y"],
            "w": pos["w"],
            "h": pos["h"]
        }
    
    # Update dashboard
    update_data = {"position_json": json.dumps(new_pos_json)}
    resp = session.put(f"{SUPERSET_URL}/api/v1/dashboard/{dashboard_id}", json=update_data)
    return resp.status_code == 200


def main():
    """Rebuild all dashboards."""
    print("🔧 Rebuilding dashboards...")
    
    session = requests.Session()
    if not login(session):
        print("❌ Failed to login")
        return
    
    # Dashboard configurations
    dashboards = {
        1: {  # Executive Overview
            "chart_ids": [1, 2, 3, 4, 5, 6, 7, 8],
            "positions": [
                {"x": 0, "y": 0, "w": 3, "h": 4},   # Total Issues
                {"x": 3, "y": 0, "w": 3, "h": 4},   # Completed Issues
                {"x": 6, "y": 0, "w": 3, "h": 4},   # Completion Rate
                {"x": 9, "y": 0, "w": 3, "h": 4},   # Active Team Members
                {"x": 0, "y": 4, "w": 6, "h": 8},   # Sprint Velocity Trend
                {"x": 6, "y": 4, "w": 6, "h": 8},   # Committed vs Completed
                {"x": 0, "y": 12, "w": 6, "h": 8},  # Team Performance
                {"x": 6, "y": 12, "w": 6, "h": 8}, # Ticket Aging
            ]
        },
        2: {  # Sprint Analytics
            "chart_ids": [9, 10, 11, 12, 13, 14],
            "positions": [
                {"x": 0, "y": 0, "w": 4, "h": 4},
                {"x": 4, "y": 0, "w": 4, "h": 4},
                {"x": 8, "y": 0, "w": 4, "h": 4},
                {"x": 0, "y": 4, "w": 4, "h": 4},
                {"x": 4, "y": 4, "w": 8, "h": 8},
                {"x": 0, "y": 12, "w": 12, "h": 8},
            ]
        },
        3: {  # Team Performance
            "chart_ids": [15, 16, 17, 19, 20, 3],
            "positions": [
                {"x": 0, "y": 0, "w": 4, "h": 4},
                {"x": 4, "y": 0, "w": 4, "h": 4},
                {"x": 8, "y": 0, "w": 4, "h": 4},
                {"x": 0, "y": 4, "w": 4, "h": 4},
                {"x": 4, "y": 4, "w": 8, "h": 8},
                {"x": 0, "y": 12, "w": 12, "h": 8},
            ]
        },
        4: {  # Ticket Analysis
            "chart_ids": [21, 22, 23, 24, 25, 26, 27],
            "positions": [
                {"x": 0, "y": 0, "w": 3, "h": 4},
                {"x": 3, "y": 0, "w": 3, "h": 4},
                {"x": 6, "y": 0, "w": 3, "h": 4},
                {"x": 9, "y": 0, "w": 3, "h": 4},
                {"x": 0, "y": 4, "w": 6, "h": 8},
                {"x": 6, "y": 4, "w": 6, "h": 8},
                {"x": 0, "y": 12, "w": 12, "h": 8},
            ]
        },
    }
    
    # Get dashboard names
    resp = session.get(f"{SUPERSET_URL}/api/v1/dashboard/")
    dashboard_names = {}
    if resp.status_code == 200:
        for dash in resp.json().get("result", []):
            dashboard_names[dash.get("id")] = dash.get("dashboard_title")
    
    fixed = 0
    for dash_id, config in dashboards.items():
        dash_name = dashboard_names.get(dash_id, f"Dashboard {dash_id}")
        if rebuild_dashboard(session, dash_id, config["chart_ids"], config["positions"]):
            print(f"  ✅ Rebuilt: {dash_name} (ID: {dash_id})")
            fixed += 1
        else:
            print(f"  ❌ Failed: {dash_name} (ID: {dash_id})")
    
    print(f"\n✅ Rebuilt {fixed} dashboards")


if __name__ == "__main__":
    main()



