#!/usr/bin/env python3
"""Fix chart visualization types to use correct Superset viz type names."""

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


def main():
    """Fix chart visualization types."""
    print("🔧 Fixing chart visualization types...")
    
    session = requests.Session()
    if not login(session):
        print("❌ Failed to login")
        return
    
    # Get all charts
    chart_resp = session.get(f"{SUPERSET_URL}/api/v1/chart/")
    charts = chart_resp.json().get("result", [])
    
    # Viz type mapping - fix incorrect types
    viz_type_fixes = {
        "big_number": "big_number_total",
        "dist_bar": "bar",
    }
    
    print(f"Found {len(charts)} charts\n")
    
    fixed = 0
    for chart in charts:
        cid = chart.get("id")
        name = chart.get("slice_name", "")
        current_viz = chart.get("viz_type")
        
        # Check if needs fixing
        new_viz = viz_type_fixes.get(current_viz)
        if new_viz:
            update_data = {
                "viz_type": new_viz
            }
            resp = session.put(f"{SUPERSET_URL}/api/v1/chart/{cid}", json=update_data)
            if resp.status_code == 200:
                print(f"  ✅ Fixed: {name} ({current_viz} → {new_viz})")
                fixed += 1
            else:
                print(f"  ❌ Failed: {name} - {resp.status_code}")
        elif current_viz not in ["big_number_total", "line", "bar", "table", "pie", "area"]:
            print(f"  ⚠️  Unknown viz type: {name} ({current_viz})")
    
    print(f"\n✅ Fixed {fixed} charts")


if __name__ == "__main__":
    main()



