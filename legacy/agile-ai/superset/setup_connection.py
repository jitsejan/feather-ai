#!/usr/bin/env python3
"""
Script to set up Motherduck connection in Superset via API.
This is an alternative to setting up the connection manually in the UI.
"""
import os
import requests
import json
from typing import Optional

SUPERSET_URL = os.environ.get("SUPERSET_URL", "http://localhost:8088")
SUPERSET_USERNAME = os.environ.get("SUPERSET_USERNAME", "admin")
SUPERSET_PASSWORD = os.environ.get("SUPERSET_PASSWORD", "admin")
MOTHERDUCK_TOKEN = os.environ.get("MOTHERDUCK_TOKEN")

if not MOTHERDUCK_TOKEN:
    print("❌ MOTHERDUCK_TOKEN environment variable not set")
    exit(1)


def get_csrf_token(session: requests.Session) -> Optional[str]:
    """Get CSRF token from Superset login page."""
    response = session.get(f"{SUPERSET_URL}/login/")
    if response.status_code != 200:
        print(f"❌ Failed to access Superset login page: {response.status_code}")
        return None
    
    # Extract CSRF token from the response
    # This is a simplified approach - in production, use proper HTML parsing
    csrf_token = None
    for line in response.text.split("\n"):
        if 'csrf_token' in line.lower() or 'csrf' in line.lower():
            # Try to extract token - this is a basic implementation
            import re
            match = re.search(r'value="([^"]+)"', line)
            if match:
                csrf_token = match.group(1)
                break
    
    return csrf_token


def login(session: requests.Session) -> bool:
    """Login to Superset and get authentication token."""
    # Get CSRF token
    csrf_token = get_csrf_token(session)
    
    # Login
    login_data = {
        "username": SUPERSET_USERNAME,
        "password": SUPERSET_PASSWORD,
    }
    if csrf_token:
        login_data["csrf_token"] = csrf_token
    
    response = session.post(
        f"{SUPERSET_URL}/login/",
        data=login_data,
        allow_redirects=False
    )
    
    if response.status_code in [200, 302]:
        print("✅ Successfully logged in to Superset")
        return True
    else:
        print(f"❌ Login failed: {response.status_code}")
        print(response.text[:500])
        return False


def create_database_connection(session: requests.Session) -> bool:
    """Create Motherduck database connection in Superset."""
    connection_string = f"duckdb:///agile_ai_db?motherduck_token={MOTHERDUCK_TOKEN}"
    
    db_data = {
        "database_name": "Motherduck - Agile AI",
        "sqlalchemy_uri": connection_string,
        "engine": "duckdb",
        "impersonate_user": False,
        "allow_ctas": True,
        "allow_cvas": True,
        "allow_run_async": True,
        "cache_timeout": None,
        "expose_in_sqllab": True,
        "allow_dml": False,
        "force_ctas_schema": None,
        "extra": json.dumps({
            "metadata_params": {},
            "engine_params": {},
            "metadata_cache_timeout": {},
            "schemas_allowed_for_csv_upload": ["gold"],
        }),
    }
    
    # Try to get CSRF token for API
    response = session.get(f"{SUPERSET_URL}/api/v1/database/")
    if response.status_code == 200:
        # Check if connection already exists
        databases = response.json().get("result", [])
        for db in databases:
            if db.get("database_name") == "Motherduck - Agile AI":
                print("✅ Database connection already exists")
                return True
    
    # Create new connection
    response = session.post(
        f"{SUPERSET_URL}/api/v1/database/",
        json=db_data,
        headers={"Content-Type": "application/json"}
    )
    
    if response.status_code in [200, 201]:
        print("✅ Successfully created database connection")
        print(f"   Database: {db_data['database_name']}")
        return True
    else:
        print(f"❌ Failed to create database connection: {response.status_code}")
        print(response.text[:500])
        return False


def main():
    """Main function to set up Superset connection."""
    print("🚀 Setting up Motherduck connection in Superset...")
    print(f"   Superset URL: {SUPERSET_URL}")
    
    session = requests.Session()
    
    # Login
    if not login(session):
        print("\n💡 Tip: Make sure Superset is running and you have the correct credentials")
        print("   You can also set up the connection manually in the Superset UI")
        return
    
    # Create database connection
    if create_database_connection(session):
        print("\n✅ Setup complete!")
        print(f"   You can now access Superset at {SUPERSET_URL}")
        print("   Go to Data → Datasets to start creating charts")
    else:
        print("\n💡 You can set up the connection manually:")
        print("   1. Go to Settings → Database Connections → + Database")
        print("   2. Select DuckDB")
        print(f"   3. Connection string: duckdb:///agile_ai_db?motherduck_token={MOTHERDUCK_TOKEN[:20]}...")


if __name__ == "__main__":
    main()

