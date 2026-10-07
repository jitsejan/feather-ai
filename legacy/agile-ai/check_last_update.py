#!/usr/bin/env python3
"""Quick script to check the last updated timestamp in MotherDuck."""
import duckdb
import os

motherduck_token = os.environ.get("MOTHERDUCK_TOKEN")
if not motherduck_token:
    print("❌ MOTHERDUCK_TOKEN not set")
    exit(1)

conn = duckdb.connect(f"md:agile_ai_db?motherduck_token={motherduck_token}")

# Check for DT project
print("🔍 Checking DT project data...")
try:
    result = conn.execute("""
        SELECT
            COUNT(*) as count,
            MIN(key) as min_key,
            MAX(key) as max_key,
            MAX(fields__updated) as last_updated
        FROM raw.issues
        WHERE split_part(key, '-', 1) = 'DT'
    """).fetchone()

    if result[0] > 0:
        print(f"✅ Found {result[0]} DT issues")
        print(f"   Key range: {result[1]} to {result[2]}")
        print(f"   Last updated: {result[3]}")
    else:
        print("📥 No DT issues found (table might be empty)")
except Exception as e:
    print(f"⚠️  Query failed: {e}")

# Check for TI project
print("\n🔍 Checking TI project data...")
try:
    result = conn.execute("""
        SELECT
            COUNT(*) as count,
            MIN(key) as min_key,
            MAX(key) as max_key,
            MAX(fields__updated) as last_updated
        FROM raw.issues
        WHERE split_part(key, '-', 1) = 'TI'
    """).fetchone()

    if result[0] > 0:
        print(f"✅ Found {result[0]} TI issues")
        print(f"   Key range: {result[1]} to {result[2]}")
        print(f"   Last updated: {result[3]}")
    else:
        print("📥 No TI issues found (table might be empty)")
except Exception as e:
    print(f"⚠️  Query failed: {e}")

conn.close()
