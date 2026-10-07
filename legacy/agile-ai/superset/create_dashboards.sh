#!/bin/bash
# Script to create datasets and prepare for dashboard import
# This sets up all the datasets needed for the dashboards

set -e

echo "🚀 Setting up Superset datasets..."

# Check if Superset is running
if ! docker-compose ps | grep -q superset; then
    echo "❌ Superset is not running. Start it with: make superset-up"
    exit 1
fi

echo "✅ Superset is running"
echo ""
echo "📋 Next steps:"
echo "   1. Go to http://localhost:8088"
echo "   2. Login (admin/admin)"
echo "   3. Go to Data → Datasets → + Dataset"
echo "   4. Create datasets for these tables in 'gold' schema:"
echo "      - user_insights"
echo "      - sprint_velocity"
echo "      - sprint_performance"
echo "      - team_member_performance"
echo "      - ticket_aging"
echo "      - sprint_carryover"
echo "      - personal_activity"
echo "      - personal_ticket_status"
echo "      - jira_config"
echo ""
echo "   Or run: python superset/import_dashboards.py"
echo "   (This will create datasets via API)"

