-- Test queries to verify Motherduck connection in Superset
-- Run these in SQL Lab to test your connection

-- Test 1: Basic connection test
SELECT 1 AS test_connection;

-- Test 2: List available schemas
SHOW SCHEMAS;

-- Test 3: List tables in gold schema
SHOW TABLES FROM gold;

-- Test 4: Test query on user_insights (as per user example)
SELECT
  assignee,
  issues_assigned,
  issues_completed
FROM gold.user_insights
LIMIT 100;

-- Test 5: Count records in each gold table
SELECT 'user_insights' AS table_name, COUNT(*) AS record_count FROM gold.user_insights
UNION ALL
SELECT 'sprint_velocity', COUNT(*) FROM gold.sprint_velocity
UNION ALL
SELECT 'sprint_performance', COUNT(*) FROM gold.sprint_performance
UNION ALL
SELECT 'team_member_performance', COUNT(*) FROM gold.team_member_performance
UNION ALL
SELECT 'ticket_aging', COUNT(*) FROM gold.ticket_aging
UNION ALL
SELECT 'sprint_carryover', COUNT(*) FROM gold.sprint_carryover
UNION ALL
SELECT 'personal_activity', COUNT(*) FROM gold.personal_activity
UNION ALL
SELECT 'personal_ticket_status', COUNT(*) FROM gold.personal_ticket_status
UNION ALL
SELECT 'jira_config', COUNT(*) FROM gold.jira_config;

