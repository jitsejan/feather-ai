-- SQL Queries for Superset Dashboards
-- These replicate the Evidence dashboard queries
--
-- Database Structure:
--   Database: agile_ai_db
--   Schema: gold
--   Tables: user_insights, sprint_velocity, sprint_performance, etc.
--
-- In Superset, when connected to agile_ai_db, reference tables as:
--   gold.table_name (e.g., gold.user_insights)
--
-- Example:
--   SELECT assignee, issues_assigned, issues_completed
--   FROM gold.user_insights
--   LIMIT 100;

-- ============================================
-- EXECUTIVE OVERVIEW (index.md)
-- ============================================

-- KPIs Query
SELECT
  SUM(issues_assigned) AS total_issues,
  SUM(issues_completed) AS completed_issues,
  ROUND(100.0 * SUM(issues_completed) / NULLIF(SUM(issues_assigned), 0), 1) AS overall_completion_rate,
  COUNT(DISTINCT assignee) AS active_team_members
FROM gold.user_insights
WHERE assignee IS NOT NULL;

-- Sprint Summary
SELECT
  COUNT(*) AS total_sprints,
  ROUND(AVG(completion_ratio) * 100, 1) AS avg_completion_rate,
  ROUND(AVG(issues_completed), 1) AS avg_velocity
FROM gold.sprint_velocity;

-- Sprint Velocity Trend
SELECT
  sprint_name,
  start_date,
  end_date,
  issues_committed,
  issues_completed,
  ROUND(completion_ratio * 100, 1) AS completion_pct
FROM gold.sprint_velocity
ORDER BY start_date ASC;

-- Team Performance (All Time)
SELECT
  assignee,
  issues_assigned,
  issues_completed,
  ROUND(100.0 * issues_completed / NULLIF(issues_assigned, 0), 1) AS completion_pct
FROM gold.user_insights
WHERE assignee IS NOT NULL
ORDER BY issues_assigned DESC;

-- Team Performance (Current Sprint)
SELECT
  assignee,
  SUM(issues_assigned) AS issues_assigned,
  SUM(issues_completed) AS issues_completed,
  ROUND(100.0 * SUM(issues_completed) / NULLIF(SUM(issues_assigned), 0), 1) AS completion_pct
FROM gold.team_member_performance
WHERE sprint_id = (
  SELECT sprint_id 
  FROM gold.sprint_velocity 
  ORDER BY start_date DESC 
  LIMIT 1
)
AND assignee IS NOT NULL
GROUP BY assignee
ORDER BY issues_assigned DESC;

-- Ticket Aging Summary
SELECT
  CASE
    WHEN days_in_status < 7 THEN '< 1 week'
    WHEN days_in_status < 30 THEN '1-4 weeks'
    WHEN days_in_status < 90 THEN '1-3 months'
    ELSE '> 3 months'
  END AS age_bucket,
  COUNT(*) AS ticket_count,
  ROUND(AVG(days_in_status), 1) AS avg_days
FROM gold.ticket_aging
GROUP BY 1
ORDER BY MIN(days_in_status);

-- Top Aging Tickets
SELECT
  t.issue_key,
  c.jira_base_url || '/browse/' || t.issue_key AS issue_url,
  t.assignee,
  t.status,
  t.days_in_status
FROM gold.ticket_aging t
LEFT JOIN gold.jira_config c
  ON SPLIT_PART(t.issue_key, '-', 1) = c.project_key
ORDER BY t.days_in_status DESC
LIMIT 10;

-- ============================================
-- SPRINT ANALYTICS (sprints.md)
-- ============================================

-- Sprint Performance
SELECT *
FROM gold.sprint_performance
ORDER BY start_date ASC;

-- Sprint Velocity
SELECT *
FROM gold.sprint_velocity
ORDER BY start_date ASC;

-- Sprint Carryover
SELECT *
FROM gold.sprint_carryover
ORDER BY from_sprint_number DESC, changed_at DESC;

-- Sprint KPIs
SELECT
  COUNT(*) AS total_sprints,
  ROUND(AVG(completion_ratio) * 100, 1) AS avg_completion_rate,
  ROUND(AVG(issues_completed), 1) AS avg_velocity,
  MAX(issues_completed) AS best_velocity
FROM gold.sprint_velocity;

-- Carryover by Sprint
SELECT
  c.from_sprint AS sprint_name,
  COUNT(DISTINCT c.issue_id) AS carryover_count,
  COUNT(DISTINCT c.issue_key) AS issues_moved,
  SUM(c.story_points) AS total_story_points,
  MIN(s.start_date) AS start_date
FROM gold.sprint_carryover c
LEFT JOIN gold.sprint_performance s ON c.from_sprint = s.sprint_name
WHERE c.from_sprint IS NOT NULL
GROUP BY c.from_sprint
ORDER BY start_date ASC;

-- Carryover Summary
SELECT
  CASE
    WHEN carryover_count = 0 THEN 'No Carryover'
    WHEN carryover_count <= 3 THEN 'Low (1-3)'
    WHEN carryover_count <= 6 THEN 'Medium (4-6)'
    ELSE 'High (>6)'
  END AS carryover_category,
  COUNT(*) AS sprint_count
FROM (
  SELECT
    c.from_sprint,
    COUNT(DISTINCT c.issue_id) AS carryover_count
  FROM gold.sprint_carryover c
  WHERE c.from_sprint IS NOT NULL
  GROUP BY c.from_sprint
) sub
GROUP BY 1
ORDER BY CASE
  WHEN carryover_category = 'No Carryover' THEN 0
  WHEN carryover_category = 'Low (1-3)' THEN 1
  WHEN carryover_category = 'Medium (4-6)' THEN 2
  ELSE 3
END;

-- ============================================
-- TEAM PERFORMANCE (team.md)
-- ============================================

-- Team Member Performance
SELECT *
FROM gold.team_member_performance
ORDER BY assignee;

-- User Insights
SELECT *
FROM gold.user_insights
WHERE assignee IS NOT NULL
ORDER BY issues_assigned DESC;

-- Team KPIs
SELECT
  COUNT(DISTINCT assignee) AS team_size,
  SUM(issues_assigned) AS total_workload,
  ROUND(AVG(issues_assigned), 1) AS avg_per_person,
  ROUND(100.0 * SUM(issues_completed) / NULLIF(SUM(issues_assigned), 0), 1) AS team_completion_rate
FROM gold.user_insights
WHERE assignee IS NOT NULL;

-- Completion Rates
SELECT
  assignee,
  issues_assigned,
  issues_completed,
  ROUND(100.0 * issues_completed / NULLIF(issues_assigned, 0), 1) AS completion_rate
FROM gold.user_insights
WHERE assignee IS NOT NULL
ORDER BY completion_rate DESC;

-- Workload Buckets
SELECT
  CASE
    WHEN issues_assigned < 5 THEN '< 5 issues'
    WHEN issues_assigned < 10 THEN '5-9 issues'
    WHEN issues_assigned < 20 THEN '10-19 issues'
    ELSE '20+ issues'
  END AS workload_bucket,
  COUNT(*) AS team_members,
  SUM(issues_assigned) AS total_issues
FROM gold.user_insights
WHERE assignee IS NOT NULL
GROUP BY 1
ORDER BY MIN(issues_assigned);

-- Top Performers
SELECT
  assignee,
  issues_completed,
  ROUND(100.0 * issues_completed / NULLIF(issues_assigned, 0), 1) AS completion_rate
FROM gold.user_insights
WHERE assignee IS NOT NULL
ORDER BY issues_completed DESC
LIMIT 5;

-- Needs Attention
SELECT
  assignee,
  issues_assigned,
  issues_completed,
  (issues_assigned - issues_completed) AS backlog,
  ROUND(100.0 * issues_completed / NULLIF(issues_assigned, 0), 1) AS completion_rate
FROM gold.user_insights
WHERE assignee IS NOT NULL
  AND (issues_assigned - issues_completed) > 5
ORDER BY backlog DESC;

-- ============================================
-- TICKET ANALYSIS (tickets.md)
-- ============================================

-- Ticket Aging
SELECT *
FROM gold.ticket_aging
ORDER BY days_in_status DESC;

-- Aging KPIs
SELECT
  COUNT(*) AS total_tickets,
  ROUND(AVG(days_in_status), 1) AS avg_age,
  MAX(days_in_status) AS oldest_ticket,
  COUNT(CASE WHEN days_in_status > 90 THEN 1 END) AS critical_aging
FROM gold.ticket_aging;

-- Age Buckets
SELECT
  CASE
    WHEN days_in_status < 7 THEN '< 1 week'
    WHEN days_in_status < 30 THEN '1-4 weeks'
    WHEN days_in_status < 90 THEN '1-3 months'
    WHEN days_in_status < 180 THEN '3-6 months'
    ELSE '> 6 months'
  END AS age_bucket,
  COUNT(*) AS ticket_count,
  ROUND(AVG(days_in_status), 1) AS avg_days,
  ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) AS pct_of_total
FROM gold.ticket_aging
GROUP BY 1
ORDER BY MIN(days_in_status);

-- Status Breakdown
SELECT
  status,
  COUNT(*) AS ticket_count,
  ROUND(AVG(days_in_status), 1) AS avg_age,
  ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) AS pct_of_total
FROM gold.ticket_aging
GROUP BY status
ORDER BY ticket_count DESC;

-- Aging by Assignee
SELECT
  assignee,
  COUNT(*) AS ticket_count,
  ROUND(AVG(days_in_status), 1) AS avg_age,
  MAX(days_in_status) AS max_age,
  COUNT(CASE WHEN days_in_status > 90 THEN 1 END) AS critical_count
FROM gold.ticket_aging
WHERE assignee IS NOT NULL
GROUP BY assignee
ORDER BY avg_age DESC;

-- Critical Tickets
SELECT
  t.issue_key,
  c.jira_base_url || '/browse/' || t.issue_key AS issue_url,
  t.assignee,
  t.status,
  t.days_in_status
FROM gold.ticket_aging t
LEFT JOIN gold.jira_config c
  ON SPLIT_PART(t.issue_key, '-', 1) = c.project_key
WHERE t.days_in_status > 90
ORDER BY t.days_in_status DESC;

-- Oldest Tickets
SELECT
  t.issue_key,
  c.jira_base_url || '/browse/' || t.issue_key AS issue_url,
  t.assignee,
  t.status,
  t.days_in_status,
  CASE
    WHEN t.days_in_status > 180 THEN 'Critical'
    WHEN t.days_in_status > 90 THEN 'High'
    WHEN t.days_in_status > 30 THEN 'Medium'
    ELSE 'Low'
  END AS priority
FROM gold.ticket_aging t
LEFT JOIN gold.jira_config c
  ON SPLIT_PART(t.issue_key, '-', 1) = c.project_key
ORDER BY t.days_in_status DESC
LIMIT 20;

-- ============================================
-- PERSONAL ACTIVITY (for drill-down)
-- ============================================

-- Personal Activity Summary
SELECT
  user_name,
  COUNT(DISTINCT activity_date) AS active_days,
  SUM(comments_made) AS total_comments,
  SUM(updates_made) AS total_updates,
  SUM(total_activities) AS total_activities,
  MAX(activity_date) AS last_activity_date,
  DATEDIFF('day', MAX(activity_date), CURRENT_DATE) AS days_since_last_activity
FROM gold.personal_activity
GROUP BY user_name
ORDER BY total_activities DESC;

-- Personal Activity Trend (Last 30 days)
SELECT
  activity_date,
  user_name,
  comments_made,
  updates_made,
  total_activities
FROM gold.personal_activity
WHERE activity_date >= CURRENT_DATE - INTERVAL '30 days'
ORDER BY activity_date DESC, user_name;

-- User Activity Heatmap Data
SELECT
  user_name,
  activity_date,
  total_activities,
  CASE
    WHEN total_activities = 0 THEN 'No Activity'
    WHEN total_activities < 3 THEN 'Low'
    WHEN total_activities < 10 THEN 'Medium'
    ELSE 'High'
  END AS activity_level
FROM gold.personal_activity
WHERE activity_date >= CURRENT_DATE - INTERVAL '90 days'
ORDER BY user_name, activity_date DESC;

-- ============================================
-- PERSONAL TICKET STATUS (for drill-down)
-- ============================================

-- Personal Ticket Health Summary
SELECT
  assignee,
  COUNT(*) AS total_tickets,
  COUNT(CASE WHEN health_status = 'Completed' THEN 1 END) AS completed,
  COUNT(CASE WHEN health_status = 'Active' THEN 1 END) AS active,
  COUNT(CASE WHEN health_status = 'At Risk' THEN 1 END) AS at_risk,
  COUNT(CASE WHEN health_status = 'Stale' THEN 1 END) AS stale,
  ROUND(AVG(days_since_last_update), 1) AS avg_days_since_update,
  COUNT(CASE WHEN alert_no_update_2days = 1 THEN 1 END) AS tickets_needing_update
FROM gold.personal_ticket_status
GROUP BY assignee
ORDER BY total_tickets DESC;

-- Personal Tickets by Status
SELECT
  assignee,
  status,
  COUNT(*) AS ticket_count,
  ROUND(AVG(days_since_last_update), 1) AS avg_days_since_update,
  ROUND(AVG(age_days), 1) AS avg_age_days
FROM gold.personal_ticket_status
GROUP BY assignee, status
ORDER BY assignee, ticket_count DESC;

-- Tickets Needing Attention (Personal View)
SELECT
  assignee,
  issue_key,
  summary,
  status,
  health_status,
  days_since_last_update,
  days_since_last_comment,
  alert_no_update_2days,
  alert_no_comments
FROM gold.personal_ticket_status
WHERE health_status IN ('At Risk', 'Stale')
  OR alert_no_update_2days = 1
ORDER BY assignee, days_since_last_update DESC;

