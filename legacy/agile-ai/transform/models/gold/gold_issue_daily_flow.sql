{{ config(
    materialized = 'table'
) }}

with base as (

    -- this is essentially what you have now
    select
        project_key,
        date,
        coalesce(issues_created,   0) as issues_created,
        coalesce(issues_completed, 0) as issues_completed,
        avg_cycle_time_days,
        median_cycle_time_days
    from {{ ref('gold_issue_daily_flow_base') }}  -- or directly inline your current CTE
),

with_net as (

    select
        project_key,
        date,
        issues_created,
        issues_completed,
        issues_created - issues_completed as net_flow,
        avg_cycle_time_days,
        median_cycle_time_days
    from base
),

with_cumulative as (

    select
        project_key,
        date,
        issues_created,
        issues_completed,
        net_flow,

        sum(issues_created)   over (
            partition by project_key
            order by date
            rows between unbounded preceding and current row
        ) as cum_issues_created,

        sum(issues_completed) over (
            partition by project_key
            order by date
            rows between unbounded preceding and current row
        ) as cum_issues_completed
    from with_net
)

select
    project_key,
    date,
    issues_created,
    issues_completed,
    net_flow,

    cum_issues_created,
    cum_issues_completed,

    cum_issues_created - cum_issues_completed as backlog_estimate
from with_cumulative