{{ config(
    materialized = 'view'
) }}

with base as (

    select
        i.issue_id,
        i.issue_key,

        -- TI-1234 -> TI
        split_part(i.issue_key, '-', 1) as project_key,

        i.summary,
        i.status,

        -- status_category is a JSON blob from Jira
        json_extract_string(i.status_category, '$.key')  as status_category_key,
        json_extract_string(i.status_category, '$.name') as status_category_name,
        json_extract_string(i.status_category, '$.colorName') as status_category_colour,

        i.assignee,
        i.reporter,
        i.issue_type,
        i.priority,

        -- raw labels and sprint for later drill downs
        i.labels_raw,
        i.sprint_raw,

        -- often an epic key or similar in Jira, keep it explicit
        i.fields__customfield_10014 as epic_key,

        i.created_at,
        i.updated_at,
        i.completed_at

    from silver.issues as i
),

typed as (

    select
        *,
        cast(created_at   as date) as created_date,
        cast(completed_at as date) as completed_date
    from base
),

metrics as (

    select
        *,
        case
            when completed_at is not null then 1
            else 0
        end as is_completed,

        case
            when status_category_key = 'done' then 1
            else 0
        end as is_in_done_category,

        case
            when status_category_key = 'indeterminate' then 1
            else 0
        end as is_in_progress_category,

        -- cycle time in days from creation to completion
        case
            when completed_at is not null then
                datediff('day', created_at, completed_at)
            else null
        end as cycle_time_days

    from typed
)

select
    issue_id,
    issue_key,
    project_key,

    summary,
    status,
    status_category_key,
    status_category_name,
    status_category_colour,

    assignee,
    reporter,
    issue_type,
    priority,

    labels_raw,
    sprint_raw,
    epic_key,

    created_at,
    updated_at,
    completed_at,
    created_date,
    completed_date,

    is_completed,
    is_in_done_category,
    is_in_progress_category,
    cycle_time_days

from metrics