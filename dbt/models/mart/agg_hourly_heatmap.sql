{{ config(materialized='table') }}

with

daily_hourly_commits as (
    select
        date(committed_at) as commit_date,
        day_of_week,
        hour_of_day,
        count(*) as commit_count
    from {{ ref('fct_commits') }}
    group by date(committed_at), day_of_week, hour_of_day
),

heatmap_metrics as (
    select
        day_of_week,
        hour_of_day,
        sum(commit_count) as total_commits,
        max(commit_count) as max_single_day_commits
    from daily_hourly_commits
    group by day_of_week, hour_of_day
),

date_range as (
    select
        date_diff(
            max(date(committed_at)),
            min(date(committed_at)),
            week
        ) + 1 as total_weeks
    from {{ ref('fct_commits') }}
),

all_combinations as (
    select
        d.day as day_of_week,
        h.hour as hour_of_day
    from unnest(generate_array(1, 7)) as d(day)
    cross join unnest(generate_array(0, 23)) as h(hour)
),

day_labels as (
    select 1 as day_of_week, 'Sunday' as day_name union all
    select 2, 'Monday' union all
    select 3, 'Tuesday' union all
    select 4, 'Wednesday' union all
    select 5, 'Thursday' union all
    select 6, 'Friday' union all
    select 7, 'Saturday'
)

select
    a.day_of_week,
    dl.day_name,
    a.hour_of_day,
    coalesce(h.total_commits, 0) as total_commits,
    round(
        safe_divide(coalesce(h.total_commits, 0), dr.total_weeks),
        2
    ) as avg_commits_per_week,
    coalesce(h.max_single_day_commits, 0) as max_single_day_commits
from all_combinations a
left join heatmap_metrics h
    on a.day_of_week = h.day_of_week and a.hour_of_day = h.hour_of_day
cross join date_range dr
left join day_labels dl on a.day_of_week = dl.day_of_week
order by a.day_of_week, a.hour_of_day
