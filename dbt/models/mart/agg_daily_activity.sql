{{ config(materialized='table') }}

with

daily_commits as (
    select
        date_key,
        count(*) as commit_count,
        sum(additions) as total_additions,
        sum(deletions) as total_deletions,
        sum(changed_files) as total_files_changed
    from {{ ref('fct_commits') }}
    group by date_key
),

daily_prs_opened as (
    select
        created_date_key as date_key,
        count(*) as pr_opened_count
    from {{ ref('fct_pull_requests') }}
    group by created_date_key
),

daily_prs_merged as (
    select
        merged_date_key as date_key,
        count(*) as pr_merged_count
    from {{ ref('fct_pull_requests') }}
    where is_merged = true
    group by merged_date_key
),

daily_reviews as (
    select
        date_key,
        count(*) as review_count
    from {{ ref('fct_reviews') }}
    group by date_key
),

daily_active_repos as (
    select date_key, repo_id from {{ ref('fct_commits') }}
    union distinct
    select created_date_key as date_key, repo_id from {{ ref('fct_pull_requests') }}
    union distinct
    select merged_date_key as date_key, repo_id
    from {{ ref('fct_pull_requests') }}
    where is_merged = true
    union distinct
    select date_key, repo_id from {{ ref('fct_reviews') }}
),

daily_active_repo_counts as (
    select
        date_key,
        count(distinct repo_id) as active_repo_count
    from daily_active_repos
    group by date_key
),

joined as (
    select
        d.full_date,
        d.date_key,
        d.day_of_week,
        d.day_name,
        d.iso_week_number,
        d.month_number,
        d.year,
        d.is_weekend,
        coalesce(c.commit_count, 0) as commit_count,
        coalesce(po.pr_opened_count, 0) as pr_opened_count,
        coalesce(pm.pr_merged_count, 0) as pr_merged_count,
        coalesce(rv.review_count, 0) as review_count,
        coalesce(c.total_additions, 0) as total_additions,
        coalesce(c.total_deletions, 0) as total_deletions,
        coalesce(c.total_files_changed, 0) as total_files_changed,
        coalesce(ar.active_repo_count, 0) as active_repo_count
    from {{ ref('dim_date') }} d
    left join daily_commits c on d.date_key = c.date_key
    left join daily_prs_opened po on d.date_key = po.date_key
    left join daily_prs_merged pm on d.date_key = pm.date_key
    left join daily_reviews rv on d.date_key = rv.date_key
    left join daily_active_repo_counts ar on d.date_key = ar.date_key
    where d.full_date <= current_date()
),

with_rolling as (
    select
        *,
        sum(commit_count) over (
            order by full_date
            rows between 6 preceding and current row
        ) as rolling_7d_commits,
        sum(commit_count) over (
            order by full_date
            rows between 29 preceding and current row
        ) as rolling_30d_commits,
        max(case when commit_count = 0 then full_date end) over (
            order by full_date
            rows between unbounded preceding and 1 preceding
        ) as _last_zero_date,
        min(full_date) over () as _first_date
    from joined
)

select
    full_date,
    date_key,
    day_of_week,
    day_name,
    iso_week_number,
    month_number,
    year,
    is_weekend,
    commit_count,
    pr_opened_count,
    pr_merged_count,
    review_count,
    total_additions,
    total_deletions,
    total_files_changed,
    active_repo_count,
    rolling_7d_commits,
    rolling_30d_commits,
    case
        when commit_count = 0 then 0
        when _last_zero_date is null
            then date_diff(full_date, _first_date, day) + 1
        else date_diff(full_date, _last_zero_date, day)
    end as current_streak_days
from with_rolling
