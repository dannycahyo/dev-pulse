{{ config(materialized='table') }}

with

weekly_commits as (
    select
        d.year,
        d.iso_week_number,
        count(*) as commit_count,
        sum(c.additions) as total_additions,
        sum(c.deletions) as total_deletions,
        sum(c.changed_files) as total_files_changed
    from {{ ref('fct_commits') }} c
    join {{ ref('dim_date') }} d on c.date_key = d.date_key
    group by d.year, d.iso_week_number
),

weekly_prs_opened as (
    select
        d.year,
        d.iso_week_number,
        count(*) as pr_opened_count
    from {{ ref('fct_pull_requests') }} p
    join {{ ref('dim_date') }} d on p.created_date_key = d.date_key
    group by d.year, d.iso_week_number
),

weekly_prs_merged as (
    select
        d.year,
        d.iso_week_number,
        count(*) as pr_merged_count
    from {{ ref('fct_pull_requests') }} p
    join {{ ref('dim_date') }} d on p.merged_date_key = d.date_key
    where p.is_merged = true
    group by d.year, d.iso_week_number
),

weekly_reviews as (
    select
        d.year,
        d.iso_week_number,
        count(*) as review_count
    from {{ ref('fct_reviews') }} r
    join {{ ref('dim_date') }} d on r.date_key = d.date_key
    group by d.year, d.iso_week_number
),

weekly_active_repos as (
    select d.year, d.iso_week_number, c.repo_id
    from {{ ref('fct_commits') }} c
    join {{ ref('dim_date') }} d on c.date_key = d.date_key
    union distinct
    select d.year, d.iso_week_number, p.repo_id
    from {{ ref('fct_pull_requests') }} p
    join {{ ref('dim_date') }} d on p.created_date_key = d.date_key
    union distinct
    select d.year, d.iso_week_number, p.repo_id
    from {{ ref('fct_pull_requests') }} p
    join {{ ref('dim_date') }} d on p.merged_date_key = d.date_key
    where p.is_merged = true
    union distinct
    select d.year, d.iso_week_number, r.repo_id
    from {{ ref('fct_reviews') }} r
    join {{ ref('dim_date') }} d on r.date_key = d.date_key
),

weekly_active_repo_counts as (
    select
        year,
        iso_week_number,
        count(distinct repo_id) as active_repo_count
    from weekly_active_repos
    group by year, iso_week_number
),

all_weeks as (
    select
        year,
        iso_week_number,
        min(full_date) as week_start_date
    from {{ ref('dim_date') }}
    where full_date <= current_date()
    group by year, iso_week_number
),

joined as (
    select
        w.year,
        w.iso_week_number,
        w.week_start_date,
        coalesce(c.commit_count, 0) as commit_count,
        coalesce(po.pr_opened_count, 0) as pr_opened_count,
        coalesce(pm.pr_merged_count, 0) as pr_merged_count,
        coalesce(rv.review_count, 0) as review_count,
        coalesce(c.total_additions, 0) as total_additions,
        coalesce(c.total_deletions, 0) as total_deletions,
        coalesce(c.total_files_changed, 0) as total_files_changed,
        coalesce(ar.active_repo_count, 0) as active_repo_count
    from all_weeks w
    left join weekly_commits c
        on w.year = c.year and w.iso_week_number = c.iso_week_number
    left join weekly_prs_opened po
        on w.year = po.year and w.iso_week_number = po.iso_week_number
    left join weekly_prs_merged pm
        on w.year = pm.year and w.iso_week_number = pm.iso_week_number
    left join weekly_reviews rv
        on w.year = rv.year and w.iso_week_number = rv.iso_week_number
    left join weekly_active_repo_counts ar
        on w.year = ar.year and w.iso_week_number = ar.iso_week_number
)

select
    year,
    iso_week_number,
    week_start_date,
    commit_count,
    pr_opened_count,
    pr_merged_count,
    review_count,
    total_additions,
    total_deletions,
    total_files_changed,
    active_repo_count,
    round(
        safe_divide(
            commit_count - lag(commit_count) over (order by year, iso_week_number),
            lag(commit_count) over (order by year, iso_week_number)
        ) * 100,
        2
    ) as week_over_week_commit_change
from joined
