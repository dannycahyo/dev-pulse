{{ config(materialized='table') }}

with

monthly_commits as (
    select
        d.year,
        d.month_number,
        count(*) as commit_count,
        sum(c.additions) as total_additions,
        sum(c.deletions) as total_deletions,
        sum(c.changed_files) as total_files_changed
    from {{ ref('fct_commits') }} c
    join {{ ref('dim_date') }} d on c.date_key = d.date_key
    group by d.year, d.month_number
),

monthly_prs_opened as (
    select
        d.year,
        d.month_number,
        count(*) as pr_opened_count
    from {{ ref('fct_pull_requests') }} p
    join {{ ref('dim_date') }} d on p.created_date_key = d.date_key
    group by d.year, d.month_number
),

monthly_prs_merged as (
    select
        d.year,
        d.month_number,
        count(*) as pr_merged_count
    from {{ ref('fct_pull_requests') }} p
    join {{ ref('dim_date') }} d on p.merged_date_key = d.date_key
    where p.is_merged = true
    group by d.year, d.month_number
),

monthly_reviews as (
    select
        d.year,
        d.month_number,
        count(*) as review_count
    from {{ ref('fct_reviews') }} r
    join {{ ref('dim_date') }} d on r.date_key = d.date_key
    group by d.year, d.month_number
),

monthly_active_repos as (
    select d.year, d.month_number, c.repo_id
    from {{ ref('fct_commits') }} c
    join {{ ref('dim_date') }} d on c.date_key = d.date_key
    union distinct
    select d.year, d.month_number, p.repo_id
    from {{ ref('fct_pull_requests') }} p
    join {{ ref('dim_date') }} d on p.created_date_key = d.date_key
    union distinct
    select d.year, d.month_number, p.repo_id
    from {{ ref('fct_pull_requests') }} p
    join {{ ref('dim_date') }} d on p.merged_date_key = d.date_key
    where p.is_merged = true
    union distinct
    select d.year, d.month_number, r.repo_id
    from {{ ref('fct_reviews') }} r
    join {{ ref('dim_date') }} d on r.date_key = d.date_key
),

monthly_active_repo_counts as (
    select
        year,
        month_number,
        count(distinct repo_id) as active_repo_count
    from monthly_active_repos
    group by year, month_number
),

all_months as (
    select
        year,
        month_number,
        month_name,
        count(*) as days_in_month
    from {{ ref('dim_date') }}
    where full_date <= current_date()
    group by year, month_number, month_name
),

joined as (
    select
        m.year,
        m.month_number,
        m.month_name,
        m.days_in_month,
        coalesce(c.commit_count, 0) as commit_count,
        coalesce(po.pr_opened_count, 0) as pr_opened_count,
        coalesce(pm.pr_merged_count, 0) as pr_merged_count,
        coalesce(rv.review_count, 0) as review_count,
        coalesce(c.total_additions, 0) as total_additions,
        coalesce(c.total_deletions, 0) as total_deletions,
        coalesce(c.total_files_changed, 0) as total_files_changed,
        coalesce(ar.active_repo_count, 0) as active_repo_count
    from all_months m
    left join monthly_commits c
        on m.year = c.year and m.month_number = c.month_number
    left join monthly_prs_opened po
        on m.year = po.year and m.month_number = po.month_number
    left join monthly_prs_merged pm
        on m.year = pm.year and m.month_number = pm.month_number
    left join monthly_reviews rv
        on m.year = rv.year and m.month_number = rv.month_number
    left join monthly_active_repo_counts ar
        on m.year = ar.year and m.month_number = ar.month_number
)

select
    year,
    month_number,
    month_name,
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
            commit_count - lag(commit_count) over (order by year, month_number),
            lag(commit_count) over (order by year, month_number)
        ) * 100,
        2
    ) as month_over_month_commit_change,
    round(safe_divide(commit_count, days_in_month), 2) as avg_daily_commits
from joined
