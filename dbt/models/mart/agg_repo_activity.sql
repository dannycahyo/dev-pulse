{{ config(materialized='table') }}

with

monthly_commits as (
    select
        d.year,
        d.month_number,
        c.repo_id,
        count(*) as commit_count,
        sum(c.additions) as additions,
        sum(c.deletions) as deletions,
        count(distinct c.author_email) as unique_contributors
    from {{ ref('fct_commits') }} c
    join {{ ref('dim_date') }} d on c.date_key = d.date_key
    group by d.year, d.month_number, c.repo_id
),

monthly_prs as (
    select
        d.year,
        d.month_number,
        p.repo_id,
        count(*) as pr_count
    from {{ ref('fct_pull_requests') }} p
    join {{ ref('dim_date') }} d on p.created_date_key = d.date_key
    group by d.year, d.month_number, p.repo_id
),

all_repo_months as (
    select repo_id, year, month_number from monthly_commits
    union distinct
    select repo_id, year, month_number from monthly_prs
),

joined as (
    select
        arm.repo_id,
        arm.year,
        arm.month_number,
        coalesce(mc.commit_count, 0) as commit_count,
        coalesce(mp.pr_count, 0) as pr_count,
        coalesce(mc.additions, 0) as additions,
        coalesce(mc.deletions, 0) as deletions,
        coalesce(mc.unique_contributors, 0) as unique_contributors
    from all_repo_months arm
    left join monthly_commits mc
        on arm.repo_id = mc.repo_id
        and arm.year = mc.year
        and arm.month_number = mc.month_number
    left join monthly_prs mp
        on arm.repo_id = mp.repo_id
        and arm.year = mp.year
        and arm.month_number = mp.month_number
)

select
    r.repo_full_name,
    r.repo_name,
    r.primary_language,
    r.stargazers_count,
    r.visibility,
    r.is_fork,
    j.year,
    j.month_number,
    j.commit_count,
    j.pr_count,
    j.additions,
    j.deletions,
    j.unique_contributors
from joined j
join {{ ref('dim_repository') }} r on j.repo_id = r.repo_id
