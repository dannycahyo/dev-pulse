{{ config(materialized='table') }}

with

repo_language as (
    select
        repo_id,
        repo_full_name,
        primary_language
    from {{ ref('dim_repository') }}
    where primary_language is not null
),

monthly_repo_commits as (
    select
        d.year,
        d.month_number,
        c.repo_id,
        count(*) as commit_count
    from {{ ref('fct_commits') }} c
    join {{ ref('dim_date') }} d on c.date_key = d.date_key
    group by d.year, d.month_number, c.repo_id
),

language_bytes as (
    select
        r.primary_language as language,
        l.language_name,
        sum(l.byte_count) as total_bytes
    from {{ ref('stg_languages') }} l
    join {{ ref('dim_repository') }} r on l.repo_full_name = r.repo_full_name
    group by r.primary_language, l.language_name
),

primary_language_bytes as (
    select
        language,
        sum(total_bytes) as total_bytes
    from language_bytes
    where language = language_name
    group by language
),

monthly_language_activity as (
    select
        mc.year,
        mc.month_number,
        rl.primary_language as language,
        sum(mc.commit_count) as commit_count,
        count(distinct mc.repo_id) as repo_count
    from monthly_repo_commits mc
    join repo_language rl on mc.repo_id = rl.repo_id
    group by mc.year, mc.month_number, rl.primary_language
),

with_bytes as (
    select
        mla.year,
        mla.month_number,
        mla.language,
        coalesce(lb.total_bytes, 0) as total_bytes,
        mla.repo_count,
        mla.commit_count
    from monthly_language_activity mla
    left join primary_language_bytes lb on mla.language = lb.language
)

select
    year,
    month_number,
    language,
    total_bytes,
    repo_count,
    commit_count,
    round(
        safe_divide(
            total_bytes,
            sum(total_bytes) over (partition by year, month_number)
        ) * 100,
        2
    ) as percentage_of_total_bytes
from with_bytes
