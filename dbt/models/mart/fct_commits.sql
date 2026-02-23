{{
    config(
        materialized='incremental',
        unique_key='commit_sha',
        incremental_strategy='merge'
    )
}}

with

commits as (
    select * from {{ ref('stg_commits') }}
    {% if is_incremental() %}
    where ingestion_timestamp > (select max(ingestion_timestamp) from {{ this }})
    {% endif %}
),

dim_repo as (
    select * from {{ ref('dim_repository') }}
),

dim_dt as (
    select * from {{ ref('dim_date') }}
)

select
    c.commit_sha,
    r.repo_id,
    d.date_key,
    c.authored_at as committed_at,
    c.hour_of_day,
    c.day_of_week,
    c.author_name,
    c.author_email,
    left(c.commit_message, 100) as message_first_line,
    c.lines_added as additions,
    c.lines_deleted as deletions,
    c.files_changed as changed_files,
    coalesce(c.lines_added, 0) + coalesce(c.lines_deleted, 0) as total_changes,
    c.ingestion_timestamp

from commits c
left join dim_repo r
    on c.repo_full_name = r.repo_full_name
left join dim_dt d
    on date(c.authored_at) = d.full_date
