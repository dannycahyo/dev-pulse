{{
    config(
        materialized='incremental',
        unique_key='pr_key',
        incremental_strategy='merge'
    )
}}

with

pull_requests as (
    select * from {{ ref('stg_pull_requests') }}
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
    concat(pr.repo_full_name, '#', cast(pr.pr_number as string)) as pr_key,
    r.repo_id,
    pr.pr_number,
    pr.pr_title as title,
    pr.state,
    pr.is_merged,
    pr.created_at,
    d_created.date_key as created_date_key,
    pr.merged_at,
    d_merged.date_key as merged_date_key,
    pr.time_to_merge_hours,
    pr.author_login as user_login,
    pr.ingestion_timestamp

from pull_requests pr
left join dim_repo r
    on pr.repo_full_name = r.repo_full_name
left join dim_dt d_created
    on date(pr.created_at) = d_created.full_date
left join dim_dt d_merged
    on date(pr.merged_at) = d_merged.full_date
