{{
    config(
        materialized='incremental',
        unique_key='review_id',
        incremental_strategy='merge'
    )
}}

with

reviews as (
    select * from {{ ref('stg_reviews') }}
    {% if is_incremental() %}
    where ingestion_timestamp > (select max(ingestion_timestamp) from {{ this }})
    {% endif %}
),

dim_repo as (
    select * from {{ ref('dim_repository') }}
),

fct_prs as (
    select * from {{ ref('fct_pull_requests') }}
),

dim_dt as (
    select * from {{ ref('dim_date') }}
)

select
    rv.review_id,
    r.repo_id,
    pr.pr_key,
    rv.reviewer_login,
    rv.review_state,
    rv.submitted_at,
    d.date_key,
    rv.ingestion_timestamp

from reviews rv
left join dim_repo r
    on rv.repo_full_name = r.repo_full_name
left join fct_prs pr
    on concat(rv.repo_full_name, '#', cast(rv.pr_number as string)) = pr.pr_key
left join dim_dt d
    on date(rv.submitted_at) = d.full_date
