{{ config(materialized='table') }}

with

repositories as (
    select * from {{ ref('stg_repositories') }}
)

select
    cast(farm_fingerprint(full_name) as int64) as repo_id,
    repository_name as repo_name,
    full_name as repo_full_name,
    owner,
    primary_language,
    created_at,
    visibility,
    is_fork,
    stargazers_count

from repositories
