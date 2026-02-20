{{ config(materialized='view') }}

with

source as (
    select * from {{ source('raw', 'raw_repositories') }}
),

deduplicated as (
    select
        *,
        row_number() over (
            partition by id
            order by ingestion_timestamp desc
        ) as _row_num
    from source
),

renamed as (
    select
        cast(id as int64) as repository_id,
        name as repository_name,
        full_name as full_name,
        split(full_name, '/')[safe_offset(0)] as owner,
        owner_login,
        language as primary_language,
        safe_cast(created_at as timestamp) as created_at,
        safe_cast(updated_at as timestamp) as updated_at,
        visibility,
        fork as is_fork,
        cast(stargazers_count as int64) as stargazers_count,
        ingestion_timestamp
    from deduplicated
    where _row_num = 1
)

select * from renamed
