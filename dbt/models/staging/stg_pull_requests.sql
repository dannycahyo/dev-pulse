{{ config(materialized='view') }}

with

source as (
    select * from {{ source('raw', 'raw_pull_requests') }}
),

deduplicated as (
    select
        *,
        row_number() over (
            partition by repo_full_name, number
            order by ingestion_timestamp desc
        ) as _row_num
    from source
),

renamed as (
    select
        concat(repo_full_name, '-', cast(number as string)) as pr_id,
        cast(number as int64) as pr_number,
        repo_full_name,
        title as pr_title,
        case
            when merged_at is not null then 'merged'
            when lower(state) = 'closed' then 'closed'
            else 'open'
        end as state,
        user_login as author_login,
        safe_cast(created_at as timestamp) as created_at,
        safe_cast(updated_at as timestamp) as updated_at,
        safe_cast(merged_at as timestamp) as merged_at,
        merge_commit_sha,
        case
            when merged_at is not null then true
            else false
        end as is_merged,
        case
            when merged_at is not null
                then timestamp_diff(
                    safe_cast(merged_at as timestamp),
                    safe_cast(created_at as timestamp),
                    hour
                )
            else null
        end as time_to_merge_hours,
        ingestion_timestamp
    from deduplicated
    where _row_num = 1
)

select * from renamed
