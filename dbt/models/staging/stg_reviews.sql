{{ config(materialized='view') }}

with

source as (
    select * from {{ source('raw', 'raw_reviews') }}
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
        cast(id as int64) as review_id,
        repo_full_name,
        cast(pr_number as int64) as pr_number,
        concat(repo_full_name, '-', cast(pr_number as string)) as pr_id,
        user_login as reviewer_login,
        case upper(state)
            when 'APPROVED' then 'APPROVED'
            when 'CHANGES_REQUESTED' then 'CHANGES_REQUESTED'
            when 'COMMENTED' then 'COMMENTED'
            when 'DISMISSED' then 'DISMISSED'
            else upper(state)
        end as review_state,
        safe_cast(submitted_at as timestamp) as submitted_at,
        body as review_body,
        ingestion_timestamp
    from deduplicated
    where _row_num = 1
)

select * from renamed
