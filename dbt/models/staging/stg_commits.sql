{{ config(materialized='view') }}

with

source as (
    select * from {{ source('raw', 'raw_commits') }}
),

deduplicated as (
    select
        *,
        row_number() over (
            partition by sha
            order by ingestion_timestamp desc
        ) as _row_num
    from source
),

renamed as (
    select
        sha as commit_sha,
        repo_full_name,
        author_name,
        author_email,
        safe_cast(author_date as timestamp) as authored_at,
        message as commit_message,
        cast(additions as int64) as lines_added,
        cast(deletions as int64) as lines_deleted,
        cast(changed_files as int64) as files_changed,
        extract(hour from safe_cast(author_date as timestamp)) as hour_of_day,
        extract(dayofweek from safe_cast(author_date as timestamp)) as day_of_week,
        extract(isoweek from safe_cast(author_date as timestamp)) as iso_week,
        extract(month from safe_cast(author_date as timestamp)) as month,
        extract(year from safe_cast(author_date as timestamp)) as year,
        ingestion_timestamp
    from deduplicated
    where _row_num = 1
)

select * from renamed
