{{ config(materialized='view') }}

with

source as (
    select * from {{ source('raw', 'raw_languages') }}
),

deduplicated as (
    select
        *,
        row_number() over (
            partition by repo_full_name, language_name
            order by ingestion_timestamp desc
        ) as _row_num
    from source
),

with_percentage as (
    select
        repo_full_name,
        language_name,
        cast(byte_count as int64) as byte_count,
        safe_divide(
            cast(byte_count as int64),
            sum(cast(byte_count as int64)) over (partition by repo_full_name)
        ) as language_percentage,
        ingestion_timestamp
    from deduplicated
    where _row_num = 1
)

select * from with_percentage
