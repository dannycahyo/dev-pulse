{{ config(materialized='table') }}

with

date_spine as (
    select
        date_day
    from
        unnest(
            generate_date_array(
                date '2020-01-01',
                date_add(current_date(), interval 365 day),
                interval 1 day
            )
        ) as date_day
)

select
    cast(format_date('%Y%m%d', date_day) as int64) as date_key,
    date_day as full_date,
    extract(dayofweek from date_day) as day_of_week,
    format_date('%A', date_day) as day_name,
    extract(day from date_day) as day_of_month,
    extract(isoweek from date_day) as iso_week_number,
    extract(month from date_day) as month_number,
    format_date('%B', date_day) as month_name,
    extract(quarter from date_day) as quarter,
    extract(year from date_day) as year,
    case
        when extract(dayofweek from date_day) in (1, 7) then true
        else false
    end as is_weekend

from date_spine
