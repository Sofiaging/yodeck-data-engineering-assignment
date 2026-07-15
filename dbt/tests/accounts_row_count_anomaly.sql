with daily_counts as (

    select
        _snapshot_date,
        count(*) as row_count
    from {{ ref('stg_accounts') }}
    group by 1

),

comparison as (

    select
        _snapshot_date,
        row_count,
        lag(row_count) over(order by _snapshot_date) as previous_row_count
    from daily_counts

)

select *

from comparison

where previous_row_count is not null

and abs(row_count - previous_row_count)
    > previous_row_count * 0.20
