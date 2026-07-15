{{ config(
    materialized = 'table'
) }}

with snapshots as (

    select *

    from {{ ref('stg_subscriptions') }}

),

changes as (

    select

        subscription_id,

        account_id,

        _snapshot_date,

        plan_name,

        plan_price,

        end_date,

        lag(plan_name) over (

            partition by subscription_id
            order by _snapshot_date

        ) as previous_plan_name,

        lag(plan_price) over (

            partition by subscription_id
            order by _snapshot_date

        ) as previous_plan_price,

        lag(end_date) over (

            partition by subscription_id
            order by _snapshot_date

        ) as previous_end_date

    from snapshots

)

select *

from changes

where

    previous_plan_name is distinct from plan_name

    or previous_plan_price is distinct from plan_price

    or previous_end_date is distinct from end_date
