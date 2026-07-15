{{ config(materialized='table') }}

select

    (select count(distinct account_id)
     from {{ ref('stg_accounts') }}) as distinct_accounts,

    (select count(distinct subscription_id)
     from {{ ref('stg_subscriptions') }}) as distinct_subscriptions,

    current_timestamp as calculated_at