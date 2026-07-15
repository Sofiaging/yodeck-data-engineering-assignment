{{ config(
    materialized = 'view'
) }}

select

    subscription_id,

    account_id,

    plan_name,

    plan_interval,

    plan_price,

    start_date,

    end_date,

    _source_file,

    _snapshot_date,

    _ingested_at,

    _file_checksum

from {{ source('raw', 'subscriptions') }}

