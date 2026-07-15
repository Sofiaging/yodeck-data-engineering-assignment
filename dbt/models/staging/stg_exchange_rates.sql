{{ config(
    materialized = 'view'
) }}

select

    rate_date,

    currency,

    rate_to_usd,

    _source_file,

    _snapshot_date,

    _ingested_at,

    _file_checksum

from {{ source('raw', 'exchange_rates') }}
