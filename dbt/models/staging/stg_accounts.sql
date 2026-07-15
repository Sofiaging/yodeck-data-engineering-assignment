{{ config(
    materialized = 'view'
) }}

select

    account_id,

    account_name,

    country,

    company_size,

    industry,

    created_at,

    status,

    _source_file,

    _snapshot_date,

    _ingested_at,

    _file_checksum

from {{ source('raw', 'accounts') }}
