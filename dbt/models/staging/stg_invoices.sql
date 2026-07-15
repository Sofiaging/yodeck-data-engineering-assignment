{{ config(
    materialized = 'view'
) }}

select

    invoice_id,

    account_id,

    subscription_id,

    amount,

    currency,

    status,

    invoice_date,

    paid_at,

    _source_file,

    _snapshot_date,

    _ingested_at,

    _file_checksum

from {{ source('raw', 'invoices') }}
