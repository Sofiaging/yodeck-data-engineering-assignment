select
    invoice_id,
    _snapshot_date,
    count(*) as cnt
from {{ ref('stg_invoices') }}
group by
    invoice_id,
    _snapshot_date
having count(*) > 1
