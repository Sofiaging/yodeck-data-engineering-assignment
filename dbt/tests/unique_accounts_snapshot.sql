select
    account_id,
    _snapshot_date,
    count(*) as cnt
from {{ ref('stg_accounts') }}
group by
    account_id,
    _snapshot_date
having count(*) > 1
