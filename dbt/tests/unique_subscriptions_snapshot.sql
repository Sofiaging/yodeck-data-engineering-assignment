select
    subscription_id,
    _snapshot_date,
    count(*) as cnt
from {{ ref('stg_subscriptions') }}
group by
    subscription_id,
    _snapshot_date
having count(*) > 1
