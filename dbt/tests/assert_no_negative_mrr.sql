select *
from {{ ref('fct_subscriptions') }}
where subscription_status = 'active'
  and mrr_amount < 0