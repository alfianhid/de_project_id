{{ config(
    materialized='ephemeral',
    tags=['intermediate', 'billing', 'metrics']
) }}

with subscriptions as (
    select * from {{ ref('fct_subscriptions') }}
),

account_metrics as (
    select
        account_id,
        account_sk,
        count(*) as total_subscriptions,
        countif(subscription_status = 'active') as active_subscriptions,
        countif(subscription_status = 'cancelled') as cancelled_subscriptions,
        countif(subscription_status = 'trialing') as trialing_subscriptions,
        sum(case when subscription_status = 'active' then mrr_amount else 0 end) as total_active_mrr,
        sum(case when subscription_status = 'active' then arr_amount else 0 end) as total_active_arr,
        min(started_at) as first_subscription_at,
        max(started_at) as latest_subscription_at,
        count(distinct plan_id) as unique_plans,
        count(distinct currency) as unique_currencies
    from subscriptions
    group by account_id, account_sk
)

select * from account_metrics