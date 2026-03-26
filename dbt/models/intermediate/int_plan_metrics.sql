{{ config(
    materialized='ephemeral',
    tags=['intermediate', 'billing', 'planning']
) }}

with subscriptions as (
    select * from {{ ref('fct_subscriptions') }}
),

plan_metrics as (
    select
        plan_id,
        count(*) as total_subscriptions,
        countif(subscription_status = 'active') as active_subscriptions,
        countif(subscription_status = 'cancelled') as cancelled_subscriptions,
        countif(subscription_status = 'trialing') as trialing_subscriptions,
        countif(subscription_status in ('past_due', 'incomplete', 'unpaid')) as at_risk_subscriptions,
        sum(case when subscription_status = 'active' then mrr_amount else 0 end) as plan_active_mrr,
        sum(case when subscription_status = 'active' then arr_amount else 0 end) as plan_active_arr,
        avg(case when subscription_status = 'active' then mrr_amount end) as avg_active_mrr,
        count(distinct account_id) as unique_accounts,
        min(started_at) as first_subscription_at,
        max(started_at) as latest_subscription_at
    from subscriptions
    group by plan_id
)

select * from plan_metrics