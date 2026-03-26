{{
    config(
        materialized='table',
        tags=['reporting', 'account', 'health']
    )
}}

with accounts as (
    select
        account_id,
        account_name,
        industry,
        company_size_band,
        region,
        account_tier,
        csm_user_id,
        is_active,
        created_at,
        updated_at
    from {{ ref('snap_accounts') }}
    where dbt_valid_to is null
),

subscription_metrics as (
    select * from {{ ref('int_account_metrics') }}
),

account_overview as (
    select
        a.account_id,
        a.account_name,
        a.industry,
        a.company_size_band,
        a.region,
        a.account_tier,
        a.csm_user_id,
        a.is_active as account_active,
        a.created_at as account_created_at,
        a.updated_at as account_updated_at,
        coalesce(m.total_subscriptions, 0) as total_subscriptions,
        coalesce(m.active_subscriptions, 0) as active_subscriptions,
        coalesce(m.cancelled_subscriptions, 0) as cancelled_subscriptions,
        coalesce(m.trialing_subscriptions, 0) as trialing_subscriptions,
        coalesce(m.total_active_mrr, 0) as total_mrr,
        coalesce(m.total_active_arr, 0) as total_arr,
        m.first_subscription_at,
        m.latest_subscription_at,
        coalesce(m.unique_plans, 0) as unique_plans_subscribed,
        coalesce(m.unique_currencies, 1) as currencies_used,
        case
            when a.is_active = false then 'Inactive'
            when coalesce(m.active_subscriptions, 0) = 0 and coalesce(m.cancelled_subscriptions, 0) > 0 then 'Churned'
            when coalesce(m.active_subscriptions, 0) = 0 then 'No Subscription'
            when coalesce(m.total_active_mrr, 0) >= 1000 then 'Enterprise'
            when coalesce(m.total_active_mrr, 0) >= 500 then 'Growth'
            when coalesce(m.total_active_mrr, 0) > 0 then 'Starter'
            else 'Free'
        end as account_health_segment,
        case
            when coalesce(m.active_subscriptions, 0) >= 3 then 'Multi-Product'
            when coalesce(m.active_subscriptions, 0) >= 2 then 'Dual-Product'
            when coalesce(m.active_subscriptions, 0) = 1 then 'Single-Product'
            else 'No Active Products'
        end as product_adoption,
        timestamp_diff(current_timestamp(), cast(a.created_at as timestamp), day) as account_age_days,
        case
            when coalesce(m.total_active_mrr, 0) = 0 then null
            when timestamp_diff(current_timestamp(), cast(m.first_subscription_at as timestamp), day) <= 30 then 'New'
            when timestamp_diff(current_timestamp(), cast(m.latest_subscription_at as timestamp), day) <= 90 then 'Active'
            when timestamp_diff(current_timestamp(), cast(m.latest_subscription_at as timestamp), day) <= 180 then 'Engaged'
            else 'Dormant'
        end as engagement_status
    from accounts a
    left join subscription_metrics m on a.account_id = m.account_id
)

select * from account_overview