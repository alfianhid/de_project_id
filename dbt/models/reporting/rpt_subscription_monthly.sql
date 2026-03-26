{{
    config(
        materialized='table',
        partition_by={"field": "snapshot_month", "data_type": "date", "granularity": "month"},
        cluster_by=['account_id', 'subscription_status'],
        tags=['reporting', 'billing', 'monthly']
    )
}}

with subscriptions as (
    select * from {{ ref('fct_subscriptions') }}
),

accounts as (
    select
        account_id,
        account_name,
        industry,
        company_size_band,
        region,
        account_tier
    from {{ ref('snap_accounts') }}
    where dbt_valid_to is null
),

monthly_snapshot as (
    select
        date_trunc(cast(started_at as date), month) as snapshot_month,
        s.subscription_id,
        s.account_id,
        a.account_name,
        a.industry,
        a.company_size_band,
        a.region,
        a.account_tier,
        s.plan_id,
        s.subscription_status,
        s.billing_interval,
        s.currency,
        s.mrr_amount,
        s.arr_amount,
        s.started_at,
        s.cancelled_at,
        s.ended_at,
        case
            when s.subscription_status = 'active' then 'Active'
            when s.subscription_status = 'cancelled' then 'Cancelled'
            when s.subscription_status = 'trialing' then 'Trialing'
            when s.subscription_status in ('past_due', 'incomplete', 'unpaid') then 'At Risk'
            else 'Other'
        end as health_status,
        case
            when s.subscription_status = 'active' and s.mrr_amount >= 500 then 'High Value'
            when s.subscription_status = 'active' and s.mrr_amount >= 100 then 'Medium Value'
            when s.subscription_status = 'active' then 'Low Value'
            else 'Non-Revenue'
        end as value_tier,
        case
            when s.billing_interval = 'year' then 'Annual'
            when s.billing_interval = 'month' then 'Monthly'
            else 'Other'
        end as billing_type,
        s._loaded_at
    from subscriptions s
    left join accounts a on s.account_id = a.account_id
)

select * from monthly_snapshot