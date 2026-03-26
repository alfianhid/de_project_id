{{
    config(
        materialized='incremental',
        unique_key='subscription_id',
        incremental_strategy='merge',
        partition_by={"field": "started_at", "data_type": "timestamp", "granularity": "month"},
        cluster_by=['account_id', 'subscription_status'],
        tags=['transform', 'billing', 'core']
    )
}}

with staged as (
    select * from {{ ref('stg_subscriptions') }}
    {% if is_incremental() %}
        where _loaded_at > (select max(_loaded_at) from {{ this }})
    {% endif %}
),

accounts as (
    select account_id, dbt_scd_id as account_sk
    from {{ ref('snap_accounts') }}
    where dbt_valid_to is null
),

enriched as (
    select
        {{ dbt_utils.generate_surrogate_key(['s.subscription_id']) }} as subscription_sk,
        s.subscription_id,
        s.account_id,
        a.account_sk,
        s.plan_id,
        s.subscription_status,
        s.billing_interval,
        s.currency,
        s.mrr_amount,
        s.mrr_amount * 12   as arr_amount,
        s.started_at,
        s.cancelled_at,
        s.ended_at,
        s.trial_start_at,
        s.trial_end_at,
        s._source_system,
        s._loaded_at
    from staged s
    left join accounts a on s.account_id = a.account_id
)

select * from enriched