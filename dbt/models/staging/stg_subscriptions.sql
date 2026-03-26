{{ config(materialized='view', tags=['staging', 'billing']) }}

with source as (
    select * from {{ source('raw', 'raw_subscriptions') }}
),

deduplicated as (
    select *,
        row_number() over (
            partition by id
            order by _loaded_at desc
        ) as _row_num
    from source
),

cleaned as (
    select
        id                                              as subscription_id,
        customer                                        as account_id,
        plan_id,
        lower(trim(status))                             as subscription_status,
        lower(trim(billing_interval))                  as billing_interval,
        upper(trim(currency))                           as currency,

        safe_cast(amount as numeric) / 100.0            as mrr_amount,

        timestamp_seconds(safe_cast(created as int64))           as started_at,
        timestamp_seconds(nullif(safe_cast(canceled_at as int64), 0)) as cancelled_at,
        timestamp_seconds(nullif(safe_cast(ended_at as int64), 0))    as ended_at,
        timestamp_seconds(nullif(safe_cast(trial_start as int64), 0)) as trial_start_at,
        timestamp_seconds(nullif(safe_cast(trial_end as int64), 0))   as trial_end_at,

        _source_system,
        _loaded_at,
        _pipeline_run_date

    from deduplicated
    where _row_num = 1
      and id is not null
      and customer is not null
)

select * from cleaned