{% snapshot snap_accounts %}

{{ config(
    target_schema='transform',
    unique_key='account_id',
    strategy='timestamp',
    updated_at='updated_at',
    invalidate_hard_deletes=true,
) }}

select
    account_id, account_name, industry,
    company_size_band, region, account_tier,
    csm_user_id, is_active, created_at, updated_at,
    _source_system, _loaded_at
from {{ source('raw', 'raw_accounts') }}

{% endsnapshot %}