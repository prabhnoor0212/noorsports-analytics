with base as (
  select
    u.user_id,
    u.signup_ts,
    u.country,
    u.marketing_channel,
    u.fav_sport,
    -- SCD2 scaffold: one current row per user in this dataset
    u.signup_ts as row_active_start_ts,
    timestamptz '9999-12-31' as row_active_end_ts,
    true as is_current
  from {{ ref('stg_users') }} u
),
final as (
  select
    -- deterministic surrogate key: stable per (user_id, start_ts)
    md5(user_id::text || '|' || row_active_start_ts::text) as user_sk,
    user_id,
    signup_ts,
    country,
    marketing_channel,
    fav_sport,
    row_active_start_ts,
    row_active_end_ts,
    is_current
  from base
)
select * from final
