{{ config(
    materialized='incremental',
    unique_key='content_session_id',
    incremental_strategy='merge'
) }}

with base_events as (
  select
    user_id,
    session_id,
    content_id,
    event_ts,
    event_date,
    time_spent_seconds,
    scroll_count,
    completion_pct,
    completed_flag,
    live_game_flag,
    major_tournament_flag
  from {{ ref('stg_user_content_events') }}

  {% if is_incremental() %}
    -- Only process new events beyond what we've already materialized
    where event_ts > (select coalesce(max(max_event_ts), timestamptz '1900-01-01') from {{ this }})
  {% endif %}
),

agg as (
  select
    md5(user_id::text || '|' || session_id::text || '|' || content_id::text) as content_session_id,
    user_id,
    session_id,
    content_id,

    min(event_ts) as first_event_ts,
    max(event_ts) as last_event_ts,
    max(event_ts) as max_event_ts,

    -- derive a date_id for the session-content interaction (use first event date)
    to_char(min(event_ts)::date, 'YYYYMMDD')::int as date_id,

    sum(time_spent_seconds)::int as time_spent_seconds,
    sum(scroll_count)::int as scroll_count,
    max(completion_pct)::int as completion_pct_max,
    max(completed_flag)::smallint as completed_flag,

    max(live_game_flag)::smallint as live_game_flag,
    max(major_tournament_flag)::smallint as major_tournament_flag
  from base_events
  group by 1,2,3,4
)

select * from agg
