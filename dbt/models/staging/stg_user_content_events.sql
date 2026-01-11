select
  event_id::bigint as event_id,
  user_id::bigint as user_id,
  nullif(session_id, '') as session_id,
  content_id::bigint as content_id,
  event_ts::timestamptz as event_ts,
  (event_ts::timestamptz)::date as event_date,
  nullif(event_type, '') as event_type,
  time_spent_seconds::int as time_spent_seconds,
  scroll_count::int as scroll_count,
  completion_pct::int as completion_pct,
  completed_flag::smallint as completed_flag,
  live_game_flag::smallint as live_game_flag,
  major_tournament_flag::smallint as major_tournament_flag
from {{ source('raw', 'user_content_events') }}
