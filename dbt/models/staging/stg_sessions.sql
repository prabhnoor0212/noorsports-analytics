select
  nullif(session_id, '') as session_id,
  user_id::bigint as user_id,
  session_start_ts::timestamptz as session_start_ts,
  (session_start_ts::timestamptz)::date as session_date,
  nullif(platform_type, '') as platform_type,
  nullif(country, '') as country,
  nullif(state_province, '') as state_province,
  nullif(city, '') as city
from {{ source('raw', 'sessions') }}
