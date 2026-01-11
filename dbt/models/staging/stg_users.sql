select
  user_id::bigint as user_id,
  signup_ts::timestamptz as signup_ts,
  nullif(country, '') as country,
  nullif(marketing_channel, '') as marketing_channel,
  nullif(fav_sport, '') as fav_sport
from {{ source('raw', 'users') }}
