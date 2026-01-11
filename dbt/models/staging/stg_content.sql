select
  content_id::bigint as content_id,
  nullif(content_title, '') as content_title,
  publish_ts::timestamptz as publish_ts,
  nullif(content_type, '') as content_type,
  author_id::bigint as author_id,
  nullif(content_url, '') as content_url
from {{ source('raw', 'content') }}
