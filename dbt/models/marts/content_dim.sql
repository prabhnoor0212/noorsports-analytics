select
  c.content_id,
  c.content_title,
  c.publish_ts,
  c.content_type as content_type_id,
  c.author_id,
  c.content_url
from {{ ref('stg_content') }} c
