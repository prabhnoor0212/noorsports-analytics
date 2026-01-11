select
  content_id::bigint as content_id,
  category_id::bigint as category_id,
  nullif(relationship_type, '') as relationship_type,
  nullif(source, '') as source,
  confidence_score::numeric(5,3) as confidence_score,
  attribution_weight::numeric(6,3) as attribution_weight
from {{ source('raw', 'content_category') }}
