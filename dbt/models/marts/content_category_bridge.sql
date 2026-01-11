select
  content_id,
  category_id,
  relationship_type,
  source,
  confidence_score,
  attribution_weight
from {{ ref('stg_content_category') }}
