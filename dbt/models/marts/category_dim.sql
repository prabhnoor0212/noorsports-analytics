select
  category_id,
  category_name,
  category_level,
  parent_category_id,
  category_desc
from {{ ref('stg_categories') }}
