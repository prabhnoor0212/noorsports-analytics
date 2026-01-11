select
  category_id::bigint as category_id,
  nullif(category_name, '') as category_name,
  nullif(category_level, '') as category_level,
  nullif(parent_category_id::text, '')::bigint as parent_category_id,
  nullif(category_desc, '') as category_desc
from {{ source('raw', 'categories') }}
