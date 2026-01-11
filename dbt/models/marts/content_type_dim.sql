select distinct
  content_type as content_type_id,
  content_type as content_type,
  initcap(replace(content_type, '_', ' ')) as content_type_desc
from {{ ref('stg_content') }}
where content_type is not null
