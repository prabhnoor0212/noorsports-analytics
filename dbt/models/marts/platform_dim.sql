select distinct
  platform_type as platform_id,
  platform_type as platform_type,
  platform_type as platform_desc
from {{ ref('stg_sessions') }}
where platform_type is not null
