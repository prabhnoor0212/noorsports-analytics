select distinct
  md5(coalesce(country,'') || '|' || coalesce(state_province,'') || '|' || coalesce(city,'')) as geo_id,
  country,
  state_province,
  city
from {{ ref('stg_sessions') }}
where country is not null
