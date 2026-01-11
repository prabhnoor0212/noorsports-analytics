with bounds as (
  select
    least(
      (select min(event_date) from {{ ref('stg_user_content_events') }}),
      (select min(session_date) from {{ ref('stg_sessions') }})
    ) as min_date,
    greatest(
      (select max(event_date) from {{ ref('stg_user_content_events') }}),
      (select max(session_date) from {{ ref('stg_sessions') }})
    ) as max_date
),
dates as (
  select generate_series(min_date, max_date, interval '1 day')::date as date
  from bounds
)
select
  to_char(date, 'YYYYMMDD')::int as date_id,
  date,
  extract(isodow from date)::int as iso_dow,
  extract(week from date)::int as week_of_year,
  extract(month from date)::int as month,
  extract(quarter from date)::int as quarter,
  extract(year from date)::int as year,
  to_char(date, 'YYYY-MM') as month_year,
  case when extract(isodow from date) in (6,7) then 1 else 0 end as is_weekend
from dates
