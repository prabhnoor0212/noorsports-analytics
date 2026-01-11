with recursive tree as (
  -- self rows
  select
    c.category_id as ancestor_category_id,
    c.category_id as descendant_category_id,
    0 as depth
  from {{ ref('category_dim') }} c

  union all

  -- ancestor -> child
  select
    t.ancestor_category_id,
    c.category_id as descendant_category_id,
    t.depth + 1 as depth
  from tree t
  join {{ ref('category_dim') }} c
    on c.parent_category_id = t.descendant_category_id
)
select
  ancestor_category_id,
  descendant_category_id,
  depth
from tree
