with summary as (
     select *,
     row_number() over same_elems_ordered_by_importance as index,
     string_agg(class, '_:_') over same_elems_ordered_by_place_id as classes,
     string_agg(type, '_:_') over same_elems_ordered_by_place_id as types,
     string_agg(admin_level::text, '_:_') over same_elems_ordered_by_place_id as admin_levels
     from placex
     WINDOW same_elems_ordered_by_importance as (
            partition by osm_type,osm_id order by coalesce(importance, 0.75-(rank_search*1.0/40)) desc),
     same_elems_ordered_by_place_id as (
            partition by osm_type,osm_id order by place_id range between unbounded preceding and unbounded following))
select
    name->'name',
    classes,
    types,
    admin_levels,
    osm_type,
    osm_id
from
    summary
where
    index = 1 and name->'name' != '' and classes ~ ':_'
order by
      coalesce(importance, 0.75-(rank_search*1.0/40)) desc;
