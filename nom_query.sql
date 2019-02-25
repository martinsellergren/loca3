with summary as (
     select *,
     row_number() over ordered_by_importance as index,
     array_agg(class) over ordered_by_place_id as classes,
     array_agg(type) over ordered_by_place_id as types,
     array_agg(admin_level) over ordered_by_place_id as admin_levels,
     array_agg(geometry) over ordered_by_importance as geometries
     from placex
     window ordered_by_importance as (
            partition by osm_type,osm_id
            order by coalesce(importance, 0.75-(rank_search*1.0/40)) desc
            range between unbounded preceding and unbounded following),
     ordered_by_place_id as (
            partition by osm_type,osm_id
            order by place_id
            range between unbounded preceding and unbounded following))
select
    name->'name',
    classes,
    types,
    admin_levels,
    geometries,
    osm_type,
    osm_id
from
    summary
where
    index = 1 and name->'name' != ''
order by
      coalesce(importance, 0.75-(rank_search*1.0/40)) desc;
