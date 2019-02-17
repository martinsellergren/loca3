WITH summary as (
     select *,
     ROW_NUMBER() OVER (PARTITION BY osm_type,osm_id ORDER BY importance) as index,
     string_agg(class, '_:_') OVER (PARTITION BY osm_type,osm_id ORDER BY place_id) as classes,
     string_agg(type, '_:_') OVER (PARTITION BY osm_type,osm_id ORDER BY place_id) as types
     from placex)
select
    name->'name',
    classes,
    types,
    admin_level,
    geometry,
    osm_type,
    osm_id
from summary
where index = 1 AND name->'name' != ''
order by
      coalesce(importance, 0.75-(rank_search*1.0/40)) desc;
