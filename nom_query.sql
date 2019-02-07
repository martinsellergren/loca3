WITH summary as (
     select *, ROW_NUMBER() OVER (PARTITION BY osm_type,osm_id ORDER BY importance) as index
     from placex)
select
    name->'name',
    class,
    type,
    geometry,
    osm_type,
    osm_id
from summary
where index = 1 AND name->'name' != ''
order by
      coalesce(importance, 0.75-(rank_search*1.0/40)) desc;
