-- Group by id. Add index based on importance in each group. Give elems data from all elems in same group.
with aux1 as (
select *,
     row_number() over ordered_by_importance as index,
     array_agg(class) over ordered_by_place_id as classes,
     array_agg(type) over ordered_by_place_id as types,
     array_agg(admin_level) over ordered_by_place_id as admin_levels,
     array_agg(geometry) over ordered_by_importance as geometries
from placex
where name != ''
window ordered_by_importance as (
            partition by osm_type,osm_id
            order by coalesce(importance, 0.75-(rank_search*1.0/40)) desc
            range between unbounded preceding and unbounded following),
     ordered_by_place_id as (
            partition by osm_type,osm_id
            order by place_id
            range between unbounded preceding and unbounded following)),

-- Filter out most important elems of each group. Select relevant data for those.
aux2 as (
select
    name->'name' as name,
    coalesce(importance, 0.75-(rank_search*1.0/40)) as importance,
    classes,
    types,
    admin_levels,
    geometries,
    osm_type::text || ':' || osm_id as id,
    extratags->'wikidata' as wikidata
from aux1
where index = 1),

-- Group by wikidata.
aux3 as (
select
    name,
    importance,
    row_number() over ordered_by_importance as index,
    array_agg(classes) over ordered_by_id as classes,
    array_agg(types) over ordered_by_id as types,
    array_agg(admin_levels) over ordered_by_id as admin_levels,
    array_agg(geometries) over ordered_by_id as geometries,
    array_agg(id) over ordered_by_importance as ids
from aux2
window ordered_by_importance as (
            partition by coalesce(wikidata, id)
            order by importance desc
            range between unbounded preceding and unbounded following),
ordered_by_id as (
            partition by coalesce(wikidata, id)
            order by id
            range between unbounded preceding and unbounded following))

select
    name,
    classes,
    types,
    admin_levels,
    --geometries,
    ids
from aux3
where index = 1
order by importance desc;
