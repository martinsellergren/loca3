
with aux as (
select
    coalesce(importance, 0.75-(rank_search*1.0/40)) as importance,
    row_number() over same_ids as same_ids_index,
    row_number() over same_wikidata as same_wikidata_index,
    row_number() over same_wikipedia as same_wikipedia_index,
    array_agg(class) over same_ids ||
       array_agg(class) over same_wikidata ||
       array_agg(class) over same_wikipedia
       as classes,
    array_agg(type) over same_ids ||
       array_agg(type) over same_wikidata ||
       array_agg(type) over same_wikipedia
       as types,
    array_agg(admin_level) over same_ids ||
       array_agg(admin_level) over same_wikidata ||
       array_agg(admin_level) over same_wikipedia
       as admin_levels,
    array_agg(geometry) over same_ids ||
       array_agg(geometry) over same_wikidata ||
       array_agg(geometry) over same_wikipedia
       as geometries,
    array_agg(osm_type::text || ':' || osm_id) over same_ids ||
       array_agg(osm_type::text || ':' || osm_id) over same_wikidata ||
       array_agg(osm_type::text || ':' || osm_id) over same_wikipedia
    as osm_ids
from placex
where name != ''
window same_ids as (
       partition by osm_type, osm_id
       order by coalesce(importance, 0.75-(rank_search*1.0/40)) desc, place_id
       range between unbounded preceding and unbounded following),
same_wikidata as (
       partition by coalesce(extratags->'wikidata', id)
       order by coalesce(importance, 0.75-(rank_search*1.0/40)) desc, place_id
       range between unbounded preceding and unbounded following),
same_wikipedia as (
       partition by coalesce(wikipedia, id)
       order by coalesce(importance, 0.75-(rank_search*1.0/40)) desc, place_id
       range between unbounded preceding and unbounded following))

select
    row_number() over (order by importance) as popindex,
    name,
    classes,
    types,
    admin_levels,
    geometries,
    osm_ids
from
    aux
where
    same_ids_index = 1 and
    same_wikidata_index = 1 and
    same_wikipedia_index = 1
order by
      importance desc;
