drop function if exists withUniqueId;
create function withUniqueId(anyarray, text[]) returns anyarray as $$
       with elems as (
       select (array_agg(elem))[1] as elem
       from unnest($1, $2) as t(elem, id)
       group by id)
       select array_agg(elem) from elems;
$$ LANGUAGE SQL;


with aux as (
select
    coalesce(importance, 0.75-(rank_search*1.0/40)) as importance,
    name->'name' as name,
    row_number() over same_id as same_ids_index,
    row_number() over same_wikidata as same_wikidata_index,
    row_number() over same_wikipedia as same_wikipedia_index,
    array_agg(class) over same_id ||
       array_agg(class) over same_wikidata ||
       array_agg(class) over same_wikipedia
       as classes,
    array_agg(type) over same_id ||
       array_agg(type) over same_wikidata ||
       array_agg(type) over same_wikipedia
       as types,
    array_agg(admin_level) over same_id ||
       array_agg(admin_level) over same_wikidata ||
       array_agg(admin_level) over same_wikipedia
       as admin_levels,
    array_agg(geometry) over same_id ||
       array_agg(geometry) over same_wikidata ||
       array_agg(geometry) over same_wikipedia
       as geometries,
    array_agg(osm_type::text || ':' || osm_id) over same_id ||
       array_agg(osm_type::text || ':' || osm_id) over same_wikidata ||
       array_agg(osm_type::text || ':' || osm_id) over same_wikipedia
    as osm_ids,
    array_agg(coalesce(importance, 0.75-(rank_search*1.0/40))) over same_id ||
       array_agg(coalesce(importance, 0.75-(rank_search*1.0/40))) over same_wikidata ||
       array_agg(coalesce(importance, 0.75-(rank_search*1.0/40))) over same_wikipedia
    as importances
from placex
where name != ''
window same_id as (
       partition by osm_type, osm_id
       order by coalesce(importance, 0.75-(rank_search*1.0/40)) desc, place_id
       range between unbounded preceding and unbounded following),
same_wikidata as (
       partition by coalesce(extratags->'wikidata', place_id::text)
       order by coalesce(importance, 0.75-(rank_search*1.0/40)) desc, place_id
       range between unbounded preceding and unbounded following),
same_wikipedia as (
       partition by coalesce(wikipedia, place_id::text)
       order by coalesce(importance, 0.75-(rank_search*1.0/40)) desc, place_id
       range between unbounded preceding and unbounded following))

select
    row_number() over (order by importance desc) as popindex,
    name,
    withUniqueId(classes, osm_ids),
    withUniqueId(types, osm_ids),
    withUniqueId(admin_levels, osm_ids),
    --pickGeometry(geometries, importances),
    withUniqueId(osm_ids, osm_ids)
from
    aux
where
    same_ids_index = 1 and
    same_wikidata_index = 1 and
    same_wikipedia_index = 1
order by
      importance desc;
