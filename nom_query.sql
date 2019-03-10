create extension if not exists postgis;
create extension if not exists unaccent;


-------------------------------------------------------FUNCTION DEFS

/**
 * @param1 elems
 * @param2 ids
 * Each elem in elems has corresponding id in ids.
 * Returns elems with unique ids, ordered by id.
 */
drop function if exists withUniqueId;
create function withUniqueId(anyarray, text[]) returns anyarray as $$
       with elems as (
       select (array_agg(elem))[1] as elem, id
       from unnest($1, $2) as t(elem, id)
       group by id
       order by id)
       select array_agg(elem order by id) from elems;
$$ language sql;

/**
 * @param1 geometries
 * @param2 importances
 * Each geometry in geometries has corresponding importance in importances.
 * Return most important geometry that isn't a node.
 * If all geometries are nodes, return most important node.
 */
drop function if exists pickGeometry;
create function pickGeometry(geometry[], double precision[]) returns geometry as $$
       select coalesce(
              (select geometry
              from unnest($1, $2) as t(geometry, importance)
              where ST_NPoints(geometry) > 1
              order by importance desc
              limit 1)
              ,
              (select geometry
              from unnest($1, $2) as t(geometry, importance)
              order by importance desc
              limit 1));
$$ language sql;


/**
 * @param1 nom_importance
 * @param2 rank_search
 * @return importance. Same as nom_importance if exists.
 */
drop function if exists toImportance;
create function toImportance(double precision, smallint) returns double precision as $$
       select coalesce($1, 0.75-($2*1.0/40));
$$ language sql;

/**
 * Convert osm_type, osm_id to 'id' used by elems in this database.
 * @param1 osm_type
 * @param2 osm_id
 * @return osm_id * 10 + 0/1/2(node/way/relation)
 */
drop function if exists toId;
create function toId(character(1), bigint) returns text as $$
       select case when $1 = 'N' then 'node'
                   when $1 = 'W' then 'way'
                   else 'relation'
              end || '/' || $2;
$$ language sql;

/**
 * @param1 id (node/way/relation)/id
 * @return Osm webaddress of element with specified id.
 */
drop function if exists toWeb;
create function toWeb(text) returns text as $$
       select 'https://www.openstreetmap.org/' || $1;
$$ language sql;

/**
 * @param1 ids
 * @return Osm webaddresses.
 */
drop function if exists toWebs;
create function toWebs(text[]) returns text as $$
       select string_agg(toWeb(id), ', ')
       from unnest($1) as id;
$$ language sql;

---------------------------------------------------------MAIN QUERY

/**
 * Partitions with same osm_id, same wikidata and same wikipedia.
 * Complete elems with data from all elems in same partitions.
 * Most important elem in each partition has index 1.
 */
with aux1 as (
select
    toImportance(importance, rank_search) as importance,
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
    array_agg(toId(osm_type, osm_id)) over same_id ||
       array_agg(toId(osm_type, osm_id)) over same_wikidata ||
       array_agg(toId(osm_type, osm_id)) over same_wikipedia
    as ids,
    array_agg(toImportance(importance, rank_search)) over same_id ||
       array_agg(toImportance(importance, rank_search)) over same_wikidata ||
       array_agg(toImportance(importance, rank_search)) over same_wikipedia
    as importances
from placex
where name->'name' != '' --TODO?
window same_id as (
       partition by osm_type, osm_id
       order by toImportance(importance, rank_search) desc, place_id
       range between unbounded preceding and unbounded following),
same_wikidata as (
       partition by coalesce(extratags->'wikidata', place_id::text)
       order by toImportance(importance, rank_search) desc, place_id
       range between unbounded preceding and unbounded following),
same_wikipedia as (
       partition by coalesce(wikipedia, place_id::text)
       order by toImportance(importance, rank_search) desc, place_id
       range between unbounded preceding and unbounded following)),

/**
 * Filter out elems that are most important in all three groups.
 * Pick importance, name from most important elem. Classes, types etc
 * from all elems in same group (any of them). Pick one of the
 * geometries, and pick id to relate to the element of this geometry.
 *
 * Milestone before deduping.
 */
aux2 as (
select
    importance,
    name,
    withUniqueId(classes, ids) as classes,
    withUniqueId(types, ids) as types,
    withUniqueId(admin_levels, ids) as admin_levels,
    pickGeometry(geometries, importances) as geometry,
    ids[ array_position(geometries, pickGeometry(geometries, importances)) ] as id
from
    aux1
where
    same_ids_index = 1 and
    same_wikidata_index = 1 and
    same_wikipedia_index = 1),

------------------------------------------------------------DEDUPE

/**
 * Add cluster-index. Clusters of proximate elements with same
 * name. Only dedupe linestrings (i.e not nodes, polygons or
 * multipolygons).
 *
 * NOTE: MAX_DEDUPE_DISTANCE (i.e max distance between two same-name
 * objects for merging them is specified in the ST_ClusterDBSCAN's
 * eps parameter.
 */
aux3 as (
select *,
       case when GeometryType(geometry) != 'LINESTRING'
            then -1 * row_number() over (order by id)
            else ST_ClusterDBSCAN(geometry, eps := 100, minpoints := 1)
                 over same_name_and_geometryType
       end AS cid
from aux2 as outerQuery
window same_name_and_geometryType as (
       partition by unaccent(name), GeometryType(geometry))),

-- select cid
-- from aux3
-- group by cid
-- having count(*) > 1;

-- select name, cid, toWeb(id)
-- from aux3
-- where GeometryType(geometry) = 'LINESTRING'
--  and name = 'Avenida das Naçoes Unidas';

/**
 * Aggregate data from same cluster. Importance and name from most
 * important elem. Geometries collected into a singel geometry.
 */
aux4 as (
select
    row_number() over same_cluster as index_in_cluster,
    (array_agg(importance) over same_cluster)[1] as importance,
    (array_agg(name) over same_cluster)[1] as name,
    array_agg(classes) over same_cluster as classes,
    array_agg(types) over same_cluster as types,
    array_agg(admin_levels) over same_cluster as admin_levels,
    ST_Union(geometry) over same_cluster as geometry,
    array_agg(id) over same_cluster as ids
from aux3
window same_cluster as (
       partition by unaccent(name), cid
       order by importance desc
       range between unbounded preceding and unbounded following)),

/**
 * Final fix: unnest data and add popindex.
 */
aux5 as (
select
    row_number() over (order by importance desc) as popindex,
    name,
    array_to_string(classes, ',') as classes,
    array_to_string(types, ',') as types,
    array_to_string(admin_levels, ',') as admin_levels,
    geometry,
    array_to_string(ids, ',') as ids
from aux4
where index_in_cluster = 1
order by importance desc)


select *
from aux5;
