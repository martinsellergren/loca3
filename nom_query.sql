create extension if not exists postgis;
create extension if not exists unaccent;

DROP AGGREGATE IF EXISTS array_concat_agg(anyarray);
CREATE AGGREGATE array_concat_agg(anyarray) (
  SFUNC = array_cat,
  STYPE = anyarray
);


-- * FUNCTION DEFS

/**
 * @param1 geometries
 * @param2 importances
 * Each geometry in geometries has corresponding importance in importances.
 * Returns most important geometry that isn't a node.
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
       with vars as (
       select
              0.8   5 as IMPORTANCE_FACTOR,
              coalesce($1, 0) as importance1, --todo
              --$1 as importance1,
              (30 - (coalesce($2, 30))) / 30::double precision as importance2
       )
       select
        case when importance1 is not null
             then IMPORTANCE_FACTOR * importance1 + (1 - IMPORTANCE_FACTOR) * importance2
             else importance2 end
        from vars;
$$ language sql;

/**
 * Convert osm_type, osm_id to 'id' used by elems in this database.
 * @param1 osm_type
 * @param2 osm_id
 * @return osm_id * 10 + 0/1/2(node/way/relation)
 */
drop function if exists toId;
create function toId(character(1), bigint) returns text as $$
       select $1 || $2;
$$ language sql;

/**
 * @param1 Name
 * @param2 Wikipedia-entry
 * @return param1 or NULL if Wikipedia-entry seems invalid.
 */
drop function if exists testWikipedia;
create function testWikipedia(text, text) returns text as $$
       with vars as (
       select
            $1 as name,
            $2 as wiki
       )
       select
            case when replace(lower(unaccent(wiki)), '_', ' ') like '%' || replace(lower(unaccent(name)), '_', ' ') || '%'
                 then wiki
                 else null
            end
       from vars
$$ language sql;

/**
 * @param1 geometries
 * @param2 importances. Ordered desc.
 * geometry[i] has importance importances[i].
 *
 * Returns geometries except every linestring replaced with 'all linestrings collected into one'.
 * Array-length preserved. Preserved order.
 */
drop function if exists mergeLinestrings;
create function mergeLinestrings(geometry[], double precision[]) returns geometry[] as $$
       with aux1 as (
       select
        importance,
        case when GeometryType(geometry) = 'LINESTRING'
             then (
                  select st_union(geometry)
                  from unnest($1) as geometry
                  where GeometryType(geometry) = 'LINESTRING'
             )
             else geometry end
             as geometry
        from unnest($1, $2) as t(geometry, importance))

        select array_agg(geometry order by importance) from aux1;
$$ language sql;


-- * QUERY

/**
 * Fetch needed stuff, filter and minor fixes.
 */
with aux0 as (
select
    name->'name' as name,
    class,
    type,
    admin_level,
    geometry,
    osm_type,
    osm_id,
    importance as nom_importance,
    rank_search,
    place_id,
    extratags,
    testWikipedia(name->'name', wikipedia) as wikipedia
from placex
where name is not null and name->'name' is not null and name->'name' != '' and st_isvalid(geometry)
),

/**
 * Partitions with same osm_id, same wikidata and same wikipedia.
 * Complete elems with data from all elems in same partitions.
 * Most important elem in each partition has index 1.
 */
aux1 as (
select
    case when wikipedia is not null
         then toImportance(nom_importance, rank_search)
         else toImportance(null, rank_search) end
         as importance,
    name,
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
    array_agg(toImportance(nom_importance, rank_search)) over same_id ||
       array_agg(toImportance(nom_importance, rank_search)) over same_wikidata ||
       array_agg(toImportance(nom_importance, rank_search)) over same_wikipedia
    as importances
from aux0
window same_id as (
       partition by osm_type, osm_id
       order by toImportance(nom_importance, rank_search) desc, place_id
       range between unbounded preceding and unbounded following),
same_wikidata as (
       partition by coalesce(extratags->'wikidata', place_id::text)
       order by toImportance(nom_importance, rank_search) desc, place_id
       range between unbounded preceding and unbounded following),
same_wikipedia as (
       partition by coalesce(wikipedia, place_id::text)
       order by toImportance(nom_importance, rank_search) desc, place_id
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
    classes,
    types,
    admin_levels,
    pickGeometry(geometries, importances) as geometry,
    ids[ array_position(geometries, pickGeometry(geometries, importances)) ] as id
from
    aux1
where
    same_ids_index = 1 and
    same_wikidata_index = 1 and
    same_wikipedia_index = 1),



-- ** DEDUPE

/**
 * Add cluster-index. Clusters of proximate elements with same
 * name.
 *
 * NOTE: MAX_DEDUPE_DISTANCE (i.e max distance between two same-name
 * objects for merging them is specified in the ST_ClusterDBSCAN's
 * eps parameter. ST_ClusterDBSCAN eps-parameter in input geometry unit
 * (srid:4326=lon/lat) so transform geometry (3857-Pseudo-Mercator=meter).
 * Eps valid close to equator, distorted (decreases) towards poles.
 */
aux3 as (
select *,
       ST_ClusterDBSCAN(ST_Transform(geometry, 3857), eps := 100, minpoints := 1) over same_name as cid
from aux2
window same_name as (
       partition by unaccent(name))),

-- aux3 as (
-- select *,
--        case when GeometryType(geometry) != 'LINESTRING'
--             then -1 * row_number() over (order by id)
--             else ST_ClusterDBSCAN(ST_Transform(geometry, 3857), eps := 100, minpoints := 1)
--                  over same_name_and_geometryType
--        end AS cid
-- from aux2
-- window same_name_and_geometryType as (
--        partition by unaccent(name), GeometryType(geometry))),

/**
 * Aggregate data from same cluster. Importance and name from most
 * important elem.
 */
aux4 as (
select
    row_number() over same_cluster as index_in_cluster,
    (array_agg(importance) over same_cluster)[1] as importance,
    (array_agg(name) over same_cluster)[1] as name,
    array_concat_agg(classes) over same_cluster as classes,
    array_concat_agg(types) over same_cluster as types,
    array_concat_agg(admin_levels) over same_cluster as admin_levels,
    array_agg(geometry) over same_cluster as geometries,
    array_agg(importance) over same_cluster as importances,
    array_agg(id) over same_cluster as ids
from aux3
window same_cluster as (
       partition by unaccent(name), cid
       order by importance desc, id
       range between unbounded preceding and unbounded following))


/**
 * Geometries converted into a singel geometry.
 * Unnest data and add popindex.
 */
select
    row_number() over (order by importance desc, ids) as popindex,
    name,
    array_to_string(classes, ',') as classes,
    array_to_string(types, ',') as types,
    array_to_string(array_replace(admin_levels, null, -1::smallint), ',') as admin_levels,
    pickGeometry(mergeLinestrings(geometries, importances), importances) as geometry,
    array_to_string(ids, ',') as ids
from aux4
where index_in_cluster = 1
order by importance desc, ids;
