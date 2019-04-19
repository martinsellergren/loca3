/**
 * @param1 osm_type
 * @param2 osm_id
 */
drop function if exists toWeb;
create function toWeb(character(1), bigint) returns text as $$
       select 'https://www.openstreetmap.org/' ||
              case when $1='N' then 'node'
                   when $1='W' then 'way'
                   else 'relation'
              end
              || '/' || $2
$$ language sql;





-- * nom

select
    left(name->'name', 30),
    type,
    class,
    GeometryType(geometry),
    'https://www.openstreetmap.org/' ||
                                     case when osm_type='N' then 'node'
                                          when osm_type='W' then 'way'
                                          else 'relation'
                                     end
                                     || '/' || osm_id,
    coalesce(importance, 0.75-(rank_search*1.0/40))
from
    placex
where
    GeometryType(geometry) = 'linestring' and st_isring(geometry) = true
order by
      coalesce(importance, 0.75-(rank_search*1.0/40)) desc;


select
    place_id,
    parent_place_id,
    linked_place_id,
    importance,
    geometry_sector,
    rank_address,
    rank_search,
    partition,
    indexed_status,
    osm_id,
    osm_type,
    class,
    type,
    left(name->'name',20),
    admin_level,
    address,
    extratags,
    country_code,
    housenumber,
    postcode,
    wikipedia,
    centroid
from
    placex
where
    name->'name' = 'São Tomé e Príncipe';
    place_id in (102524, 100001);

    geometry,
    name


WITH summary as (
     select *, ROW_NUMBER() OVER (PARTITION BY osm_type,osm_id ORDER BY importance) as index
     from placex)
select
    left(name->'name', 30),
    place_id,
    rank_search,
    'https://www.openstreetmap.org/' ||
                                     case when osm_type='N' then 'node'
                                          when osm_type='W' then 'way'
                                          else 'relation'
                                     end
                                     || '/' || osm_id,
    coalesce(importance, 0.75-(rank_search*1.0/40))
from summary
where index = 1 AND name->'name' != ''
order by
      coalesce(importance, 0.75-(rank_search*1.0/40)) desc;


test tar bort kopior:
osm_id=2401213448
select osm_type,osm_id from placex group by osm_type,osm_id having count(*) > 1;

select
    class,
    type,
    left(name->'name',40),
    admin_level,
    'https://www.openstreetmap.org/' ||
                                     case when osm_type='N' then 'node'
                                          when osm_type='W' then 'way'
                                          else 'relation'
                                     end
                                     || '/' || osm_id
from
    placex
where
    osm_type = 'N' and osm_id = 2563096203;


select
    array_agg(name->'name'),
    string_agg('https://www.openstreetmap.org/' ||
                                     case when osm_type='N' then 'node'
                                          when osm_type='W' then 'way'
                                          else 'relation'
                                     end
                                     || '/' || osm_id, ', ')
from placex
where wikipedia != ''
group by wikipedia
having count(*) > 1;

select
    array_agg(name->'name'),
    array_agg(importance),
    string_agg('https://www.openstreetmap.org/' ||
                                     case when osm_type='N' then 'node'
                                          when osm_type='W' then 'way'
                                          else 'relation'
                                     end
                                     || '/' || osm_id, ', ')
from placex
where extratags->'wikidata' != ''
group by extratags->'wikidata'
having count(*) > 1;



 en:Obo_Natural_Park
 en:Sete_Pedras

select
    name->'name',
    'https://www.openstreetmap.org/' ||
                                     case when osm_type='N' then 'node'
                                          when osm_type='W' then 'way'
                                          else 'relation'
                                     end
                                     || '/' || osm_id
from placex where wikipedia = 'en:Obo_Natural_Park';







select
    name->'name',
    ROW_NUMBER() OVER (PARTITION BY osm_type,osm_id ORDER BY coalesce(importance, 0.75-(rank_search*1.0/40))) as osmId_index,
    ROW_NUMBER() OVER (PARTITION BY coalesce(wikipedia, place_id::text) ORDER BY coalesce(importance, 0.75-(rank_search*1.0/40))) as wikipedia_index,
    ROW_NUMBER() OVER (PARTITION BY coalesce(extratags->'wikidata', place_id::text) ORDER BY coalesce(importance, 0.75-(rank_search*1.0/40))) as wikidata_index
from placex;


WITH summary as (
     select *,
     ROW_NUMBER() OVER (PARTITION BY osm_type,osm_id ORDER BY coalesce(importance, 0.75-(rank_search*1.0/40))) as osmId_index,
     ROW_NUMBER() OVER (PARTITION BY wikipedia ORDER BY coalesce(importance, 0.75-(rank_search*1.0/40))) as wikipedia_index,
     ROW_NUMBER() OVER (PARTITION BY extratags->'wikidata' ORDER BY coalesce(importance, 0.75-(rank_search*1.0/40))) as wikidata_index,
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


select
    array_agg(name->'name'),
    array_agg(coalesce(importance, 0.75-(rank_search*1.0/40)))
from
    placex
group by
      osm_type, osm_id
having
    count(*) > 1;


select name->'name',class,type
from placex
where name->'name' = 'Cascata de Bombaim';

select
    class,type,classes,types
from
(
    select
        *,
        array_agg(name->'name') over w as names,
        array_agg(class) over w as classes,
        array_agg(type) over w as types
    from
        placex
    window w as (partition by osm_type,osm_id order by place_id range between unbounded preceding and unbounded following)
) as foo
where
    name->'name' = 'Cascata de Bombaim';


select array_agg(name->'name')
from placex
where name->'name' ~ 'cipe';

select name->'name' as nameee
from placex
group by nameeeee;


select st_npoints(st_simplify(geometry,0.05,true)), st_asGeoJson(st_simplify(geometry,0.05,true)) FROM placex where place_id=329195;

select place_id from placex where geometrytype(geometry) = 'LINESTRING' order by st_npoints(geometry) desc limit 1;
select st_npoints(st_simplify(geometry,0.0003, true)) FROM placex where place_id=306124;
select st_asGeoJson(st_simplify(geometry,0.0003, true)) FROM placex where place_id=306124;

select count(*) from placex where st_npoints(geometry) > 100;

-- * loca

select
    string_agg(popindex || '', ', '),
    name,
    string_agg(supercat, ', '),
    string_agg(subcat, ', '),
    string_agg(osm_id, ', ')
from
    elems
group by
      name
having
    count(*) > 1;


Avenida Marginal 12 Julho      | {617,516,514,517,525,597,613,618,611,612}    |    10

select
    left(name,30),
    array_agg(popindex) over same_names
    array_agg(popindex) filter(where popindex=min(popindex))

from elems
window same_names as (
       partition by name order by popindex);





select
    left(name->'name',30),
    GeometryType(geometry),
    pg_typeof(ST_Intersects(geometry, 'SRID=4326;POLYGON((6.4071253 0.4696603,6.2251096 -0.1675413,6.7856924 -0.0137329,7.0137726 0.4669138,6.4071253 0.4696603))')),
    ST_Area(ST_Intersection(geometry, 'SRID=4326;POLYGON((6.4071253 0.4696603,6.2251096 -0.1675413,6.7856924 -0.0137329,7.0137726 0.4669138,6.4071253 0.4696603))')),
    GeometryType(ST_Intersection(geometry, 'SRID=4326;POLYGON((6.4071253 0.4696603,6.2251096 -0.1675413,6.7856924 -0.0137329,7.0137726 0.4669138,6.4071253 0.4696603))'))
from
    placex;


SELECT name->'name', octet_length(t.*::text) FROM placex as t order by 2 desc limit 10;

-- * other

drop function if exists withUniqueId;
create function withUniqueId(anyarray, text[]) returns anyarray as $$
       with elems as (
       select (array_agg(elem))[1] as elem
       from unnest($1, $2) as t(elem, id)
       group by id)
       select array_agg(elem) from elems;
$$ LANGUAGE SQL;

select withUniqueId(ARRAY[1,2,3,4,5,6,7,8,9,1], ARRAY['1','2','3','1','3','4','1','2','2','432']);


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
$$ LANGUAGE SQL;



select pickGeometry(geoms, ARRAY[3, 2])
from(
select
    name->'name',
    array_agg(GeometryType(geometry)),
    array_agg(ST_NPoints(geometry)),
    array_agg(geometry) as geoms
from placex
group by name->'name'
order by coalesce(max(importance), 0.75-(min(rank_search)*1.0/40)) desc
limit 1
) as _;


select ST_ClusterDBSCAN(geometry, eps := 50, minpoints := 1)
from placex
group by name;

select ST_Area(geometry) from elems where GeometryType(geometry) = 'MULTILINESTRING';



SELECT
    popindex,
    name,
    supercat,
    subcat,
    osm_ids,
    GeometryType(geometry)
FROM
    elems
WHERE
    popindex > 0 AND
    ST_Intersects(geometry, 'SRID=4326;POLYGON((6.4071253 0.4696603,6.2251096 -0.1675413,6.7856924 -0.0137329,7.0137726 0.4669138,6.4071253 0.4696603))') AND
    case
        when GeometryType(geometry) in ('POLYGON', 'MULTIPOLYGON') then
             ST_Area(ST_Intersection(geometry, 'SRID=4326;POLYGON((6.4071253 0.4696603,6.2251096 -0.1675413,6.7856924 -0.0137329,7.0137726 0.4669138,6.4071253 0.4696603))')) / ST_Area(geometry) >= 0.5
        else TRUE
    end
order by popindex
limit 100;


select ST_Transform(ST_GeomFromText('POINT(-181 -180)', 4326), 3857);
