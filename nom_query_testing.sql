--------------------------------------------------------------------nom

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
from
    placex
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

------------------------------------------------------------------loca

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

--------------------------------------------------------------other

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
