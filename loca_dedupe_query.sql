CREATE EXTENSION unaccent;

-- Add cluster-index. Clusters of proximate elements with same names.
with aux1 as (
select *,
    ST_ClusterDBSCAN(geometry, eps := 50, minpoints := 1) over same_names AS cid
from elems
where supercat = 'r'
window same_names as (
            partition by unaccent(name)
            range between unbounded preceding and unbounded following))

-- Group by name and cluster-id. Add index based on importance in each group. Give data from all elems in same group.
with aux2 as (
select *,
    row_number() over same_clusters as index,
    array_agg(geometry) over same_clusters as geometries,
    array_agg(osm_id) over same_clusters as osm_ids
from aux1
window same_clusters as (
       partition by unaccent(name), cid
       order by popindex
       range between unbounded preceding and unbounded following))

-- Filter out most important elem, with additional data.
select
    popindex,
    name,
    supercat,
    subcat,
    geometries,
    osm_ids
from aux2
where index = 1
order by popindex;
