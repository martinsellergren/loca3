-- Add cluster-index. Clusters of proximate elements with same names.
with aux1 as (
select *,
    ST_ClusterDBSCAN(geometry, eps := 50, minpoints := 1)
                               over (partition by name) AS cid
from elems
where supercat = 'r'
window same_names as (
            partition by unaccent(name)
            range between unbounded preceding and unbounded following))

-- Aggregate clusters.
select
    min(popindex) as popindex,
    name filter(where popindex=min(popindex)) as name,
    supercat filter(where popindex=min(popindex)) as supercat,
    subcat filter(where popindex=min(popindex)) as subcat,
    array_agg(geometry) as geometries,
    array_agg(osm_id) as osm_ids
from aux1
group by unaccent(name), cid;
