/**
 * Return elems 'inside' user's working-area with descending popindex.
 * Inside means touches. Ignore polygons where more than half is
 * outside working-area.
 *
 * %1$s Popindex-limit. Return elems with popindex > this.
 * %2$s User's working-area.
 * %3$s Number of elements to return.
 */
SELECT *
FROM elems
WHERE
    popindex > %1$s AND
    ST_Intersects(geometry, '%2$s') AND
    case
        when GeometryType(geometry) in ('POLYGON', 'MULTIPOLYGON') then
             ST_Area(ST_Intersection(geometry, '%2$s')) / area >= 0.5
        else TRUE
    end
order by popindex
limit %3$s;
