select hexagon 
from hexgrid h 

with track as (
select st_setsrid(st_makeline(coords), 4326) as g 
from rawtracks r 
where track_id = 1
)
select g
from track
union
select st_setsrid(hexagon, 4326) as g
from hexgrid h, track t
where st_crosses(t.g, st_setsrid(hexagon, 4326))

with track as (
		select st_setsrid(st_makeline(coords), 4326) as g 
		from rawtracks r 
		where track_id = 1
	 ), hex_on_flight as (
		select hexagon_id, st_setsrid(hexagon, 4326) 
		from hexgrid h, track t
		where st_crosses(t.g, st_setsrid(hexagon, 4326))
	 )
select *
from hex_on_flight

select *
from rawtracks r, hexgrid h 
where track_id = 127 and h.hexagon_id = 1118 and ST_within(r.coords, h.hexagon) 

drop table lines;
create table lines (
	id serial primary key, 
	geom geometry);

insert into lines (geom) 
	    select st_setsrid(st_makeline(coords), 4326) as geom 
		from rawtracks r 
		where track_id = 127

insert  table lines (id primary key SERIAL, geom geometry) as (
		select st_setsrid(st_makeline(coords), 4326) as geom 
		from rawtracks r 
		where track_id = 127
)

SELECT ply.hexagon_id AS ply_id, ln.id AS ln_id,
       ROW_NUMBER() OVER(PARTITION BY ply.hexagon_id, ln.id ORDER BY _its.path) AS seg_id,
       _inside::GEOMETRY(LINESTRING, 4326) AS geom
FROM   lines AS ln
JOIN   hexgrid AS ply
  ON   ST_Intersects(ln.geom, st_setsrid(ply.hexagon, 4326))
CROSS JOIN LATERAL
       ST_Dump(ST_Intersection(ln.geom, st_setsrid(ply.hexagon, 4326))) AS _its
CROSS JOIN LATERAL
       ST_LineLocatePoint(ln.geom, ST_StartPoint(_its.geom)) AS __sfrac
CROSS JOIN LATERAL
       ST_LineLocatePoint(ln.geom, ST_EndPoint(_its.geom)) AS __efrac
CROSS JOIN LATERAL
       ST_LineSubstring(ln.geom, __sfrac, __efrac) AS _inside
WHERE  _inside IS NOT NULL
;


SELECT hexagon_id AS ply_id, ln.id AS ln_id,
       ROW_NUMBER() OVER(PARTITION BY ply.hexagon_id, ln.id ORDER BY _its.path) AS seg_id,
       _sm, _em,
       _inside::GEOMETRY(LINESTRINGM, 4326) AS geom
FROM  lines AS ln
JOIN  hexgrid AS ply
  ON   ST_Intersects(ln.geom, ply.hexagon)
CROSS JOIN LATERAL
       ST_Dump(ST_Intersection(ln.geom, ply.hexagon)) AS _its
CROSS JOIN LATERAL
       ST_LineLocatePoint(ln.geom, ST_StartPoint(_its.geom)) AS __sfrac
CROSS JOIN LATERAL
       ST_LineLocatePoint(ln.geom, ST_EndPoint(_its.geom)) AS __efrac
CROSS JOIN LATERAL
       ST_M(ST_LineInterpolatePoint(ln.geom, __sfrac)) AS _sm
CROSS JOIN LATERAL
       ST_M(ST_LineInterpolatePoint(ln.geom, __efrac)) AS _em
CROSS JOIN LATERAL
       ST_LineSubstring(ln.geom, __sfrac, __efrac) AS _inside
WHERE  _inside IS NOT NULL
;

SELECT ply_id, ln_id,
       MIN(_sm) AS entry, MAX(_em) AS exit
FROM   (
    SELECT ply_id, ln_id, seg_id,
           _sm, _em, __sgap, __egap, COALESCE((__sgap OR __egap), __egap),
           ROW_NUMBER() OVER wa AS __seq,
           ROW_NUMBER() OVER wb AS __cseq
    FROM   (
        SELECT ply_id, ln_id, seg_id,
               _sm, _em,
               _sm - LAG(_em) OVER wa < 10 AS __sgap,
               LEAD(_sm) OVER wa - _em < 10 AS __egap
        FROM   <above_query>
        WINDOW
             wa AS (PARTITION BY ply_id, ln_id ORDER BY seg_id)
    ) q
    WINDOW
         wa AS (PARTITION BY ply_id, ln_id ORDER BY seg_id),
         wb AS (PARTITION BY ply_id, ln_id, COALESCE((__sgap OR __egap), __egap) ORDER BY seg_id)
) q
GROUP BY
      ply_id, ln_id, (__seq - __cseq)
ORDER BY
      MIN(_sm)
;


select ST_Dump(ST_Intersection(ln.geom, ply.hexagon))
from lines ln, hexgrid ply


select hexagon
from hexgrid h 
where hexagon_id = 1118

SELECT startTime, 
       endTime, 
       ST_MakeLine(geom_array) AS line,
       endTime - startTime as duration,
       ST_Z(st_startpoint( ST_MakeLine(geom_array))) as alt_entry,
       ST_Z(st_endpoint(( ST_MakeLine(geom_array)))) as alt_exit,
       - ST_Z(st_startpoint( ST_MakeLine(geom_array))) + ST_Z(st_endpoint(( ST_MakeLine(geom_array)))) as alt_gain,
       st_geogfromtext('POLYGON ((-100.13388595389931 19.057000000000002, -100.13388595389931 19.0638, -100.13977492664505 19.0672, -100.14566389939078 19.0638, -100.14566389939078 19.057000000000002, -100.13977492664505 19.053600000000003, -100.13388595389931 19.057000000000002))') as hex
FROM (
    -- Aggregate the points based on sequence group
    SELECT min(captureTime) AS startTime, 
           max(CaptureTime) AS endTime, 
           array_agg(location) AS geom_array, 
           grp
    FROM (
        SELECT *, 
               seq - ROW_NUMBER() OVER (ORDER BY captureTime) AS grp
        FROM (
            -- Sequence the points based on time captured
            SELECT ST_M(coords) as captureTime, 
                   coords as location, 
                   ROW_NUMBER() OVER (ORDER BY ST_M(coords)) AS seq 
            FROM rawtracks_m rm
            ) orderPoints
        WHERE ST_Intersects(location,st_geogfromtext('POLYGON ((-100.13388595389931 19.057000000000002, -100.13388595389931 19.0638, -100.13977492664505 19.0672, -100.14566389939078 19.0638, -100.14566389939078 19.057000000000002, -100.13977492664505 19.053600000000003, -100.13388595389931 19.057000000000002))'
)) --Replace envelope with polygon
        ) pointsInside
    GROUP BY grp
    ) pointGroup;
   
   
with orderPoints as (   
		SELECT 
			ST_M(coords) as captureTime,	
			coords as location,
			turning,
       		ROW_NUMBER() OVER (ORDER BY ST_M(coords)) AS seq 
		FROM rawtracks_m rm
), pointsInside as (
        SELECT 
        	*, 
            seq - ROW_NUMBER() OVER (ORDER BY captureTime) AS grp
        FROM orderPoints
        WHERE ST_Intersects(location,st_geogfromtext('POLYGON ((-100.13388595389931 19.057000000000002, -100.13388595389931 19.0638, -100.13977492664505 19.0672, -100.14566389939078 19.0638, -100.14566389939078 19.057000000000002, -100.13977492664505 19.053600000000003, -100.13388595389931 19.057000000000002))')) 
), pointGroup as (
		SELECT 
		   min(captureTime) AS startTime, 
           max(CaptureTime) AS endTime, 
           array_agg(location) AS geom_array,
           count(*) as n_points,
           count(CASE WHEN turning = TRUE THEN 1 END) as n_turning,
           grp
		from pointsInside
		GROUP BY grp
), crossings as (
		select
			1.0 * n_turning / n_points as turning_ratio,
			startTime, 
		    endTime, 
		    ST_MakeLine(geom_array) AS line,
		    endTime - startTime as duration,
		    ST_Z(st_startpoint( ST_MakeLine(geom_array))) as alt_entry,
		    ST_Z(st_endpoint(( ST_MakeLine(geom_array)))) as alt_exit,
		    st_geogfromtext('POLYGON ((-100.13388595389931 19.057000000000002, -100.13388595389931 19.0638, -100.13977492664505 19.0672, -100.14566389939078 19.0638, -100.14566389939078 19.057000000000002, -100.13977492664505 19.053600000000003, -100.13388595389931 19.057000000000002))') as hex
		from pointGroup
), crossings_with_deltas as (
	  	select 
		    *,
			alt_exit - alt_entry as alt_gain
		from crossings
)
select
	*,
	alt_gain / duration as vertical_speed
from
	crossings_with_deltas
where duration > 100 and alt_gain > 0
	

with 
	hexi as (
	select hexagon
	from hexgrid h
	where hexagon_id = 1118),	
orderPoints as (   
		SELECT 
			ST_M(coords) as captureTime,	
			coords as location,
			turning,
       		ROW_NUMBER() OVER (ORDER BY ST_M(coords)) AS seq 
		FROM rawtracks_m rm
), pointsInside as (
        SELECT 
        	*, 
            seq - ROW_NUMBER() OVER (ORDER BY captureTime) AS grp
        FROM orderPoints
        WHERE ST_Intersects(location, (select hexagon from hexgrid h2 where hexagon_id = 1118))        
)
select *
from pointsInside


select st_extrude(
	 	st_geomfromtext('POLYGON ((-100.13388595389931 19.057000000000002, -100.13388595389931 19.0638, -100.13977492664505 19.0672, -100.14566389939078 19.0638, -100.14566389939078 19.057000000000002, -100.13977492664505 19.053600000000003, -100.13388595389931 19.057000000000002))')
, 0, 0, 300)
	 
	 
select ST_Extrude(st_force3d((select hexagon from hexgrid h2 where hexagon_id = 1118), 1500),0,0,100);

(select hexagon from hexgrid h2 where hexagon_id = 1118)


   
with orderPoints as (   
		SELECT 
			ST_M(coords) as captureTime,	
			coords as location,
			turning,
       		ROW_NUMBER() OVER (ORDER BY ST_M(coords)) AS seq 
		FROM rawtracks_m rm
), pointsInside as (
        SELECT 
        	*, 
            seq - ROW_NUMBER() OVER (ORDER BY captureTime) AS grp
        FROM orderPoints
        WHERE ST_3DIntersects(location,(select hexagon_z from extruded_hexgrid h where hexagon_id = 1120 and fl_down = 25 and fl_up = 27)) 
), pointGroup as (
		SELECT 
		   min(captureTime) AS startTime, 
           max(CaptureTime) AS endTime, 
           array_agg(location) AS geom_array,
           count(*) as n_points,
           count(CASE WHEN turning = TRUE THEN 1 END) as n_turning,
           grp
		from pointsInside
		GROUP BY grp
), crossings as (
		select
			1.0 * n_turning / n_points as turning_ratio,
			startTime, 
		    endTime, 
		    ST_MakeLine(geom_array) AS line,
		    endTime - startTime as duration,
		    ST_Z(st_startpoint( ST_MakeLine(geom_array))) as alt_entry,
		    ST_Z(st_endpoint(( ST_MakeLine(geom_array)))) as alt_exit,
		    (select hexagon from hexgrid h where hexagon_id = 1120) as hex
		from pointGroup
), crossings_with_deltas as (
	  	select 
		    *,
			alt_exit - alt_entry as alt_gain
		from crossings
)
select
	*,
	alt_gain / duration as vertical_speed
from
	crossings_with_deltas
where duration > 100 and alt_gain > 0

select *
from extruded_hexgrid eh 
where hexagon_id = 1120

select * from extruded_hexgrid h where hexagon_id = 1120 
