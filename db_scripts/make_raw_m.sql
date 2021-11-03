create table rawtracks_m as
select track_id, st_setsrid(
	ST_MakePoint( 
		ST_X(r.coords), 
		ST_Y(r.coords),
		r.altitude, 
		FLOOR((extract(epoch from time_begin) + extract(epoch from time_end)) / 2))
		, 4326) as coords
from rawtracks r 

create table lines (id serial primary key, geom geometry)

insert into lines (geom)  
select st_setsrid(st_makeline(coords), 4326) as geom
from rawtracks_m rm 
group by track_id 