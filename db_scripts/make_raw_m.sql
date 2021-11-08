create table rawtracks_m as
	with _t1 as (
			select 
				track_id,
				altitude,
				coords,
				FLOOR((extract(epoch from time_begin) + extract(epoch from time_end)) / 2) as point_time,
				first_value(altitude) over
					(order by time_begin rows between 1 preceding and current row) as previous_alt,
				first_value(coords) over (order by time_begin rows between 1 preceding and current row) as coord_1
			from rawtracks r 
		), _t2 as (
			select 
				*,
				first_value(point_time) over (order by point_time rows between 5 preceding and current row) as first_time,
				first_value(altitude) over (order by point_time rows between 5 preceding and current row) as first_alt,
				first_value(point_time) over
					(order by point_time rows between 1 preceding and current row) as previous_time,
				st_azimuth(coords, coord_1) as azimuth
			from _t1
		), _t3 as (
			select
				*,	
				(altitude - first_alt) / (point_time - first_time) as vertical_speed_ma5,
				stddev_samp(azimuth) over (order by point_time rows between 5 preceding and current row) as azimuth_sdev,
				(altitude - previous_alt) / (point_time - previous_time) as instant_vario
			from _t2
			where point_time - first_time != 0 and point_time - previous_time != 0
		)
	select track_id, 
		   instant_vario,
		   avg(instant_vario) over (order by point_time rows between 5 preceding and current row) as avg_vario_5s,
		   st_setsrid(st_makepoint(ST_X(coords), ST_Y(coords), altitude, point_time), 4326) as coords, 
		   azimuth_sdev > 0.7 as turning
	from _t3

create table lines (id serial primary key, geom geometry)

insert into lines (geom)  
select st_setsrid(st_makeline(coords), 4326) as geom
from rawtracks_m rm 
group by track_id 


	with _t1 as (
			select 
				track_id,
				altitude,
				coords,
				FLOOR((extract(epoch from time_begin) + extract(epoch from time_end)) / 2) as point_time,
				first_value(altitude) over
					(order by time_begin rows between 1 preceding and current row) as previous_alt,
				first_value(coords) over (order by time_begin rows between 1 preceding and current row) as coord_1
			from rawtracks r 
		), _t2 as (
			select 
				*,
				first_value(point_time) over (order by point_time rows between 5 preceding and current row) as first_time,
				first_value(altitude) over (order by point_time rows between 5 preceding and current row) as first_alt,
				first_value(point_time) over
					(order by point_time rows between 1 preceding and current row) as previous_time,
				st_azimuth(coords, coord_1) as azimuth
			from _t1
		), _t3 as (
			select
				*,	
				(altitude - first_alt) / (point_time - first_time) as vertical_speed_ma5,
				stddev_samp(azimuth) over (order by point_time rows between 5 preceding and current row) as azimuth_sdev,
				(altitude - previous_alt) / (point_time - previous_time) as instant_vario
			from _t2
			where point_time - first_time != 0 and point_time - previous_time != 0
		)
	select track_id, 
		   instant_vario,
		   avg(instant_vario) over (order by point_time rows between 5 preceding and current row) as avg_vario_5s,
		   st_setsrid(st_makepoint(ST_X(coords), ST_Y(coords), altitude, point_time), 4326) as coords, 
		   azimuth_sdev > 0.7 as turning
	from _t3