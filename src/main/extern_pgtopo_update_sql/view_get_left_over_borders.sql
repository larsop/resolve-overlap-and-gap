
drop
	function if exists topo_update.get_left_over_borders(
	overlapgap_grid_ varchar,
		bb geometry
	);

create
	or REPLACE function topo_update.get_left_over_borders(
	overlapgap_grid_ varchar,
		_bb geometry
	) returns table
		(
  geo geometry(LineString, 4258)
		) language 'plpgsql' as $function$ 
declare 

command_string text;
bb_outer_geom geometry;

begin 
--TODO fix use dynamic sql
	command_string := FORMAT('RETURN QUERY SELECT 
	distinct lg3.geo 
	FROM topo_update.border_line_segments lg3
	where ST_IsValid(lg3.geo) and ST_Intersects(lg3.geo,_bb)
	and (
	ST_StartPoint(lg3.geo) && _bb or 
	(ST_EndPoint(lg3.geo) && _bb and NOT EXISTS (SELECT 1 FROM %s gt where ST_StartPoint(lg3.geo) && gt.geom))
	)',overlapgap_grid_);
	
    RAISE NOTICE 'zzzzzcommand_string %', command_string;
	
	RETURN QUERY SELECT 
	distinct lg3.geo 
	FROM topo_update.border_line_segments lg3
	where ST_IsValid(lg3.geo) and ST_Intersects(lg3.geo,_bb)
	and (
	ST_StartPoint(lg3.geo) && _bb or 
	(ST_EndPoint(lg3.geo) && _bb and NOT EXISTS (SELECT 1 FROM test_data.overlap_gap_input_t2_res_grid gt where ST_StartPoint(lg3.geo) && gt.geom))
	);

end $function$;



--select count(*) from topo_update.get_left_over_borders('0103000020E8640000010000000500000002A2DA7FD33B20418E3D023C3128594102A2DA7FD33B2041F2711E02B22F594102F3C73FD9872041F2711E02B22F594102F3C73FD98720418E3D023C3128594102A2DA7FD33B20418E3D023C31285941');

--create table topo_update.border_line_segments_result as (
--select   b1.id,                                                                                                                                                     
--ST_setSrid(ST_MakeLine(b1.point_geo,b2.point_geo),25832)::Geometry(LineString,25832) as geo
--from                                                                                       
--topo_update.border_line_segments b1,                                              
--topo_update.border_line_segments b2
--where b1.id != b2.id                                                                                           
--and ST_Distance(b1.geo,b2.geo) < 3                                                                            
--)
