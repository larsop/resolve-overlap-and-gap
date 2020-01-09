drop
	function if exists topo_update.get_simplified_border_lines(
		input_table_name varchar,
		input_table_geo_column_name varchar,
		_bb geometry,
		_snap_tolerance float8,
		_do_chaikins boolean
	);

create
	or REPLACE function topo_update.get_simplified_border_lines(
		input_table_name varchar,
		input_table_geo_column_name varchar,
		_bb geometry,
		_snap_tolerance float8,
		_do_chaikins boolean
	) returns table
		(
  json text, 
  geo geometry, 
  objectid integer,
  line_type integer
		) language 'plpgsql' as $function$ 
declare 

command_string text;

-- This is the boundary geom that contains lines pieces that will added after each single cell is done
boundary_geom geometry;
bb_boundary_inner geometry;
bb_boundary_outer geometry;

-- This is is a box used to make small glue lines. This lines is needed to make that we don't do any snap out side our own cell
bb_inner_glue_geom geometry;
boundary_glue_geom geometry;


-- TODO add as parameter
--boundary_with real = 1.5;
--glue_boundary_with real = 0.5;
--overlap_width_inner real = 1;

boundary_with real = _snap_tolerance * 1.5;
glue_boundary_with real = _snap_tolerance * 0.5;
overlap_width_inner real = _snap_tolerance;

begin 
	-- buffer in to work with geom that lines are only meter from the border
	-- will only work with polygons

	-- make the the polygon that contains lines	that will be added in the post process 
	bb_boundary_outer := ST_Expand(_bb,boundary_with);
	bb_boundary_inner := ST_Expand(_bb,(boundary_with*-1));
	boundary_geom := ST_MakePolygon(
	(SELECT ST_ExteriorRing(ST_Expand(_bb,boundary_with)) AS outer_ring),  
	ARRAY(SELECT ST_ExteriorRing(ST_Expand(_bb,((boundary_with+overlap_width_inner)*-1))) AS inner_rings)
	);

	-- make the the polygon that contains lines is used a glue lines 
	bb_inner_glue_geom := ST_Expand(_bb,((boundary_with+glue_boundary_with)*-1));
	boundary_glue_geom := ST_MakePolygon(
	(SELECT ST_ExteriorRing(bb_boundary_inner) AS outer_ring),  
	ARRAY(SELECT ST_ExteriorRing(bb_inner_glue_geom) AS inner_rings)
	);


	-- holds the lines inside bb_boundary_inner
	drop table if exists tmp_data_all_lines;

	-- get the all the line parts based the bb_boundary_outer
	command_string := format('CREATE temp table tmp_data_all_lines AS 
	WITH rings AS (
	SELECT ST_ExteriorRing((ST_DumpRings((st_dump(%3$s)).geom)).geom) as geom
	FROM %1$s v
	where ST_Intersects(v.%3$s,%2$L)
	),
	lines as (select distinct (ST_Dump(geom)).geom as geom from rings)
	select geom from lines 
	where  ST_IsEmpty(geom) is false'
	,input_table_name
	,bb_boundary_outer
	,input_table_geo_column_name);
	EXECUTE command_string;

	command_string := format('create index %1$s on tmp_data_all_lines using gist(geom)', 'idx1'||md5(ST_AsBinary(_bb)));
	EXECUTE command_string;

	-- 1 make line parts for inner box
	-- holds the lines inside bb_boundary_inner
	--#############################
	drop table if exists tmp_inner_line_parts;
	create temp table  tmp_inner_line_parts as (Select (ST_Dump(ST_Intersection(rings.geom,bb_inner_glue_geom))).geom as geo from tmp_data_all_lines as rings);
	
	drop table if exists tmp_inner_lines_merged;
	create temp table tmp_inner_lines_merged as (
		select (ST_Dump(ST_LineMerge(ST_Union(lg.geo)))).geom as geo,0 as line_type
		from tmp_inner_line_parts as lg
	);

	IF (_snap_tolerance > 0 and _do_chaikins is true) THEN
		update tmp_inner_lines_merged lg
		set geo = ST_simplifyPreserveTopology(topo_update.chaikinsAcuteAngle(lg.geo,120,240),_snap_tolerance);
		RAISE NOTICE ' do snap_tolerance % and do do_chaikins %', _snap_tolerance, _do_chaikins;
		
		-- TODO send paratmeter if this org data or not. _do_chaikins

--		insert into tmp_inner_lines_merged(geo,line_type)
--		SELECT e1.geom as geo , 2 as line_type from  topo_ar5_forest_sysdata.edge e1
--		where e1.geom && bb_inner_glue_geom;

	ELSE IF (_snap_tolerance > 0) THEN
		update tmp_inner_lines_merged lg
		set geo = ST_simplifyPreserveTopology(lg.geo,_snap_tolerance);
		RAISE NOTICE ' do snap_tolerance % and not do do_chaikins %', _snap_tolerance, _do_chaikins;
	END IF;

--	update tmp_inner_lines_merged lg
--	set geo = ST_Segmentize(geo, 1);

	END IF;
	-- log error lines
	insert into topo_update.no_cut_line_failed(error_info,geo) 
	select 'Failed to make valid input line ' as error_info, r.geo
	from tmp_inner_lines_merged r where ST_IsValid(r.geo) = false;

	
	-- make linns for glue parts.
	--#############################
	drop table if exists tmp_boundary_line_type_parts;
	create temp table  tmp_boundary_line_type_parts as (Select (ST_Dump(ST_Intersection(rings.geom,boundary_glue_geom))).geom as geo from tmp_data_all_lines as rings);

	drop table if exists tmp_boundary_line_types_merged;
	create temp table tmp_boundary_line_types_merged as (
		select r.geo, 1 as line_type from (
			select (ST_Dump(ST_LineMerge(ST_Union(lg.geo)))).geom as geo
			from tmp_boundary_line_type_parts as lg
		) r
	);

	insert into tmp_inner_lines_merged(geo,line_type)
	select r.geo, 1 as line_type from tmp_boundary_line_types_merged r where ST_ISvalid(r.geo);

	-- log error lines
	insert into topo_update.no_cut_line_failed(error_info,geo) 
	select 'Failed to make valid input border line ' as error_info, r.geo
	from tmp_boundary_line_types_merged r where ST_IsValid(r.geo) = false;

	-- make line part for outer box, that contains the line parts will be added add the final stage when all the cell are done.
	--#############################
	drop table if exists tmp_boundary_line_parts;
	create temp table  tmp_boundary_line_parts as (Select (ST_Dump(ST_Intersection(rings.geom,boundary_geom))).geom as geo from tmp_data_all_lines as rings);
	
	drop table if exists tmp_boundary_lines_merged;
	create temp table tmp_boundary_lines_merged as (
		select (ST_Dump(ST_LineMerge(ST_Union(lg.geo)))).geom as geo
		from tmp_boundary_line_parts as lg
	);

	insert into topo_update.border_line_segments(geo,point_geo) 
	select
	r.geo, 
	null as point_geo
	FROM (
		select r.geo from tmp_boundary_lines_merged r 
		where ST_IsValid(r.geo) is true
	) as r;

	-- log error lines
	insert into topo_update.no_cut_line_failed(error_info,geo) 
	select 'Failed to make valid input border line ' as error_info, r.geo
	from tmp_boundary_lines_merged r where ST_IsValid(r.geo) = false;



	-- return the result of inner geos to handled imediatly 
	RETURN QUERY 
	SELECT 
	  * 
	FROM 
  	(
    SELECT 
      '{"type": "Feature",' || '"geometry":' || ST_AsGeoJSON(lg3.geo, 10, 2):: json || ',' || '"properties":' || row_to_json(
        (
          SELECT 
            l 
          FROM 
            (
              SELECT 
                null as "oppdateringsdato"
            ) As l
        )
      ) || '}' as json, 
      lg3.geo, 
      1 as objectid,
      lg3.line_type
      FROM (
      select l1.geo, l1.line_type 
      FROM tmp_inner_lines_merged l1
      where ST_IsValid(l1.geo)
      ) as lg3
    ) As f;


end $function$;


--truncate table topo_update.border_line_segments;

--select count(g.*) FROM topo_update.get_simplified_border_lines('tmp_sf_ar5_forest_input.not_selected_forest_area','wkb_geometry','0103000020E8640000010000000500000000000000E4152141000000C0E313594100000000E4152141000000000317594100000000CA2F2141000000000317594100000000CA2F2141000000C0E313594100000000E4152141000000C0E3135941',1,false) g;
--select count(g.*) FROM topo_update.get_simplified_border_lines('tmp_sf_ar5_forest_input.not_selected_forest_area','wkb_geometry','0103000020E8640000010000000500000000000000E4152141000000C0E313594100000000E4152141000000000317594100000000CA2F2141000000000317594100000000CA2F2141000000C0E313594100000000E4152141000000C0E3135941',1,true) g;

--drop table sss_1;
--create table sss_1 as (select g.* FROM topo_update.get_simplified_border_lines('tmp_sf_ar5_forest_input.not_selected_forest_area','wkb_geometry','0103000020E8640000010000000500000000000000E4152141000000C0E313594100000000E4152141000000000317594100000000CA2F2141000000000317594100000000CA2F2141000000C0E313594100000000E4152141000000C0E3135941',1,true) as g);
--select * from sss_1;

--SELECT distinct geo from topo_update.border_line_segments;
