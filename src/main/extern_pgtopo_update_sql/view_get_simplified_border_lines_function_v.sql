CREATE OR REPLACE FUNCTION topo_update.get_simplified_border_lines (
_input_table_name varchar, 
_input_table_geo_column_name varchar, 
_bb geometry, 
_topology_snap_tolerance float, 
_table_name_result_prefix varchar -- The topology schema name where we store store result sufaces and lines from the simple feature dataset,
)
  RETURNS TABLE (
    json text,
    geo geometry,
    objectid integer,
    line_type integer)
  LANGUAGE 'plpgsql'
  AS $function$
DECLARE
  command_string text;
  
  -- This is used to sure that no lines can snap to each other between two cells
  -- The size wil the this value multiplied by _topology_snap_tolerance;
  -- TODO make this as parameter
  cell_boundary_tolerance_with_multi real = 6;
  
  -- This is the boundary geom that contains lines pieces that will added after each single cell is done
  boundary_geom geometry;
  bb_boundary_inner geometry;
  bb_boundary_outer geometry;
  
  -- This is is a box used to make small glue lines. This lines is needed to make that we don't do any snap out side our own cell
  bb_inner_glue_geom geometry;
  boundary_glue_geom geometry;

  -- The inpux
  boundary_with float = _topology_snap_tolerance*1.1;
  glue_boundary_with float = _topology_snap_tolerance * cell_boundary_tolerance_with_multi;
  overlap_width_inner float = 0;
  try_update_invalid_rows int;
  
  _max_point_in_line int = 10000;
  
BEGIN
	
	
  -- buffer in to work with geom that lines are only meter from the border
  -- will only work with polygons
  -- make the the polygon that contains lines	that will be added in the post process
  bb_boundary_outer := ST_Expand (_bb, boundary_with);
  bb_boundary_inner := ST_Expand (_bb, (boundary_with * - 1));
  boundary_geom := ST_MakePolygon ((
      SELECT ST_ExteriorRing (ST_Expand (_bb, boundary_with)) AS outer_ring), ARRAY (
        SELECT ST_ExteriorRing (ST_Expand (_bb, ((boundary_with + overlap_width_inner) * - 1))) AS inner_rings));
        
  -- make the the polygon that contains lines is used a glue lines
  bb_inner_glue_geom := ST_Expand (_bb, ((boundary_with + glue_boundary_with) * - 1));
  boundary_glue_geom := ST_MakePolygon ((
      SELECT ST_ExteriorRing (bb_boundary_inner) AS outer_ring), ARRAY (
        SELECT ST_ExteriorRing (bb_inner_glue_geom) AS inner_rings));
  -- holds the lines inside bb_boundary_inner
  DROP TABLE IF EXISTS tmp_data_all_lines;
  -- get the all the line parts based the bb_boundary_outer
  command_string := Format('CREATE temp table tmp_data_all_lines AS 
 	WITH rings AS (
 	SELECT ST_ExteriorRing((ST_DumpRings((st_dump(%3$s)).geom)).geom) as geom
 	FROM %1$s v
 	where ST_Intersects(v.%3$s,%2$L)
 	),
 	lines as 
    (
    select distinct (ST_Dump(geom)).geom as geom 
    from rings
    ),
    my_lines as 
    (
    select min(g.id) as min_id, l.geom 
    from lines l,
    %5$s g
    where ST_Intersects(l.geom,g.%3$s)
    group by l.geom 
    )

 	select 
     ST_Multi(ST_RemoveRepeatedPoints (l.geom,%4$s)) as geom, 
     ST_NPoints(l.geom) as npoints,
     (ST_CoveredBy(l.geom,%6$L) = false) as touch_outside
    from 
    my_lines l,
    %5$s g
    where g.%3$s = %2$L and ST_IsEmpty(l.geom) is false and l.min_id = g.id
    ', 
 	_input_table_name, _bb, _input_table_geo_column_name, _topology_snap_tolerance,
 	_table_name_result_prefix||'_grid', bb_boundary_inner
 	);
  EXECUTE command_string;
  command_string := Format('create index %1$s on tmp_data_all_lines using gist(geom)', 'idx1' || Md5(ST_AsBinary (_bb)));
  EXECUTE command_string;

  

  -- insert lines with more than max point
  EXECUTE Format('WITH long_lines AS 
    (DELETE FROM tmp_data_all_lines r where npoints >  %2$s and touch_outside = true RETURNING geom) 
    INSERT INTO %1$s (geo) SELECT distinct (ST_dump(geom)).geom as geo from long_lines'
  ,_table_name_result_prefix||'_border_line_many_points', _max_point_in_line);

    
  -- 1 make line parts for inner box
  -- holds the lines inside bb_boundary_inner
  --#############################
  DROP TABLE IF EXISTS tmp_inner_lines_final_result;
  CREATE temp TABLE tmp_inner_lines_final_result AS (
    SELECT (ST_Dump(ST_Multi(ST_LineMerge(ST_union(rings.geom))))).geom as geo,
 --   SELECT (ST_Dump (ST_Intersection (rings.geom, bb_inner_glue_geom ) ) ).geom AS geo,
    0 AS line_type
    FROM tmp_data_all_lines as rings where touch_outside = false 
  );
    
  
  -- make line part for outer box, that contains the line parts will be added add the final stage when all the cell are done.
  --#############################
  DROP TABLE IF EXISTS tmp_boundary_line_parts;
  CREATE temp TABLE tmp_boundary_line_parts AS (
    SELECT (ST_Dump (rings.geom) ).geom AS geo
    FROM tmp_data_all_lines AS rings where touch_outside = true );
 

     -- Try to fix invalid lines
  UPDATE tmp_boundary_line_parts r 
  SET geo = ST_MakeValid(r.geo)
  WHERE ST_IsValid (r.geo) = FALSE; 
  GET DIAGNOSTICS try_update_invalid_rows = ROW_COUNT;
  IF  try_update_invalid_rows > 0 THEN
    -- log error lines
    EXECUTE Format('INSERT INTO %s (error_info, geo)
    SELECT %L AS error_info, r.geo
    FROM tmp_boundary_line_parts r
    WHERE ST_IsValid (r.geo) = FALSE',_table_name_result_prefix||'_no_cut_line_failed','Failed to make valid input border line for tmp_boundary_lines_merged' );
    
    EXECUTE Format('INSERT INTO %s (geo, point_geo)
    SELECT r.geo, NULL AS point_geo
    FROM (
    SELECT r.geo
    FROM tmp_boundary_line_parts r
    WHERE ST_IsValid (r.geo) IS TRUE) AS r',_table_name_result_prefix||'_border_line_segments');

  ELSE

    EXECUTE Format('INSERT INTO %s (geo, point_geo)
    SELECT r.geo, NULL AS point_geo
    FROM (
    SELECT r.geo
    FROM tmp_boundary_line_parts r) AS r',_table_name_result_prefix||'_border_line_segments');

  END IF; 

    

  -- log error lines
  
  -- return the result of inner geos to handled imediatly
  RETURN QUERY
  SELECT *
  FROM (
    SELECT '{"type": "Feature",' || '"geometry":' || ST_AsGeoJSON (lg3.geo, 10, 2)::json || ',' || '"properties":' || Row_to_json((
        SELECT l FROM (
            SELECT NULL AS "oppdateringsdato") AS l)) || '}' AS json, lg3.geo, 1 AS objectid, lg3.line_type
    FROM (
      SELECT l1.geo, l1.line_type
      FROM tmp_inner_lines_final_result  l1
      WHERE ST_IsValid (l1.geo)) AS lg3) AS f;
END
$function$;

--drop table if exists test_tmp_simplified_border_lines_1;
--
--create table test_tmp_simplified_border_lines_1 as 
--(select g.* , ST_NPoints(geo) as num_points, ST_IsClosed(geo) as is_closed  
--FROM topo_update.get_simplified_border_lines('sl_esh.ar50_utvikling_flate','geo',
--'0103000020E9640000010000000500000000000000B0A6074100000000F9F05A4100000000B0A607410000008013F75A4100000000006A08410000008013F75A4100000000006A084100000000F9F05A4100000000B0A6074100000000F9F05A41'
--,'1','test_topo_ar50_t11.ar50_utvikling_flate') g);
--alter table test_tmp_simplified_border_lines_1 add column id serial;
--
--drop if exists table test_tmp_simplified_border_lines_2;
--
--create table test_tmp_simplified_border_lines_2 as 
--(select g.* , ST_NPoints(geo) as num_points, ST_IsClosed(geo) as is_closed  
--FROM topo_update.get_simplified_border_lines('sl_esh.ar50_utvikling_flate','geo',
--'0103000020E9640000010000000500000000000000B0A60741000000C0EBED5A4100000000B0A6074100000000F9F05A41000000005808084100000000F9F05A410000000058080841000000C0EBED5A4100000000B0A60741000000C0EBED5A41'
--,'1','test_topo_ar50_t11.ar50_utvikling_flate') g);
--
--alter table test_tmp_simplified_border_lines_2 add column id serial;
