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
  -- get the all the line parts based the bb_boundary_outer
  command_string := Format('CREATE temp table tmp_data_exterior_rings AS 
 	WITH rings AS (
 	SELECT ST_ExteriorRing((ST_DumpRings((st_dump(%3$s)).geom)).geom) as geom
 	FROM %1$s v
 	where ST_Intersects(v.%3$s,%2$L)
 	),
 	lines as 
    (
    select distinct (ST_Dump(geom)).geom as geom 
    from rings
    )
 	select 
     ST_Multi(ST_RemoveRepeatedPoints (l.geom,%4$s)) as geom
    from 
    rings l', 
 	_input_table_name, _bb, _input_table_geo_column_name, _topology_snap_tolerance,
 	_table_name_result_prefix||'_grid', ST_ExteriorRing(_bb)
 	);
  EXECUTE command_string;

   command_string := Format('INSERT INTO tmp_data_exterior_rings(geom)
    SELECT distinct geom FROM ( 
    SELECT 
 	ST_ExteriorRing(v.%2$s) AS geom FROM
    %1$s v,
    (select geom from tmp_data_exterior_rings r where ST_Intersects(r.geom,%3$L)) as r
 	where ST_Intersects(v.%2$s,r.geom)
    ) AS r',
 	_input_table_name, _input_table_geo_column_name,ST_ExteriorRing(_bb) );
  EXECUTE command_string;
  
  
  
 
  -- get the all the line parts based the bb_boundary_outer
  command_string := Format('CREATE TEMP TABLE tmp_data_all_lines AS SELECT r.geom, ST_NPoints(r.geom) AS npoints 
  FROM 
  (
  SELECT min(g.id) as min_id, l.geom FROM
  ( SELECT(ST_Dump(ST_Multi(ST_LineMerge(ST_union(rings.geom))))).geom AS geom 
    FROM tmp_data_exterior_rings rings
  ) AS l,
  %1$s g
  where ST_Intersects(l.geom,g.%2$s)
  group by l.geom
  ) AS r,
  %1$s g 
  WHERE g.%2$s = %3$L and ST_IsEmpty(r.geom) is false and r.min_id = g.id', 
  _table_name_result_prefix||'_grid', _input_table_geo_column_name, _bb);

  EXECUTE command_string;

  -- TODO user partion by
  
  UPDATE tmp_data_all_lines r 
  SET geom = ST_MakeValid(r.geom)
  WHERE ST_IsValid (r.geom) = FALSE; 

  
 
  command_string := Format('create index %1$s on tmp_data_all_lines using gist(geom)', 'idx1' || Md5(ST_AsBinary (_bb)));
  EXECUTE command_string;


  -- insert lines with more than max point
  EXECUTE Format('WITH long_lines AS 
    (DELETE FROM tmp_data_all_lines r where npoints >  %2$s and ST_Intersects(geom,%3$L) RETURNING geom) 
    INSERT INTO %1$s (geo) SELECT distinct (ST_dump(geom)).geom as geo from long_lines'
  ,_table_name_result_prefix||'_border_line_many_points', _max_point_in_line, ST_ExteriorRing(_bb) );

    
  -- 1 make line parts for inner box
  -- holds the lines inside bb_boundary_inner
  --#############################
  
  CREATE temp TABLE tmp_inner_lines_final_result AS
  WITH lr AS
  (DELETE FROM tmp_data_all_lines l WHERE  ST_CoveredBy(l.geom,_bb) and ST_IsValid(l.geom) RETURNING geom)
    SELECT lr.geom as geo, 0 AS line_type FROM lr;
  
  -- make line part for outer box, that contains the line parts will be added add the final stage when all the cell are done.
  --#############################
 
  CREATE temp TABLE tmp_boundary_line_parts AS
  WITH lr AS
  (DELETE FROM tmp_data_all_lines l WHERE  ST_Intersects(l.geom,ST_ExteriorRing(_bb)) and ST_IsValid(l.geom) RETURNING geom)
    SELECT lr.geom as geom FROM lr;
 
    -- log error lines
  EXECUTE Format('INSERT INTO %s (error_info, geo)
  SELECT %L AS error_info, r.geom as geo
  FROM tmp_data_all_lines r',
  _table_name_result_prefix||'_no_cut_line_failed','Failed to make valid input border line for tmp_boundary_lines_merged' );
    

  EXECUTE Format('INSERT INTO %s (geo, point_geo)
  SELECT r.geom as geo, NULL AS point_geo
  FROM tmp_boundary_line_parts r'
  ,_table_name_result_prefix||'_border_line_segments');

    

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

--drop table if exists test_tmp_simplified_border_lines_2 ;
--
--TRUNCATE topo_sr16_mdata_05.trl_2019_test_segmenter_mindredata_border_line_segments ;
--
--create table test_tmp_simplified_border_lines_2 as 
--(select g.* , ST_NPoints(geo) as num_points, ST_IsClosed(geo) as is_closed  
--FROM topo_update.get_simplified_border_lines('sl_kbj.trl_2019_test_segmenter_mindredata','geo',
--'0103000020E9640000010000000500000000000000F0BA0D4168C21B5B21B65A4100000000F0BA0D41D850E0F993B85A4100000000FA140E41D850E0F993B85A4100000000FA140E4168C21B5B21B65A4100000000F0BA0D4168C21B5B21B65A41'
--,'1','topo_sr16_mdata_05.trl_2019_test_segmenter_mindredata') g);
--
--alter table test_tmp_simplified_border_lines_2 add column id serial;

--\timing
--drop table if exists test_tmp_simplified_border_lines_1 ;
--
--TRUNCATE topo_sr16_mdata_05.trl_2019_test_segmenter_mindredata_border_line_segments ;
--
--create table test_tmp_simplified_border_lines_1 as 
--(select g.* , ST_NPoints(geo) as num_points, ST_IsClosed(geo) as is_closed  
--FROM topo_update.get_simplified_border_lines('sl_kbj.trl_2019_test_segmenter_mindredata','geo',
--'0103000020E9640000010000000500000000000000FA140E4154C404F028CC5A4100000000FA140E41C452C98E9BCE5A4100000000046F0E41C452C98E9BCE5A4100000000046F0E4154C404F028CC5A4100000000FA140E4154C404F028CC5A41'
--,'1','topo_sr16_mdata_05.trl_2019_test_segmenter_mindredata') g);
--
--alter table test_tmp_simplified_border_lines_1 add column id serial;
--
--drop table if exists test_tmp_simplified_border_lines_2 ;
--
--create table test_tmp_simplified_border_lines_2 as 
--(select g.* , ST_NPoints(geo) as num_points, ST_IsClosed(geo) as is_closed  
--FROM topo_update.get_simplified_border_lines('sl_kbj.trl_2019_test_segmenter_mindredata','geo',
--'0103000020E9640000010000000500000000000000F0BA0D4154C404F028CC5A4100000000F0BA0D41C452C98E9BCE5A4100000000FA140E41C452C98E9BCE5A4100000000FA140E4154C404F028CC5A4100000000F0BA0D4154C404F028CC5A41'
--,'1','topo_sr16_mdata_05.trl_2019_test_segmenter_mindredata') g);
--
--alter table test_tmp_simplified_border_lines_2 add column id serial;
	