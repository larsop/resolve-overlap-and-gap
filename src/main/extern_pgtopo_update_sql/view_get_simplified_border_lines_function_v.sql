CREATE OR REPLACE FUNCTION topo_update.get_simplified_border_lines (
_input_data resolve_overlap_data_input_type, 
--(_input_data).polygon_table_name varchar, -- The table to resolv, imcluding schema name
--(_input_data).polygon_table_geo_collumn varchar, -- The primary of the input table
--(_input_data).polygon_table_geo_collumn varchar, -- the name of geometry column on the table to analyze
--(_input_data).table_srid int, -- the srid for the given geo column on the table analyze
--(_input_data).utm boolean, 
_bb geometry, 
_topology_snap_tolerance float, 
_table_name_result_prefix varchar -- The topology schema name where we store store result sufaces and lines from the simple feature dataset,
)
  RETURNS TABLE (
    geo geometry,
    outer_border_line boolean
  )
  LANGUAGE 'plpgsql'
  AS $function$
DECLARE
  command_string text;
  
  -- This is used to sure that no lines can snap to each other between two cells
  -- The size wil the this value multiplied by _topology_snap_tolerance;
  -- TODO make this as parameter
 
  boundary_geom geometry;
  inner_boundary_geom geometry;
  
  -- TODO make paramter
  _max_point_in_line int = 10000;
  
  cell_id int;
  
  tmp_table_name varchar = 'tmp_data_all_lines' || Md5(ST_AsBinary (_bb));
  
BEGIN
	

  command_string := Format('select id from %1$s g WHERE g.%2$s = %3$L ',
  _table_name_result_prefix||'_grid', 
  (_input_data).polygon_table_geo_collumn, 
  _bb);
  EXECUTE command_string into cell_id;


  inner_boundary_geom := ST_Expand(_bb, (-2*_topology_snap_tolerance));
  
  boundary_geom := ST_MakePolygon ((
      SELECT ST_ExteriorRing (_bb) AS outer_ring), ARRAY (
       SELECT ST_ExteriorRing (inner_boundary_geom) AS inner_rings));

  
  RAISE NOTICE 'enter topo_update.get_simplified_border_lines with _bb %  ',  ST_AsText(_bb);
  
  --_bb := _bb;
  command_string := Format('CREATE TEMP TABLE %8$s AS WITH 
    lines_intersect_cell AS (
 	  SELECT distinct ST_ExteriorRing((ST_DumpRings((st_dump(%3$s)).geom)).geom) as geom
 	  FROM %1$s v
 	  where ST_Intersects(v.%3$s,%2$L)
 	),
    touch_lines_intersects AS (
      SELECT distinct ST_ExteriorRing(v.%3$s) AS geom
      FROM lines_intersect_cell l, 
      %1$s v
 	  WHERE ST_Intersects(v.%3$s,l.geom) and ST_Disjoint(v.%3$s,%2$L)
 	),
    all_lines AS (SELECT distinct r.geom as geom from 
     ( SELECT distinct (ST_Dump(ST_Multi(ST_LineMerge(ST_union(ST_SnapToGrid(l1.geom,%7$s)))))).geom as geom 
       from lines_intersect_cell l1 
       union 
       SELECT distinct (ST_Dump(ST_Multi(ST_LineMerge(ST_union(ST_SnapToGrid(l2.geom,%7$s)))))).geom as geom 
       from touch_lines_intersects l2
     ) as r
    ),
    line_parts AS ( 
      SELECT (ST_Dump(ST_Multi(ST_LineMerge(ST_union(ST_SnapToGrid(la.geom,%7$s)))))).geom
      FROM 
      all_lines la
    ),


    tmp_data_this_cell_lines AS (
      SELECT 
      case WHEN ST_IsValid (r.geom) = FALSE THEN 
       ST_MakeValid(r.geom)
       ELSE r.geom
      END as geom,
      min_cell_id
      FROM (
        SELECT min(g.id) as min_cell_id, l.geom 
        FROM
        line_parts l,
        %5$s g
        where ST_Intersects(l.geom,g.%3$s)
        group by l.geom
      ) AS r 
      WHERE ST_IsEmpty(r.geom) is false      
   
 	)
    select geom, ST_NPoints(geom) AS npoints,min_cell_id from tmp_data_this_cell_lines
    ',(_input_data).polygon_table_name, 
 	_bb, 
 	(_input_data).polygon_table_geo_collumn, 
 	_topology_snap_tolerance,
 	_table_name_result_prefix||'_grid', 
 	ST_ExteriorRing(_bb),
 	_topology_snap_tolerance/20, -- If snap to much here we may with not connected lines.
 	tmp_table_name
 	);
  EXECUTE command_string;

  
 
  command_string := Format('create index on %2$s using gist(geom)', 
  'idxtmp_data_all_lines_geom' || Md5(ST_AsBinary (_bb)),
  tmp_table_name);
  EXECUTE command_string;
  
  command_string := Format('create index on %2$s(min_cell_id)', 
  'idxtmp_data_all_lines_min_cell_id' || Md5(ST_AsBinary (_bb)),
  tmp_table_name );
  EXECUTE command_string;

   
  -- log error lines
  EXECUTE Format('WITH error_lines AS 
    (DELETE FROM %4$s r where r.min_cell_id = %1$s and ST_IsValid(r.geom) = false RETURNING geom) 
    INSERT INTO %2$s (error_info, geo)
    SELECT %3$L AS error_info, r.geom as geo
    FROM error_lines r',
    cell_id,
    _table_name_result_prefix||'_no_cut_line_failed',
    'Failed to make valid input border line for tmp_boundary_lines_merged',
 	tmp_table_name );
  
 

  -- insert lines with more than max point
  EXECUTE Format('WITH long_lines AS 
    (DELETE FROM %5$s r where npoints >  %2$s and ST_Intersects(geom,%3$L) and min_cell_id = %4$s RETURNING geom) 
    INSERT INTO %1$s (geo) SELECT distinct (ST_dump(geom)).geom as geo from long_lines',
    _table_name_result_prefix||'_border_line_many_points',
    _max_point_in_line, 
    ST_ExteriorRing(_bb), 
    cell_id,
 	tmp_table_name);

    
 
  -- make line part for outer box, that contains the line parts will be added add the final stage when all the cell are done.
 
  EXECUTE Format('INSERT INTO %s (geo, point_geo)
  SELECT l.geom as geo, NULL AS point_geo
  FROM %4$s l where ST_Intersects(geom,%2$L) and min_cell_id = %3$s and ST_IsValid(l.geom) = true ',
  _table_name_result_prefix||'_border_line_segments', 
  boundary_geom,
  cell_id,
  tmp_table_name);

  RAISE NOTICE 'done with %s' ,  tmp_table_name;

      
  -- return the result of inner geos to handled imediatly
  
  command_string := Format('SELECT * FROM (
    SELECT l.geom as geo, false as outer_border_line FROM
    %1$s l WHERE  ST_CoveredBy(l.geom,%2$L)
    union 
    SELECT ST_Intersection(ST_Union(
    ST_StartPoint(out.geom),
    ST_EndPoint(out.geom)),%3$L) as geo
    , true as outer_border_line FROM
    %1$s out where ST_Intersects(out.geom,%4$L)
  ) AS f',
  tmp_table_name, 
  inner_boundary_geom,
  _bb,
  boundary_geom);
  
  
  RETURN QUERY EXECUTE command_string;
END
$function$;


--drop table tmp_data_all_linesb54ff161031c2fb96c987afa0f0136c3;
--
--TRUNCATE test_topo_ar5_t2.ar5_2019_komm_flate_border_line_segments ;
--
--drop table if exists test_tmp_simplified_border_data ;
--drop table if exists test_tmp_simplified_border_data_lines ;
--drop table if exists test_tmp_simplified_border_data_point ;
--
--\timing
--create table test_tmp_simplified_border_data as 
--(select g.* , ST_NPoints(geo) as num_points, ST_IsClosed(geo) as is_closed  
--FROM topo_update.get_simplified_border_lines('org_ar5arsversjon.ar5_2019_komm_flate','geo',
--'0103000020A210000001000000050000004C2DA6334D08364084EB6B66108051404C2DA6334D08364059250B83E0865140EC132053A63F364059250B83E0865140EC132053A63F364084EB6B66108051404C2DA6334D08364084EB6B6610805140'::geometry
--,1e-05,'test_topo_ar5_t2.ar5_2019_komm_flate') g);
--
--create table test_tmp_simplified_border_data_lines as select geo from test_tmp_simplified_border_data where outer_border_line = false;
--alter table test_tmp_simplified_border_data_lines add column id serial;
--
--create table test_tmp_simplified_border_data_point as select geo from test_tmp_simplified_border_data where outer_border_line = TRUE;
--alter table test_tmp_simplified_border_data_point add column id serial;

--TRUNCATE topo_sr16_mdata_05.trl_2019_test_segmenter_mindredata_border_line_segments ;

--drop table if exists test_tmp_simplified_border_data ;
--drop table if exists test_tmp_simplified_border_data_lines ;
--drop table if exists test_tmp_simplified_border_data_point ;
--
--create table test_tmp_simplified_border_data as 
--(select g.* , ST_NPoints(geo) as num_points, ST_IsClosed(geo) as is_closed  
--FROM topo_update.get_simplified_border_lines('sl_kbj.trl_2019_test_segmenter_mindredata','geo',
--'0103000020E9640000010000000500000000000000F0BA0D410619B713D1C45A4100000000F0BA0D4176A77BB243C75A4100000000FA140E4176A77BB243C75A4100000000FA140E410619B713D1C45A4100000000F0BA0D410619B713D1C45A41'
--,'1','topo_sr16_mdata_05.trl_2019_test_segmenter_mindredata') g);
--
--create table test_tmp_simplified_border_data_lines as select geo from test_tmp_simplified_border_data where outer_border_line = false;
--alter table test_tmp_simplified_border_data_lines add column id serial;
--
--create table test_tmp_simplified_border_data_point as select geo from test_tmp_simplified_border_data where outer_border_line = TRUE;
--alter table test_tmp_simplified_border_data_point add column id serial;



--SELECT ST_AsText(ST_Expand(ST_GeomFromText('POLYGON((243550 7017284.3080504,243550 7019790.78879725,246431.25 7019790.78879725,246431.25 7017284.3080504,243550 7017284.3080504))'),-1));
---------------------------------------------------------------------------------------------------------------------------------------
--POLYGON((243551 7017285.3080504,243551 7019789.78879725,246430.25 7019789.78879725,246430.25 7017285.3080504,243551 7017285.3080504))



--
--CALL resolve_overlap_gap_single_cell(
--  'org_jm.jm_ukomm_flate','geo','figurid','test_topo_jm.jm_ukomm_flate',
--  'test_topo_jm',1e-05,4258,'false',
--  '(300,0,,0,140,120,240,0,35)',
--  'test_topo_jm.jm_ukomm_flate_job_list','test_topo_jm.jm_ukomm_flate_grid',
--  '0103000020A21000000100000005000000228D33786B142640EEFDA9DA05FB4D40228D33786B142640DD971EAD84014E40392509B3D1472640DD971EAD84014E40392509B3D1472640EEFDA9DA05FB4D40228D33786B142640EEFDA9DA05FB4D40'
--  ,1,1);
  

--CALL resolve_overlap_gap_single_cell(
--  'org_ar5arsversjon.ar5_2019_komm_flate','geo','sl_sdeid','test_topo_ar5_t2.ar5_2019_komm_flate',
--  'test_topo_ar5_t2',0.0001,4258,'false',
--  '(49,0,,0,140,120,240,0,35)',
--  'test_topo_ar5_t2.ar5_2019_komm_flate_job_list','test_topo_ar5_t2.ar5_2019_komm_flate_grid','0103000020A21000000100000005000000B01528D072332B401477E5B53C9F5040B01528D072332B40BEEA23EFDCAC504032B00F4ED7102C40BEEA23EFDCAC504032B00F4ED7102C401477E5B53C9F5040B01528D072332B401477E5B53C9F5040',1,1)
--  

--  drop table if exists test_topo_ar5_t4.test_tmp_simplified_border_data ;
--  drop table if exists test_topo_ar5_t4.test_tmp_simplified_border_data_lines ;
--  drop table if exists test_topo_ar5_t4.test_tmp_simplified_border_data_point ;
--  drop table if exists test_topo_ar5_t4.test_tmp_simplified_border_data_lines_exp;
--  
--  \timing
--  create table test_topo_ar5_t4.test_tmp_simplified_border_data as 
--  (select g.* , ST_NPoints(geo) as num_points, ST_IsClosed(geo) as is_closed  
--  FROM topo_update.get_simplified_border_lines('org_ar5arsversjon.ar5_2019_komm_flate','geo',
--  '0103000020A21000000100000005000000EC132053A63F3640B256119E5F505140EC132053A63F3640063E8E10A06B51406EAE07D10A1D3740063E8E10A06B51406EAE07D10A1D3740B256119E5F505140EC132053A63F3640B256119E5F505140'::geometry
--  ,1e-06,'test_topo_ar5_t4.ar5_2019_komm_flate') g);
--  
--  create table test_topo_ar5_t4.test_tmp_simplified_border_data_lines as select geo from test_topo_ar5_t4.test_tmp_simplified_border_data where outer_border_line = false;
--  alter table test_topo_ar5_t4.test_tmp_simplified_border_data_lines add column id serial;
--  
--  create table test_topo_ar5_t4.test_tmp_simplified_border_data_lines_exp as select ST_Expand(geo,1e-06) from test_topo_ar5_t4.test_tmp_simplified_border_data where outer_border_line = false;
--  alter table test_topo_ar5_t4.test_tmp_simplified_border_data_lines_exp add column id serial;
--  
--  
--  create table test_topo_ar5_t4.test_tmp_simplified_border_data_point as select geo from test_topo_ar5_t4.test_tmp_simplified_border_data where outer_border_line = TRUE;


