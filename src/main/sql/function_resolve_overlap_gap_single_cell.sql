CREATE OR REPLACE PROCEDURE resolve_overlap_gap_single_cell (
  input_table_name character varying, 
  input_table_geo_column_name character varying, 
  input_table_pk_column_name character varying, 
  _topology_schema_name varchar, -- The topology schema name where we store store result sufaces and lines from the simple feature dataset,
  _topology_name character varying, 
  _srid int,
  _utm boolean,
  _simplify_tolerance double precision, 
  _snap_tolerance double precision, 
  _do_chaikins boolean, 
  _min_area_to_keep float,
  _job_list_name character varying, 
  overlapgap_grid varchar,
  bb geometry,
  inside_cell_data boolean
)
LANGUAGE plpgsql
AS $$
DECLARE
  border_topo_info topo_update.input_meta_info;
  -- holds dynamic sql to be able to use the same code for different
  command_string text;
  added_rows int = 0;
  start_time timestamp WITH time zone;
  done_time timestamp WITH time zone;
  used_time real;
  start_remove_small timestamp WITH time zone;
  is_done integer = 0;
  area_to_block geometry;
  num_boxes_intersect integer;
  num_boxes_free integer;
  num_rows_removed integer;
  box_id integer;
  face_table_name varchar;
  -- This is used when adding lines hte tolrannce is different when adding lines inside and box and the border;
  snap_tolerance_fixed float =      _snap_tolerance;
  glue_snap_tolerance_fixed float = _snap_tolerance/10000;
  
  
BEGIN
  RAISE NOTICE 'start wwork at timeofday:% for layer %, with inside_cell_data %', Timeofday(), _topology_name || '_', inside_cell_data;
  -- check if job is done already
  command_string := Format('select count(*) from %s as gt, %s as done
   where gt.cell_geo && ST_PointOnSurface(%3$L) and gt.id = done.id', _job_list_name, _job_list_name || '_donejobs', bb);
  EXECUTE command_string INTO is_done;
  IF is_done = 1 THEN
    RAISE NOTICE 'Job is_done for  : %', ST_astext (bb);
    RETURN;
  END IF;
  start_time := Clock_timestamp();
  RAISE NOTICE 'enter work at timeofday:% for layer %, with inside_cell_data %', Timeofday(), _topology_name || '_' || box_id, inside_cell_data;
  IF bb IS NULL AND input_table_name IS NOT NULL THEN
    command_string := Format('select ST_Envelope(ST_Collect(geo)) from %s', input_table_name);
    EXECUTE command_string INTO bb;
  END IF;
  -- get area to block and set
  area_to_block := resolve_overlap_gap_block_cell(input_table_name, input_table_geo_column_name, input_table_pk_column_name, _job_list_name,
    bb);
  RAISE NOTICE 'area to block:% ', area_to_block;
  border_topo_info.snap_tolerance := _simplify_tolerance;
  --      --border_topo_info.border_layer_id = 317;
  RAISE NOTICE 'start work at timeofday:% for layer %, with inside_cell_data %', Timeofday(), _topology_name || '_' || box_id, inside_cell_data;
  IF inside_cell_data THEN
    command_string := Format('select id from %1$s where cell_geo = %2$L', _job_list_name, bb);
    RAISE NOTICE '% ', command_string;
    EXECUTE command_string INTO box_id;
    border_topo_info.topology_name := _topology_name || '_' || box_id;
    RAISE NOTICE 'use border_topo_info.topology_name %', border_topo_info.topology_name;
      
--    IF ((SELECT Count(*) FROM topology.topology WHERE name = border_topo_info.topology_name) = 1) THEN
--      EXECUTE Format('SELECT topology.droptopology(%s)', Quote_literal(border_topo_info.topology_name));
--    END IF;
    --drop this schema in case it exists
--    EXECUTE Format('DROP SCHEMA IF EXISTS %s CASCADE', border_topo_info.layer_schema_name);

    PERFORM topology.CreateTopology (border_topo_info.topology_name, _srid, snap_tolerance_fixed);
    EXECUTE Format('ALTER table %s.edge_data set unlogged', border_topo_info.topology_name);
    EXECUTE Format('ALTER table %s.node set unlogged', border_topo_info.topology_name);
    EXECUTE Format('ALTER table %s.face set unlogged', border_topo_info.topology_name);
    EXECUTE Format('ALTER table %s.relation set unlogged', border_topo_info.topology_name);
    -- get the siple feature data both the line_types and the inner lines.
    -- the boundery linnes are saved in a table for later usage
    DROP TABLE IF EXISTS tmp_simplified_border_lines;
    command_string := Format('create temp table tmp_simplified_border_lines as (select g.* FROM topo_update.get_simplified_border_lines(%L,%L,%L,%L,%L,%L) g)', 
    input_table_name, input_table_geo_column_name, bb, _simplify_tolerance, _do_chaikins,_topology_schema_name);
    RAISE NOTICE 'command_string %', command_string;
    EXECUTE command_string;
    -- add the glue line with no/small tolerance
    border_topo_info.snap_tolerance := glue_snap_tolerance_fixed;
    command_string := Format('SELECT topo_update.create_nocutline_edge_domain_obj_retry(json::Text, %L) 
                 from tmp_simplified_border_lines g where line_type = 1', border_topo_info);
    RAISE NOTICE 'command_string %', command_string;
    EXECUTE command_string;
    -- add lines aleday added som we get he same break
    border_topo_info.snap_tolerance := snap_tolerance_fixed;
    command_string := Format('SELECT topo_update.create_nocutline_edge_domain_obj_retry(json::Text, %L) 
                 from tmp_simplified_border_lines g where line_type = 2', border_topo_info);
    --              RAISE NOTICE 'command_string %' , command_string;
    --              EXECUTE command_string;
    -- add to glue lijnes to the finale result
    -- NB We have to use snap less that one meter to avpid snapping across cell
    command_string := Format('SELECT topo_update.add_border_lines(%3$L,r.geom,%1$s) FROM (
                 SELECT geom from  %2$s.edge ) as r', snap_tolerance_fixed, border_topo_info.topology_name, _topology_name);
    -- using the input tolreance for adding
    border_topo_info.snap_tolerance := snap_tolerance_fixed;
    command_string := Format('SELECT topo_update.create_nocutline_edge_domain_obj_retry(json::Text, %L) 
                 from tmp_simplified_border_lines g where line_type = 0', border_topo_info);
    RAISE NOTICE 'command_string %', command_string;
    EXECUTE command_string;
    face_table_name = border_topo_info.topology_name || '.face';
    start_remove_small := Clock_timestamp();
    RAISE NOTICE 'Start clean small polygons for face_table_name % at %', face_table_name, Clock_timestamp();
    -- remove small polygons in temp
    num_rows_removed := topo_update.do_remove_small_areas_no_block (border_topo_info.topology_name, _min_area_to_keep, face_table_name,bb,_utm);
    used_time := (Extract(EPOCH FROM (Clock_timestamp() - start_remove_small)));
    RAISE NOTICE 'Removed % clean small polygons for face_table_name % at % used_time: %', num_rows_removed, face_table_name, Clock_timestamp(), used_time;
    -- get valid faces and thise eges that touch out biedery
    -------------- this does not work
    command_string := Format('
 WITH lg as (
 SELECT 
 topology.ST_GetFaceGeometry(%2$s,lg.face_id) as geom 
 from  %3$s.face lg where ST_Area(mbr,false) > 100
 ),
 lg2 as (
 select (ST_DumpRings((st_dump(lg.geom)).geom)).geom from lg where lg.geom is not null and ST_area(lg.geom) > 49
 ),
 r as (SELECT (ST_Dump(ST_LineMerge(ST_ExteriorRing(lg2.geom)))).geom
 from lg2)
 SELECT topo_update.add_border_lines(%4$L,r.geom,%1$s) FROM r', _snap_tolerance, Quote_literal(border_topo_info.topology_name), border_topo_info.topology_name, _topology_name);
    --              RAISE NOTICE 'command_string %' , command_string;
    --              EXECUTE command_string;
    -- add to finale result
    ------- this does not work
    command_string := Format('SELECT topo_update.add_border_lines(%4$L,r.geom,%1$s) FROM (
                 SELECT geom from  %2$s.edge where ST_DWithin(geom,%3$L,0.6) is true) as r', _snap_tolerance, border_topo_info.topology_name, ST_ExteriorRing (bb), _topology_name);
    --              RAISE NOTICE 'command_string %' , command_string;
    --              EXECUTE command_string;
    -- add to finale result
    --TODO make a test for final result or not
    --if (_do_chaikins is false) THEN
    --              command_string := format('SELECT topo_update.add_border_lines(r.geom,%1$s) FROM (
    --              SELECT geom from  %2$s.edge) as r'
    --              , _snap_tolerance, border_topo_info.topology_name,ST_ExteriorRing(bb));
    --              RAISE NOTICE 'command_string %' , command_string;
    --              EXECUTE command_string;
    --ELSE
    --              ------- this does not work
    --              command_string := format('SELECT topo_update.add_border_lines(r.geom,%1$s) FROM (
    --              SELECT e1.geom from  %2$s.edge e1, tmp_sf_ar5_forest_input.not_selected_forest_area p
    --where ST_CoveredBy(p.wkb_geometry,%3$L) and ST_CoveredBy(e1.geom,p.wkb_geometry) is false) as r'
    --              , _snap_tolerance, border_topo_info.topology_name,bb);
    --              RAISE NOTICE 'command_string %' , command_string;
    --              EXECUTE command_string;
    --
    --END IF;
    command_string := Format('SELECT topo_update.add_border_lines(%4$L,r.geom,%1$s) FROM (
                 SELECT geom from  %2$s.edge) as r', _snap_tolerance, border_topo_info.topology_name, ST_ExteriorRing (bb), _topology_name);
    RAISE NOTICE 'command_string %', command_string;
    EXECUTE command_string;
    -- analyze table topo_ar5_forest_sysdata.face;
    -- remove small polygons in main table
    --              num_rows_removed := topo_update.do_remove_small_areas_no_block(border_topo_info.topology_name,'topo_ar5_forest_sysdata.face' ,'mbr','face_id',_job_list_name ,bb );
    --              RAISE NOTICE 'Removed % small polygons in face_table_name %', num_rows_removed, 'topo_ar5_forest_sysdata.face';
    COMMIT;
    PERFORM topology.DropTopology (border_topo_info.topology_name);
  ELSE
    -- on cell border
    -- test with  area to block like bb
    -- area_to_block := bb;
    -- count the number of rows that intersects
    command_string := Format('select count(*) from %1$s where cell_geo && %2$L and ST_intersects(cell_geo,%2$L);', _job_list_name, area_to_block);
    EXECUTE command_string INTO num_boxes_intersect;
    command_string := Format('select count(*) from (select * from %1$s where cell_geo && %2$L and ST_intersects(cell_geo,%2$L) for update SKIP LOCKED) as r;', _job_list_name, area_to_block);
    EXECUTE command_string INTO num_boxes_free;
    IF num_boxes_intersect != num_boxes_free THEN
      RETURN ;
    END IF;
    border_topo_info.topology_name := _topology_name;
    -- NB We have to use fixed snap to here to be sure that lines snapp
    command_string := Format('SELECT topo_update.add_border_lines(%1$L,geo,%3$s) from topo_update.get_left_over_borders(%4$L,%2$L,%5$L)', 
    _topology_name, bb, snap_tolerance_fixed, overlapgap_grid,_topology_schema_name);
    EXECUTE command_string;
  END IF;
  RAISE NOTICE 'done work at timeofday:% for layer %, with inside_cell_data %', Timeofday(), border_topo_info.topology_name, inside_cell_data;
  command_string := Format('update %1$s set block_bb = %2$L where cell_geo = %3$L', _job_list_name, bb, bb);
  RAISE NOTICE '% ', command_string;
  EXECUTE command_string;
  RAISE NOTICE 'timeofday:% ,done job nocutline ready to start next', Timeofday();
  done_time := Clock_timestamp();
  
  used_time := (Extract(EPOCH FROM (done_time - start_time)));
  RAISE NOTICE 'work done proc :% border_layer_id %, using % sec', done_time, border_topo_info.border_layer_id, used_time;
  -- This is a list of lines that fails
  -- this is used for debug
  IF used_time > 10 THEN
    RAISE NOTICE 'very long a set of lines % time with geo for bb % ', used_time, bb;
    EXECUTE FORMAT('INSERT INTO %s.long_time_log2 (execute_time, info, sql, geo) VALUES (%s, %L, %L, %L)',
    _topology_schema_name,used_time,'simplefeature_c2_topo_surface_border_retry',command_string,bb);  
  END IF;
  PERFORM topo_update.clear_blocked_area (bb, _job_list_name);
  RAISE NOTICE 'leave work at timeofday:% for layer %, with inside_cell_data %', Timeofday(), border_topo_info.topology_name, inside_cell_data;
  --RETURN added_rows;
END
$$;


