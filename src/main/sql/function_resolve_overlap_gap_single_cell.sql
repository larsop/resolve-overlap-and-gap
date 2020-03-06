CREATE OR REPLACE PROCEDURE resolve_overlap_gap_single_cell (
input_table_name character varying, 
input_table_geo_column_name character varying, 
input_table_pk_column_name character varying, 
_table_name_result_prefix varchar, 
_topology_name character varying, 
_srid int, _utm boolean, 
_simplify_tolerance double precision,
_snap_tolerance double precision, 
_do_chaikins boolean, 
_min_area_to_keep float, 
_job_list_name character varying, 
overlapgap_grid varchar, 
bb geometry, 
_cell_job_type int, -- add lines 1 inside cell, 2 boderlines, 3 exract simple
_loop_number int
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
  snap_tolerance_fixed float = _snap_tolerance;
  glue_snap_tolerance_fixed float = _snap_tolerance / 10000;
  temp_table_name varchar;
  temp_table_id_column varchar;
  final_result_table_name varchar;
  update_fields varchar;
  update_fields_source varchar;
  
  
  subtransControlLock_start timestamp;
  subtransControlLock_count int;
  subtranscontrollock int;

  has_edges boolean;
  has_edges_temp_table_name text;

  v_state text;
  v_msg text;
  v_detail text;
  v_hint text;
  v_context text;

BEGIN
	
  command_string := Format('select id from %1$s where cell_geo = %2$L', _job_list_name, bb);
  RAISE NOTICE '% ', command_string;
  EXECUTE command_string INTO box_id;

  RAISE NOTICE 'enter at timeofday:% for layer %, with _cell_job_type % and box id % .', 
  Timeofday(), _topology_name || '_', _cell_job_type, box_id;
  
  
  -- check if job is done already
  command_string := Format('select count(*) from %s as gt, %s as done
    where gt.cell_geo && ST_PointOnSurface(%3$L) and gt.id = done.id', _job_list_name, _job_list_name || '_donejobs', bb);
  EXECUTE command_string INTO is_done;
  IF is_done = 1 THEN
    RAISE NOTICE 'Job is_done for  : %', box_id;
    RETURN;
  END IF;
  start_time := Clock_timestamp();

  -- get area to block and set
  -- I don't see why we need this code ??????????? why cant we just the bb as it is so I test thi snow
  area_to_block := bb;
  -- area_to_block := resolve_overlap_gap_block_cell(input_table_name, input_table_geo_column_name, input_table_pk_column_name, _job_list_name, bb);
  -- RAISE NOTICE 'area to block:% ', area_to_block;
  border_topo_info.snap_tolerance := _simplify_tolerance;
  
  RAISE NOTICE 'start work at timeofday:% for layer %, with _cell_job_type %', Timeofday(), _topology_name || '_' || box_id, _cell_job_type;
  
      -- check if any 'SubtransControlLock' is there
        subtransControlLock_start = clock_timestamp();
        subtransControlLock_count = 0;
        
        -- this isnot good beacuse if have code making this block
        -- we need to make a check on for relation 
        -- SELECT relation::regclass, * FROM pg_locks WHERE NOT GRANTED;
        --relation           | test_topo_ar5.edge_data
    LOOP
      EXECUTE Format('SELECT count(*) from pg_stat_activity where wait_event in (%L,%L)',
      'SubtransControlLock','relation') into subtransControlLock;
      EXIT WHEN subtransControlLock = 0 OR subtransControlLock_count > 40;
      
      subtransControlLock_count := subtransControlLock_count + 1;
      PERFORM pg_sleep(subtransControlLock*subtransControlLock_count*0.1);

    END LOOP;
    
    
    
    IF subtransControlLock_count > 0 THEN
      RAISE NOTICE '% subtransControlLock,relation loops, sleep % seconds to wait for release, for _cell_job_type %',
      subtransControlLock_count, 
      (Extract(EPOCH FROM (clock_timestamp() - subtransControlLock_start))),
      _cell_job_type;
    END IF;
    
   
    
  IF _cell_job_type = 1 THEN
  
    -- get the siple feature data both the line_types and the inner lines.
    -- the boundery linnes are saved in a table for later usage
    command_string := Format('create temp table tmp_simplified_border_lines_1 as 
    (select g.* , ST_NPoints(geo) as num_points, ST_IsClosed(geo) as is_closed  
     FROM topo_update.get_simplified_border_lines(%L,%L,%L,%L,%L,%L) g)', 
    input_table_name, input_table_geo_column_name, bb, _simplify_tolerance, _do_chaikins, _table_name_result_prefix);
    EXECUTE command_string ;
    
    EXECUTE Format('CREATE INDEX ON tmp_simplified_border_lines_1(is_closed)');
    
    IF (_utm = false) THEN
      create temp table tmp_simplified_border_lines as 
      (select * from tmp_simplified_border_lines_1 where 
      is_closed = false or 
      (ST_Area(ST_Envelope(geo),true) > _min_area_to_keep and ST_Area(ST_MakePolygon(geo),true) > _min_area_to_keep)); 
    ELSE
      create temp table tmp_simplified_border_lines as 
      (select * from tmp_simplified_border_lines_1 where 
      is_closed = false or 
      (ST_Area(ST_Envelope(geo)) > _min_area_to_keep and ST_Area(ST_MakePolygon(geo)) > _min_area_to_keep)); 
    END IF;

    DROP TABLE tmp_simplified_border_lines_1;

    EXECUTE Format('CREATE INDEX ON tmp_simplified_border_lines(is_closed,num_points)');

--    IF _loop_number = 1 THEN 
--       RAISE NOTICE 'use _topology_name %', _topology_name;
--       command_string := Format('SELECT topology.TopoGeo_addLinestring(%2$L,r.geo,%1$s) FROM 
--         (SELECT geo from tmp_simplified_border_lines where line_type = 1) as r', glue_snap_tolerance_fixed, _topology_name);
--       EXECUTE command_string ; 
--       command_string := Format('SELECT topology.TopoGeo_addLinestring(%2$L,r.geo,%1$s) FROM 
--         (SELECT geo from tmp_simplified_border_lines where line_type = 0 order by is_closed desc, num_points desc) as r', snap_tolerance_fixed, _topology_name);
--       EXECUTE command_string ; 
--       RAISE NOTICE 'Start clean small polygons at _loop_number 1 for face_table_name % at %', face_table_name, Clock_timestamp();
--       -- remove small polygons in temp
--       face_table_name = _topology_name || '.face';
--       num_rows_removed := topo_update.do_remove_small_areas_no_block (_topology_name, _min_area_to_keep, face_table_name, ST_buffer(bb,_snap_tolerance * -6),
--       _utm);
--       used_time := (Extract(EPOCH FROM (Clock_timestamp() - start_remove_small)));
--       RAISE NOTICE 'Removed % clean small polygons for face_table_name % at % used_time: %', num_rows_removed, face_table_name, Clock_timestamp(), used_time;
--    ELSE 
    
    border_topo_info.topology_name := _topology_name || '_' || box_id;
    RAISE NOTICE 'use border_topo_info.topology_name %', border_topo_info.topology_name;
    
    IF ((SELECT Count(*) FROM topology.topology WHERE name = border_topo_info.topology_name) = 1) THEN
       EXECUTE Format('SELECT topology.droptopology(%s)', Quote_literal(border_topo_info.topology_name));
    END IF;
    --drop this schema in case it exists
    EXECUTE Format('DROP SCHEMA IF EXISTS %s CASCADE', border_topo_info.layer_schema_name);
 
    PERFORM topology.CreateTopology (border_topo_info.topology_name, _srid, snap_tolerance_fixed);
    EXECUTE Format('ALTER table %s.edge_data set unlogged', border_topo_info.topology_name);
    EXECUTE Format('ALTER table %s.node set unlogged', border_topo_info.topology_name);
    EXECUTE Format('ALTER table %s.face set unlogged', border_topo_info.topology_name);
    EXECUTE Format('ALTER table %s.relation set unlogged', border_topo_info.topology_name);
    
    EXECUTE Format('CREATE INDEX ON %s.node(containing_face)', border_topo_info.topology_name);
    EXECUTE Format('CREATE INDEX ON %s.relation(layer_id)', border_topo_info.topology_name);
    EXECUTE Format('CREATE INDEX ON %s.relation(abs(element_id))', border_topo_info.topology_name);
    EXECUTE Format('CREATE INDEX ON %s.edge_data USING GIST (geom)', border_topo_info.topology_name);
    EXECUTE Format('CREATE INDEX ON %s.relation(element_id)', border_topo_info.topology_name);
    EXECUTE Format('CREATE INDEX ON %s.relation(topogeo_id)', border_topo_info.topology_name);

        
    -- add the glue line with no/small tolerance
    border_topo_info.snap_tolerance := glue_snap_tolerance_fixed;
    command_string := Format('SELECT topo_update.create_nocutline_edge_domain_obj_retry(json::Text, %L) 
                  from tmp_simplified_border_lines g where line_type = 1', border_topo_info);
    --RAISE NOTICE 'command_string %', command_string;
    EXECUTE command_string;

    -- using the input tolreance for adding
    border_topo_info.snap_tolerance := snap_tolerance_fixed;
    command_string := Format('SELECT topo_update.create_nocutline_edge_domain_obj_retry(json::Text, %L) 
                  from tmp_simplified_border_lines g where line_type = 0 order by is_closed desc, num_points desc', border_topo_info);
    --RAISE NOTICE 'command_string %', command_string;
    EXECUTE command_string;

    face_table_name = border_topo_info.topology_name || '.face';
    start_remove_small := Clock_timestamp();
    RAISE NOTICE 'Start clean small polygons for face_table_name % at %', face_table_name, Clock_timestamp();
    -- remove small polygons in temp
    --not working corectly num_rows_removed := topo_update.do_remove_only_valid_small_areas (
    -- border_topo_info.topology_name, _min_area_to_keep, face_table_name, bb,_utm);
   num_rows_removed := topo_update.do_remove_small_areas_no_block (
   border_topo_info.topology_name, _min_area_to_keep, face_table_name, bb,_utm);
    
    used_time := (Extract(EPOCH FROM (Clock_timestamp() - start_remove_small)));
    RAISE NOTICE 'Removed % clean small polygons for face_table_name % at % used_time: %', num_rows_removed, face_table_name, Clock_timestamp(), used_time;
    
    command_string := Format('SELECT EXISTS(SELECT 1 from  %1$s.edge limit 1)',
    border_topo_info.topology_name);

    EXECUTE command_string into has_edges;
    IF (has_edges) THEN
      has_edges_temp_table_name := _topology_name||'.edge_data_tmp_' || box_id;
      command_string := Format('create unlogged table %1$s as (SELECT geom, ST_IsClosed(geom) as is_closed, ST_NPoints(geom) as num_points from  %2$s.edge_data)',
      has_edges_temp_table_name,
      border_topo_info.topology_name);
      EXECUTE command_string;
      
      EXECUTE Format('CREATE INDEX ON %s(is_closed,num_points)', has_edges_temp_table_name);

      
    END IF;
    execute Format('SET CONSTRAINTS ALL IMMEDIATE');
    PERFORM topology.DropTopology (border_topo_info.topology_name);

--    END IF;

    
    DROP TABLE IF EXISTS tmp_simplified_border_lines;
 
  ELSIF _cell_job_type = 2 THEN

    has_edges_temp_table_name := _topology_name||'.edge_data_tmp_' || box_id;
    command_string := Format('SELECT EXISTS(SELECT 1 from to_regclass(%L) where to_regclass is not null)',has_edges_temp_table_name);
    
    EXECUTE command_string into has_edges;
    RAISE NOTICE 'cell % cell_job_type %, has_edges %, _loop_number %', box_id, _cell_job_type, has_edges, _loop_number;
    IF (has_edges) THEN
     IF _loop_number = 1 THEN 
       command_string := Format('SELECT topology.TopoGeo_addLinestring(%3$L,r.geom,%1$s) FROM (SELECT geom from %2$s order by is_closed desc, num_points desc) as r', _snap_tolerance, has_edges_temp_table_name, _topology_name);
     ELSE
       command_string := Format('SELECT topo_update.add_border_lines(%4$L,r.geom,%1$s,%5$L) FROM (SELECT geom from %2$s order by is_closed desc, num_points desc) as r', _snap_tolerance, has_edges_temp_table_name, ST_ExteriorRing (bb), _topology_name, _table_name_result_prefix);
     END IF;
     EXECUTE command_string;
      
     command_string := Format('DROP TABLE IF EXISTS %s',has_edges_temp_table_name);
     EXECUTE command_string;
    
    END IF;
  ELSIF _cell_job_type = 3 THEN
    -- on cell border
    -- test with  area to block like bb
    -- area_to_block := bb;
    -- count the number of rows that intersects
    area_to_block := ST_BUffer (bb, glue_snap_tolerance_fixed);
    command_string := Format('select count(*) from %1$s where cell_geo && %2$L and ST_intersects(cell_geo,%2$L);', _job_list_name, area_to_block);
    EXECUTE command_string INTO num_boxes_intersect;
    command_string := Format('select count(*) from (select * from %1$s where cell_geo && %2$L and ST_intersects(cell_geo,%2$L) for update SKIP LOCKED) as r;', _job_list_name, area_to_block);
    EXECUTE command_string INTO num_boxes_free;
    IF num_boxes_intersect != num_boxes_free THEN
      RETURN;
    END IF;
    

    border_topo_info.topology_name := _topology_name;
    IF _loop_number = 1 THEN 
      command_string := Format('SELECT topology.TopoGeo_addLinestring(%1$L,geo,%3$s) from topo_update.get_left_over_borders(%4$L,%6$L,%2$L,%5$L)', 
      _topology_name, bb, snap_tolerance_fixed, overlapgap_grid, _table_name_result_prefix, input_table_geo_column_name);
    ELSE
      command_string := Format('SELECT topo_update.add_border_lines(%1$L,geo,%3$s,%5$L) from topo_update.get_left_over_borders(%4$L,%6$L,%2$L,%5$L)', 
      _topology_name, bb, snap_tolerance_fixed, overlapgap_grid, _table_name_result_prefix, input_table_geo_column_name);
    END IF;
    -- NB We have to use fixed snap to here to be sure that lines snapp
    EXECUTE command_string;

    -- add long lines that 
    IF _loop_number = 1 THEN 
      command_string := Format('SELECT topology.TopoGeo_addLinestring(%1$L,r.geo,%3$s) from %7$s r where ST_StartPoint(r.geo) && %2$L', 
      _topology_name, bb, snap_tolerance_fixed, overlapgap_grid, 
      _table_name_result_prefix, input_table_geo_column_name,
      _table_name_result_prefix||'_border_line_many_points');
    ELSE
      command_string := Format('SELECT topo_update.add_border_lines(%1$L,r.geo,%3$s,%5$L) from %7$s r where ST_StartPoint(r.geo) && %2$L' , 
      _topology_name, bb, snap_tolerance_fixed, overlapgap_grid, 
      _table_name_result_prefix, input_table_geo_column_name,
      _table_name_result_prefix||'_border_line_many_points');
    END IF;
    -- NB We have to use fixed snap to here to be sure that lines snapp
    EXECUTE command_string;
    
    
    face_table_name = _topology_name || '.face';
    start_remove_small := Clock_timestamp();
    RAISE NOTICE 'Start clean small polygons for border plygons face_table_name % at %', face_table_name, Clock_timestamp();
    -- remove small polygons in temp
    -- TODO 6 sould be based on other values
    num_rows_removed := topo_update.do_remove_small_areas_no_block (_topology_name, _min_area_to_keep, face_table_name, ST_buffer(ST_ExteriorRing(bb),_snap_tolerance * 6),
      _utm);
    used_time := (Extract(EPOCH FROM (Clock_timestamp() - start_remove_small)));
    RAISE NOTICE 'Removed % clean small polygons for face_table_name % at % used_time: %', num_rows_removed, face_table_name, Clock_timestamp(), used_time;
    -- remove small polygons in border sones
  ELSIF _cell_job_type = 4 THEN
     
     -- remove 
     face_table_name = _topology_name || '.face';
     start_remove_small := Clock_timestamp();
     RAISE NOTICE 'Start clean small polygons for border plygons face_table_name % at %', face_table_name, Clock_timestamp();
     -- remove small polygons in temp
     -- TODO 6 sould be based on other values
     num_rows_removed := topo_update.do_remove_small_areas_no_block (_topology_name, _min_area_to_keep, face_table_name, ST_buffer(bb,(_snap_tolerance * -6)),
      _utm);
     used_time := (Extract(EPOCH FROM (Clock_timestamp() - start_remove_small)));
     RAISE NOTICE 'Removed % clean small polygons for after adding to main face_table_name % at % used_time: %', num_rows_removed, face_table_name, Clock_timestamp(), used_time;

  ELSIF _cell_job_type = 5 THEN
    -- Create a temp table name
    temp_table_name := '_result_temp_' || box_id;
    temp_table_id_column := '_id' || temp_table_name;
    final_result_table_name := _table_name_result_prefix || '_result';

    -- Drop/Create a temp to hold data temporay for job
    EXECUTE Format('DROP TABLE IF EXISTS %s', temp_table_name);
    -- Create the temp for result simple feature result table  as copy of the input table
    EXECUTE Format('CREATE TEMP TABLE %s AS TABLE %s with NO DATA', temp_table_name, final_result_table_name);
    -- Add an extra column to hold a list of other intersections surfaces
    EXECUTE Format('ALTER TABLE %s ADD column %s serial', temp_table_name, temp_table_id_column);
    -- Update cl
    command_string := Format('select 
 	  	array_to_string(array_agg(quote_ident(update_column)),%L) AS update_fields,
 	  	array_to_string(array_agg(%L||quote_ident(update_column)),%L) as update_fields_source
 		  FROM (
 		   SELECT distinct(json_object_keys) AS update_column
 		   FROM json_object_keys(to_json(json_populate_record(NULL::%s, %L::Json))) 
 		   where json_object_keys != %L and json_object_keys != %L and json_object_keys != %L  
 		  ) as keys', ',', 'r.', ',', temp_table_name, '{}', temp_table_id_column, input_table_geo_column_name, '_other_intersect_id_list');
    RAISE NOTICE '% ', command_string;
    EXECUTE command_string INTO update_fields, update_fields_source;
    -- Insert new geos based on all face id
    command_string := Format('insert into %3$s(%5$s)
 	select (ST_dump(st_getFaceGeometry(%1$L,face_id))).geom as %5$s from (
 	SELECT f.face_id, min(jl.id) as cell_id  FROM
 	%1$s.face f, 
 	%4$s jl 
 	WHERE f.mbr && %2$L and jl.cell_geo && f.mbr
 	GROUP BY f.face_id
 	) as r where cell_id = %6$s', _topology_name, bb, temp_table_name, _table_name_result_prefix || '_job_list', input_table_geo_column_name, box_id);
    RAISE NOTICE 'command_string %', command_string;
    EXECUTE command_string;
    -- update/add primary key and _other_intersect_id_list based on geo
    command_string := Format('update %1$s t
 set (%3$s,_other_intersect_id_list) = (r.%3$s,r._other_intersect_id_list) 
 from (
 	SELECT r.*, r.intersect_id_list[2:] as _other_intersect_id_list , r.intersect_id_list[1] as %3$s  from (
 		select distinct %5$s, array_agg(%3$s) OVER (PARTITION BY %5$s) as intersect_id_list from (
 		select %5$s, %3$s 
 			from (
 				SELECT %5$s, i.%3$s, ST_Area(ST_Intersection(f.%4$s,i.%4$s))/ST_area(f.%4$s) as area_coverarge, ST_Area(i.%4$s) as area_neighbour
 				FROM 
 				%1$s f,
 				%2$s i
 				where f.%4$s && i.%4$s and ST_Intersects(f.%4$s,i.%4$s) and ST_IsValid(f.%4$s) and ST_IsValid(i.%4$s)
 				order by %5$s, area_coverarge desc,  i.%3$s
 			) as r where area_coverarge > 0.1
 			order by %5$s, area_neighbour desc
 		) as r
 	) as r
 ) r where r.%5$s = t.%5$s', temp_table_name, input_table_name, input_table_pk_column_name, input_table_geo_column_name, temp_table_id_column);
    EXECUTE command_string;
    -- Remove extra column column to hold a list of other intersections surfaces
    EXECUTE Format('ALTER TABLE %s DROP column %s', temp_table_name, temp_table_id_column);
    -- update/add primary key and _other_intersect_id_list based on geo
    command_string := Format('update %1$s t
 set (%4$s) = (%5$s) 
 from %2$s r
 where r.%3$s = t.%3$s', temp_table_name, input_table_name, input_table_pk_column_name, update_fields, update_fields_source);
    EXECUTE command_string;
    command_string := Format('insert into %1$s select * from %2$s', final_result_table_name, temp_table_name);
    EXECUTE command_string;
    -- Drop/Create a temp to hold data temporay for job
    EXECUTE Format('DROP TABLE IF EXISTS %s', temp_table_name);
  ELSE
    RAISE EXCEPTION 'Invalid _cell_job_type % ', _cell_job_type;
  END IF;
  RAISE NOTICE 'done work at timeofday:% for layer %, with _cell_job_type %', Timeofday(), border_topo_info.topology_name, _cell_job_type;
  command_string := Format('update %1$s set block_bb = %2$L where cell_geo = %3$L', _job_list_name, bb, bb);
  EXECUTE command_string;
  
  done_time := Clock_timestamp();
  used_time := (Extract(EPOCH FROM (done_time - start_time)));
  RAISE NOTICE 'work done for cell % at % border_layer_id %, using % sec', box_id, done_time, border_topo_info.border_layer_id, used_time;
  -- This is a list of lines that fails
  -- this is used for debug
  IF used_time > 10 THEN
    RAISE NOTICE 'very long time used for lines, % time with geo for bb % ', used_time, box_id;
    EXECUTE Format('INSERT INTO %s (execute_time, info, sql, geo) VALUES (%s, %L, %L, %L)', _table_name_result_prefix || '_long_time_log2', used_time, 'simplefeature_c2_topo_surface_border_retry', command_string, bb);
  END IF;
  PERFORM topo_update.clear_blocked_area (area_to_block, _job_list_name);
  RAISE NOTICE 'leave work at timeofday:% for layer %, with _cell_job_type % for cell %', Timeofday(), border_topo_info.topology_name, _cell_job_type, box_id;
  --RETURN added_rows;
END
$$;
