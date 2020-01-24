CREATE OR REPLACE PROCEDURE resolve_overlap_gap_single_cell (input_table_name character varying, input_table_geo_column_name character varying, input_table_pk_column_name character varying, _table_name_result_prefix varchar, _topology_name character varying, _srid int, _utm boolean, _simplify_tolerance double precision, _snap_tolerance double precision, _do_chaikins boolean, _min_area_to_keep float, _job_list_name character varying, overlapgap_grid varchar, bb geometry, _cell_job_type int -- add lines 1 inside cell, 2 boderlines, 3 exract simple
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
BEGIN
  RAISE NOTICE 'start wwork at timeofday:% for layer %, with _cell_job_type %', Timeofday(), _topology_name || '_', _cell_job_type;
  -- check if job is done already
  command_string := Format('select count(*) from %s as gt, %s as done
    where gt.cell_geo && ST_PointOnSurface(%3$L) and gt.id = done.id', _job_list_name, _job_list_name || '_donejobs', bb);
  EXECUTE command_string INTO is_done;
  IF is_done = 1 THEN
    RAISE NOTICE 'Job is_done for  : %', ST_astext (bb);
    RETURN;
  END IF;
  start_time := Clock_timestamp();
  RAISE NOTICE 'enter work at timeofday:% for layer %, with _cell_job_type %', Timeofday(), _topology_name || '_' || box_id, _cell_job_type;
  IF bb IS NULL AND input_table_name IS NOT NULL THEN
    command_string := Format('select ST_Envelope(ST_Collect(geo)) from %s', input_table_name);
    EXECUTE command_string INTO bb;
  END IF;
  -- get area to block and set
  -- I don't see why we need this code ??????????? why cant we just the bb as it is so I test thi snow
  area_to_block := bb;
  -- area_to_block := resolve_overlap_gap_block_cell(input_table_name, input_table_geo_column_name, input_table_pk_column_name, _job_list_name, bb);
  RAISE NOTICE 'area to block:% ', area_to_block;
  border_topo_info.snap_tolerance := _simplify_tolerance;
  --      --border_topo_info.border_layer_id = 317;
  command_string := Format('select id from %1$s where cell_geo = %2$L', _job_list_name, bb);
  RAISE NOTICE '% ', command_string;
  EXECUTE command_string INTO box_id;
  RAISE NOTICE 'start work at timeofday:% for layer %, with _cell_job_type %', Timeofday(), _topology_name || '_' || box_id, _cell_job_type;
  -- Create a temp table name
  temp_table_name := '_result_temp_' || box_id;
  --now()::Date::Varchar
  temp_table_id_column := '_id' || temp_table_name;
  final_result_table_name := _table_name_result_prefix || '_result';
  --  	  	array_agg(quote_ident(update_column)) AS update_fields,
  --	  	array_agg('r.'||quote_ident(update_column)) as update_fields_source
  -- 		  INTO
  --		  	update_fields,
  --		  	update_fields_source
  IF _cell_job_type = 1 THEN
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
    command_string := Format('create temp table tmp_simplified_border_lines as (select g.* FROM topo_update.get_simplified_border_lines(%L,%L,%L,%L,%L,%L) g)', input_table_name, input_table_geo_column_name, bb, _simplify_tolerance, _do_chaikins, _table_name_result_prefix);
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
    command_string := Format('SELECT topo_update.add_border_lines(%3$L,r.geom,%1$s,%4$L) FROM (
                  SELECT geom from  %2$s.edge ) as r', snap_tolerance_fixed, border_topo_info.topology_name, _topology_name, _table_name_result_prefix);
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
    num_rows_removed := topo_update.do_remove_small_areas_no_block (border_topo_info.topology_name, _min_area_to_keep, face_table_name, bb,
      _utm);
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
  SELECT topo_update.add_border_lines(%4$L,r.geom,%1$s,%5$L) FROM r', _snap_tolerance, Quote_literal(border_topo_info.topology_name), border_topo_info.topology_name, _topology_name, _table_name_result_prefix);
    --              RAISE NOTICE 'command_string %' , command_string;
    --              EXECUTE command_string;
    -- add to finale result
    ------- this does not work
    command_string := Format('SELECT topo_update.add_border_lines(%4$L,r.geom,%1$s,%5$L) FROM (
                  SELECT geom from  %2$s.edge where ST_DWithin(geom,%3$L,0.6) is true) as r', _snap_tolerance, border_topo_info.topology_name, ST_ExteriorRing (bb), _topology_name, _table_name_result_prefix);
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
    command_string := Format('SELECT topo_update.add_border_lines(%4$L,r.geom,%1$s,%5$L) FROM (
                  SELECT geom from  %2$s.edge) as r', _snap_tolerance, border_topo_info.topology_name, ST_ExteriorRing (bb), _topology_name, _table_name_result_prefix);
    RAISE NOTICE 'command_string %', command_string;
    EXECUTE command_string;
    -- analyze table topo_ar5_forest_sysdata.face;
    -- remove small polygons in main table
    --              num_rows_removed := topo_update.do_remove_small_areas_no_block(border_topo_info.topology_name,'topo_ar5_forest_sysdata.face' ,'mbr','face_id',_job_list_name ,bb );
    --              RAISE NOTICE 'Removed % small polygons in face_table_name %', num_rows_removed, 'topo_ar5_forest_sysdata.face';
    COMMIT;
    PERFORM topology.DropTopology (border_topo_info.topology_name);
  ELSIF _cell_job_type = 2 THEN
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
    -- NB We have to use fixed snap to here to be sure that lines snapp
    command_string := Format('SELECT topo_update.add_border_lines(%1$L,geo,%3$s,%5$L) from topo_update.get_left_over_borders(%4$L,%6$L,%2$L,%5$L)', _topology_name, bb, snap_tolerance_fixed, overlapgap_grid, _table_name_result_prefix, input_table_geo_column_name);
    EXECUTE command_string;
  ELSIF _cell_job_type = 3 THEN
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
 	select st_getFaceGeometry(%1$L,face_id) as %5$s from (
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
 				where f.%4$s && i.%4$s and ST_Intersects(f.%4$s,i.%4$s)
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
    EXECUTE Format('INSERT INTO %s (execute_time, info, sql, geo) VALUES (%s, %L, %L, %L)', _table_name_result_prefix || '_long_time_log2', used_time, 'simplefeature_c2_topo_surface_border_retry', command_string, bb);
  END IF;
  PERFORM topo_update.clear_blocked_area (bb, _job_list_name);
  RAISE NOTICE 'leave work at timeofday:% for layer %, with _cell_job_type %', Timeofday(), border_topo_info.topology_name, _cell_job_type;
  --RETURN added_rows;
END
$$;

