CREATE OR REPLACE PROCEDURE resolve_overlap_gap_single_cell (
_input_data resolve_overlap_data_input_type, 
--(_input_data).polygon_table_name varchar, -- The table to resolv, imcluding schema name
--(_input_data).polygon_table_pk_column varchar, -- The primary of the input table
--(_input_data).polygon_table_geo_collumn varchar, -- the name of geometry column on the table to analyze
--(_input_data).table_srid int, -- the srid for the given geo column on the table analyze
--(_input_data).utm boolean, 

_topology_info resolve_overlap_data_topology_type,
---(_topology_info).topology_name varchar, -- The topology schema name where we store store sufaces and lines from the simple feature dataset and th efinal result
-- NB. Any exting data will related to topology_name will be deleted
--(_topology_info).topology_snap_tolerance float, -- this is tolerance used as base when creating the the postgis topolayer
--(_topology_info).create_topology_attrbute_tables boolean -- if this is true and we value for line_table_name we create attribute tables refferances to  
-- this tables will have atrbuttes equal to the simple feauture tables for lines and feautures

_table_name_result_prefix varchar, 
--(_topology_info).topology_name character varying, 
--(_topology_info).topology_snap_tolerance float, -- this is tolerance used as base when creating the the postgis topolayer

_clean_info resolve_overlap_data_clean_type, -- different parameters used if need to clean up your data
--(_clean_info)._min_area_to_keep float default 0, -- if this a polygon  is below this limit it will merge into a neighbour polygon. The area is sqare meter.
--(_clean_info)._simplify_tolerance float default 0, -- is this is more than zero simply will called with
--(_clean_info)._simplify_max_average_vertex_length int default 0, -- in meter both for utm and deegrees, this used to avoid running ST_simplifyPreserveTopology for long lines lines with few points
--(_clean_info)._chaikins_nIterations int default 0, -- IF 0 NO CHAKINS WILL BE DONE,  A big value here make no sense because the number of points will increaes exponential )
--(_clean_info)._chaikins_max_length int default 0, --edge that are longer than this value will not be touched by _chaikins_min_degrees and _chaikins_max_degrees  
--(_clean_info)._chaikins_min_degrees int default 0, -- The angle has to be less this given value, This is used to avoid to touch all angles. 
--(_clean_info)._chaikins_max_degrees int default 0, -- OR the angle has to be greather than this given value, This is used to avoid to touch all angles 
--(_clean_info)._chaikins_min_steep_angle_degrees int default 0, -- The angle has to be less this given value, This is used to avoid to touch all angles. 
--(_clean_info)._chaikins_max_steep_angle_degrees int default 0-- OR The angle has to be greather than this given value, This is used to avoid to touch all angles 

_job_list_name character varying, 
overlapgap_grid varchar, 
_bb geometry, 
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
  updated_rows int;
  start_time timestamp WITH time zone;
  done_time timestamp WITH time zone;
  used_time real;
  start_time_delta_job timestamp WITH time zone;
  is_done integer = 0;
  area_to_block geometry;
  num_boxes_intersect integer;
  num_boxes_free integer;
  num_rows_removed integer;
  box_id integer;
  face_table_name varchar;
  -- This is used when adding lines hte tolrannce is different when adding lines inside and box and the border;
  snap_tolerance_fixed float = (_topology_info).topology_snap_tolerance;
  
  glue_snap_tolerance_fixed float = 0;
  
  temp_table_name varchar;
  temp_table_id_column varchar;
  final_result_table_name varchar;
  update_fields varchar;
  update_fields_source varchar;
  
  result_st_change_geom int; 
  
  subtransControlLock_start timestamp;
  subtransControlLock_count int;
  subtranscontrollock int;

  has_edges boolean;
  has_edges_temp_table_name text;
  lines_to_add geometry[];
  column_data_as_json_to_add jsonb[];
  v_state text;
  v_msg text;
  v_detail text;
  v_hint text;
  v_context text;
  v_cnt_left_over_borders int;
 
  
  -- This is the area that will handle when glus cell together, this is very importan to avoid smooth lines to times
  outer_cell_boundary_geom geometry;
  inner_cell_boundary_geom geometry;

  -- This is lines that nay intersesct with outher boudary lines
  outer_cell_boundary_lines geometry;

  -- This is the inner area tha we safly can fix
  inner_cell_geom geometry;
  
  -- Distance to inner safe area
  -- TODo find a valid value  here
  inner_cell_distance int = 10;
  
  line_edges_added integer[]; 
  line_edges_tmp integer[]; 
  
  edgelist_to_change integer[]; 
  edge_id_heal integer;

  heal_edge_status int;
  heal_edge_retry_num int;
  
  num_locked int;

  tmp_simplified_border_lines_name text;

  border_line_rec RECORD;
  line_edges_geo_failed geometry[]; 
  
  this_worker_id int;
  num_jobs_worker_id int;
  num_min_since_last_analyze int;

  analyze_done_at timestamp WITH time zone default null;
 
BEGIN
	
  command_string := Format('select id from %1$s where cell_geo = %2$L', _job_list_name, _bb);
  RAISE NOTICE '% ', command_string;
  EXECUTE command_string INTO box_id;

  tmp_simplified_border_lines_name := 'temp_simplified_lines'|| (_topology_info).topology_name || box_id;
  
  RAISE NOTICE 'enter at timeofday:% for layer %, with _cell_job_type % and box id % .', 
  Timeofday(), (_topology_info).topology_name || '_', _cell_job_type, box_id;
  
  
  -- check if job is done already
  command_string := Format('select count(*) from %s as gt, %s as done
    where ST_Equals(gt.cell_geo,%3$L) and gt.id = done.id', _job_list_name, _job_list_name || '_donejobs', _bb);
  EXECUTE command_string INTO is_done;
  IF is_done = 1 THEN
    RAISE NOTICE 'Job is_done for  : %', box_id;
    RETURN;
  END IF;
  start_time := Clock_timestamp();
  
  inner_cell_geom := ST_MakePolygon ((
      SELECT ST_ExteriorRing (ST_Expand (_bb, (((_topology_info).topology_snap_tolerance) * -inner_cell_distance))) AS outer_ring));

  outer_cell_boundary_geom := ST_MakePolygon ((
      SELECT ST_ExteriorRing (ST_Expand (_bb, (_topology_info).topology_snap_tolerance/1.5)) AS outer_ring), ARRAY (
        SELECT ST_ExteriorRing (inner_cell_geom) AS inner_rings));
  -- this cause missing faces so we expand the boubdery     
  -- outer_cell_boundary_geom := ST_MakePolygon ((
  --    SELECT ST_ExteriorRing (_bb) AS outer_ring), ARRAY (
  --      SELECT ST_ExteriorRing (inner_cell_geom) AS inner_rings));

  inner_cell_boundary_geom := ST_MakePolygon ((
      SELECT ST_ExteriorRing (ST_Expand (_bb, (((_topology_info).topology_snap_tolerance) * -(inner_cell_distance/2)))) AS outer_ring), ARRAY (
        SELECT ST_ExteriorRing (ST_Expand (_bb, (((_topology_info).topology_snap_tolerance) * -(inner_cell_distance*2)))) AS inner_rings));

  -- get area to block and set
  -- I don't see why we need this code ??????????? why cant we just the _bb as it is so I test thi snow
  area_to_block := _bb;
  -- area_to_block := resolve_overlap_gap_block_cell((_input_data).polygon_table_name, (_input_data).polygon_table_geo_collumn, (_input_data).polygon_table_pk_column, _job_list_name, _bb);
  -- RAISE NOTICE 'area to block:% ', area_to_block;
  border_topo_info.snap_tolerance := (_topology_info).topology_snap_tolerance;
  
  RAISE NOTICE 'start work at timeofday:% for layer %, (_topology_info).topology_snap_tolerance %, with _cell_job_type % and (_clean_info).chaikins_max_degrees) %', 
  Timeofday(), (_topology_info).topology_name || '_' || box_id, (_topology_info).topology_snap_tolerance, _cell_job_type, (_clean_info).chaikins_max_degrees;
  
      -- check if any 'SubtransControlLock' is there
        subtransControlLock_start = clock_timestamp();
        subtransControlLock_count = 0;
        
        -- this isnot good beacuse if have code making this block
        -- we need to make a check on for relation 
        -- SELECT relation::regclass, * FROM pg_locks WHERE NOT GRANTED;
        --relation           | test_topo_ar5.edge_data
    LOOP
      EXECUTE Format('SELECT count(*) from pg_stat_activity where query like %L and wait_event in (%L,%L)',
      'CALL resolve_overlap_gap_single_cell%','SubtransControlLock','relation') into subtransControlLock;
      EXIT WHEN subtransControlLock = 0 OR subtransControlLock_count > 100;
      
      subtransControlLock_count := subtransControlLock_count + 1;
      PERFORM pg_sleep(subtransControlLock*subtransControlLock_count*0.1);

    END LOOP;
    
    
    
    IF subtransControlLock_count > 0 THEN
      RAISE NOTICE '% subtransControlLock,relation loops, sleep % seconds to wait for release, for _cell_job_type %',
      subtransControlLock_count, 
      (Extract(EPOCH FROM (clock_timestamp() - subtransControlLock_start))),
      _cell_job_type;
    END IF;
    
   
    
  IF _cell_job_type = 1 and _loop_number = 1 THEN
    -- get the siple feature data both the line_types and the inner lines.
    -- the boundery linnes are saved in a table for later usage
    
    command_string := Format('call topo_update.get_simplified_border_lines(%L,%L,%L,%L,%L,%L,%L)', 
    _input_data, _topology_info, _bb, _table_name_result_prefix,
    outer_cell_boundary_lines, lines_to_add,column_data_as_json_to_add);
    EXECUTE command_string into outer_cell_boundary_lines, lines_to_add,column_data_as_json_to_add;

    RAISE NOTICE 'lines_to_add size %', Array_length(lines_to_add, 1);
        
    command_string := Format('create temp table %s (geo geometry, column_data_as_json jsonb)', tmp_simplified_border_lines_name);
    EXECUTE command_string ;
    
    command_string := Format('insert into %s(geo,column_data_as_json) (select * from unnest(%L::geometry[],%L::jsonb[]))', 
    tmp_simplified_border_lines_name,
    lines_to_add,
    column_data_as_json_to_add);
    EXECUTE command_string ;

    has_edges_temp_table_name := (_topology_info).topology_name||'.edge_data_tmp_' || box_id;

    -- If we use set input lines with attributes we assume that this lines are correct and genrated from a correct topology layer
    -- We need the keep this as edge as they are since we need attributtetes to assigne to each line.    
    IF (_topology_info).create_topology_attrbute_tables = true and (_input_data).line_table_name is not null THEN
    	command_string := Format('SELECT EXISTS(SELECT 1 from  %1$s limit 1)',
	    tmp_simplified_border_lines_name);
	    EXECUTE command_string into has_edges;
	    
	    IF (has_edges) THEN
	      command_string := Format('create unlogged table %1$s as 
	      SELECT geo as geom, column_data_as_json from %2$s',
	      has_edges_temp_table_name, 
	      tmp_simplified_border_lines_name);
	      EXECUTE command_string;
	    END IF;
    ELSE
	    IF ST_NumGeometries(outer_cell_boundary_lines) = 0 THEN
	     outer_cell_boundary_lines  := null;
	    END IF; 
	    
	
	    
	    IF (_clean_info).simplify_tolerance > 0  THEN
	        command_string := Format('UPDATE %4$s l
	        SET geo = ST_simplifyPreserveTopology(l.geo,%1$s)
	        WHERE NOT ST_DWithin(%2$L,l.geo,%3$s) OR %2$L IS NULL',
	        (_clean_info).simplify_tolerance , 
	        outer_cell_boundary_lines,
	        snap_tolerance_fixed,
	        tmp_simplified_border_lines_name);
	        EXECUTE command_string;
	    END IF;
	    
	    IF (_clean_info).chaikins_nIterations > 0 THEN
	        command_string := Format('UPDATE %6$s l
	        SET geo = ST_simplifyPreserveTopology(topo_update.chaikinsAcuteAngle(l.geo,%1$L,%2$L), %3$s)
	        WHERE NOT ST_DWithin(%4$L,l.geo,%5$s) OR %4$L IS NULL',
	        (_input_data).utm,
	        _clean_info,
	        (_topology_info).topology_snap_tolerance/2,
	        outer_cell_boundary_lines,
	        snap_tolerance_fixed,
	        tmp_simplified_border_lines_name);
	        EXECUTE command_string;
	    END IF;
	
	    
	    border_topo_info.topology_name := (_topology_info).topology_name || '_' || box_id;
	    RAISE NOTICE 'use border_topo_info.topology_name %', border_topo_info.topology_name;
	    
	    IF ((SELECT Count(*) FROM topology.topology WHERE name = border_topo_info.topology_name) = 1) THEN
	       EXECUTE Format('SELECT topology.droptopology(%s)', Quote_literal(border_topo_info.topology_name));
	    END IF;
	    --drop this schema in case it exists
	    EXECUTE Format('DROP SCHEMA IF EXISTS %s CASCADE', border_topo_info.topology_name);
	 
	    PERFORM topology.CreateTopology (border_topo_info.topology_name, (_input_data).table_srid, snap_tolerance_fixed);
	    EXECUTE Format('ALTER table %s.edge_data set unlogged', border_topo_info.topology_name);
	    EXECUTE Format('ALTER table %s.node set unlogged', border_topo_info.topology_name);
	    EXECUTE Format('ALTER table %s.face set unlogged', border_topo_info.topology_name);
	    EXECUTE Format('ALTER table %s.relation set unlogged', border_topo_info.topology_name);
	    
	    EXECUTE Format('CREATE INDEX ON %s.node(containing_face)', border_topo_info.topology_name);
	    EXECUTE Format('CREATE INDEX ON %s.relation(layer_id)', border_topo_info.topology_name);
	    EXECUTE Format('CREATE INDEX ON %s.relation(abs(element_id))', border_topo_info.topology_name);
	    EXECUTE Format('CREATE INDEX ON %s.edge_data USING GIST (geom)', border_topo_info.topology_name);
	    EXECUTE Format('CREATE INDEX ON %s.edge_data(abs_next_left_edge)', border_topo_info.topology_name);
	    EXECUTE Format('CREATE INDEX ON %s.edge_data(abs_next_right_edge)', border_topo_info.topology_name);
	    EXECUTE Format('CREATE INDEX ON %s.relation(element_id)', border_topo_info.topology_name);
	    EXECUTE Format('CREATE INDEX ON %s.relation(topogeo_id)', border_topo_info.topology_name);
	
	    
	    -- using the input tolreance for adding
	    border_topo_info.snap_tolerance := snap_tolerance_fixed;
	    --command_string := Format('SELECT topo_update.create_nocutline_edge_domain_obj_retry(jsonb::Text, %L) from tmp_simplified_border_lines g where line_type = 0 order by is_closed desc, num_points desc', border_topo_info);
	    --RAISE NOTICE 'command_string %', command_string;
	    command_string := Format('SELECT topo_update.add_border_lines(%1$L,r.geom,%2$s,%3$L,FALSE) 
	    FROM (select geo as geom from %4$s g) as r 
	    ORDER BY ST_X(ST_Centroid(r.geom)), ST_Y(ST_Centroid(r.geom))',
	    border_topo_info.topology_name, 
	    border_topo_info.snap_tolerance, 
	    _table_name_result_prefix,
	    tmp_simplified_border_lines_name);
	    EXECUTE command_string;
	
	    --command_string := Format('DROP TABLE IF EXISTS %1$s', tmp_simplified_border_lines_name);
	    --EXECUTE command_string;
	    
	
	    EXECUTE Format('ANALYZE %s.node', border_topo_info.topology_name);
	    EXECUTE Format('ANALYZE %s.relation', border_topo_info.topology_name);
	    EXECUTE Format('ANALYZE %s.edge_data', border_topo_info.topology_name);
	    EXECUTE Format('ANALYZE %s.face', border_topo_info.topology_name);
	
	
	 
	    command_string := Format('WITH topo_updated AS (
	      SELECT topo_update.add_border_lines(%1$L,r.geo,%2$s,%3$L,TRUE), geo 
	        FROM (
	          SELECT distinct (ST_Dump(ST_Multi(ST_LineMerge(ST_union(r.geo))))).geom as geo 
	            FROM (
	              select r.geo from %4$s r where ST_CoveredBy(r.geo, %5$L) 
	            ) as r
	          ) as r
	      )
	      update %4$s u 
	      set line_geo_lost = false
	      FROM topo_updated tu
	      where ST_DWithin(tu.geo,u.geo,%6$s) and (SELECT bool_or(x IS NOT NULL) FROM unnest(tu.add_border_lines) x)' , 
	      border_topo_info.topology_name, 
	      border_topo_info.snap_tolerance, 
	      _table_name_result_prefix,
	      _table_name_result_prefix||'_no_cut_line_failed',
	      _bb,
	      (_topology_info).topology_snap_tolerance);
	
	      RAISE NOTICE 'Try to add failed lines with retry to temp topo layer %', command_string;
	      EXECUTE command_string;
	  
	    -- Heal egdes  
	--    command_string := Format('SELECT topo_update.do_healedges_no_block(%1$L,%2$L,%3$L)', 
	--    border_topo_info.topology_name,_bb,outer_cell_boundary_lines);
	--    EXECUTE command_string;
	--    command_string = null;
	    
	         
	    -- Remome small polygons
	    -- TODO make check that do not intersect any cell border lines
	    
	      face_table_name = border_topo_info.topology_name || '.face';
	      
	      -- remove small polygons in temp in (_clean_info).min_area_to_keep
	      if (_clean_info).min_area_to_keep IS NOT NULL AND (_clean_info).min_area_to_keep > 0 THEN
		      start_time_delta_job := Clock_timestamp();
		      RAISE NOTICE 'Start clean small polygons for face_table_name % at %', face_table_name, Clock_timestamp();
		      call topo_update.do_remove_small_areas_no_block (
		      border_topo_info.topology_name, (_clean_info).min_area_to_keep, face_table_name, _bb,(_input_data).utm,outer_cell_boundary_lines);
		      used_time := (Extract(EPOCH FROM (Clock_timestamp() - start_time_delta_job)));
		      RAISE NOTICE 'Done clean small polygons for face_table_name % at % used_time: %', face_table_name, Clock_timestamp(), used_time;
		  END IF;
	    
	      --heal border edges removing small small polygins
	      --Do we need to this??????, only if we do simplify later, no we do simplify before, if change the code to do simplify in the topplogy layer we may need to do this.
	      --command_string := Format('SELECT topo_update.do_healedges_no_block(%1$L,%2$L,%3$L)', 
	      --border_topo_info.topology_name,_bb,outer_cell_boundary_lines);
	      --EXECUTE command_string;
	    
	    
	    command_string := Format('SELECT EXISTS(SELECT 1 from  %1$s.edge limit 1)',
	    border_topo_info.topology_name);
	
	    EXECUTE command_string into has_edges;
	    
	    IF (has_edges) THEN
	      command_string := Format('create unlogged table %1$s as 
	      (SELECT (ST_Dump(ST_LineMerge(ST_Union(geom)))).geom,
          %3$L::jsonb as column_data_as_json
	      from  %2$s.edge_data )',
	      has_edges_temp_table_name,
	      border_topo_info.topology_name,
	      null);
	      EXECUTE command_string;
	    END IF;
	    
	    execute Format('SET CONSTRAINTS ALL IMMEDIATE');
	    PERFORM topology.DropTopology (border_topo_info.topology_name);
	    -- STOP IF (_input_data).line_table_name IS null
    END IF;

    commit;

    IF (has_edges) THEN
    
       command_string := Format('SELECT topology.TopoGeo_addLinestring(%1$L,r.geom,%2$s) FROM 
       (SELECT geom from %3$s) as r 
       ORDER BY ST_X(ST_Centroid(r.geom)), ST_Y(ST_Centroid(r.geom))',
       (_topology_info).topology_name,
       (_topology_info).topology_snap_tolerance, 
       has_edges_temp_table_name);
       EXECUTE command_string;
    
       IF (_topology_info).create_topology_attrbute_tables = true and (_input_data).line_table_name is not null THEN
   
         command_string := Format('WITH lines_addes AS (
         SELECT DISTINCT ON(e.edge_id) e.edge_id, g.column_data_as_json
         FROM 
         %1$s as g, 
         %2$s as e 
         where ST_DWithin( e.geom, g.geom, %4$L) and
         ST_DWithin( ST_StartPoint(e.geom), g.geom, %4$L) and
         ST_DWithin( ST_EndPoint(e.geom), g.geom, %4$L)
         )
         INSERT INTO %5$s(%6$s,%3$s)  
         SELECT x.*,
         topology.CreateTopoGeom(%7$L,2,%8$L,ARRAY[ARRAY[ee.edge_id,2]]::topology.topoelementarray ) as %3$s
         FROM lines_addes ee, 
         jsonb_to_record(ee.column_data_as_json) AS x(%9$s)',
         has_edges_temp_table_name,
         (_topology_info).topology_name||'.edge_data',
         (_input_data).line_table_geo_collumn,
         (_topology_info).topology_snap_tolerance,
         (_topology_info).topology_name||'.edge_attributes',
         (_input_data).line_table_other_collumns_list,
         (_topology_info).topology_name,
         (_topology_info).topology_attrbute_tables_border_layer_id,
         (_input_data).line_table_other_collumns_def
         );
     	 EXECUTE command_string;

       END IF;
    
       command_string := Format('DROP TABLE IF EXISTS %s',has_edges_temp_table_name);
       EXECUTE command_string;

      END IF;

 
--- 
  ELSIF _cell_job_type = 1 and _loop_number > 1 THEN
	-- Added failed lines inside bbox from first loop number      
    has_edges_temp_table_name := (_topology_info).topology_name||'.edge_data_tmp_' || box_id;
    command_string := Format('SELECT EXISTS(SELECT 1 from to_regclass(%L) where to_regclass is not null)',has_edges_temp_table_name);
    EXECUTE command_string into has_edges;
    RAISE NOTICE 'cell % cell_job_type %, has_edges %, _loop_number %', box_id, _cell_job_type, has_edges, _loop_number;
 
    IF (has_edges) THEN
	       command_string := Format('SELECT topo_update.add_border_lines(%4$L,r.geom,%1$s,%5$L,FALSE) FROM 
	       (SELECT geom from %2$s) as r 
	       ORDER BY ST_X(ST_Centroid(r.geom)), ST_Y(ST_Centroid(r.geom))',
	       (_topology_info).topology_snap_tolerance, has_edges_temp_table_name, ST_ExteriorRing (_bb), (_topology_info).topology_name, _table_name_result_prefix);
	       EXECUTE command_string into line_edges_added;
	
	      command_string := Format('WITH topo_updated AS (
	      SELECT topo_update.add_border_lines(%1$L,r.geo,%2$s,%3$L,FALSE), geo 
	        FROM (
	          SELECT distinct (ST_Dump(ST_Multi(ST_LineMerge(ST_union(r.geo))))).geom as geo 
	            FROM (
	              select r.geo from %4$s r where ST_CoveredBy(r.geo, %5$L) and line_geo_lost = true
	            ) as r
	          ) as r
	      )
	      update %4$s u 
	      set line_geo_lost = false
	      FROM topo_updated tu
	      where ST_DWithin(tu.geo,u.geo,%6$s) and (SELECT bool_or(x IS NOT NULL) FROM unnest(tu.add_border_lines) x)' , 
	      (_topology_info).topology_name, 
	      (_topology_info).topology_snap_tolerance, 
	      _table_name_result_prefix,
	      _table_name_result_prefix||'_no_cut_line_failed',
	      _bb,
	      (_topology_info).topology_snap_tolerance);
	
	      RAISE NOTICE 'Try to add failed lines with no retry to master topo layer %', command_string;
	      EXECUTE command_string;
	      
	      command_string := Format('SELECT topo_update.do_healedges_no_block(%1$L,%2$L)', 
	      (_topology_info).topology_name, inner_cell_boundary_geom);
	      
	     IF (_topology_info).create_topology_attrbute_tables = true and (_input_data).line_table_name is not null THEN
   
	         command_string := Format('WITH lines_addes AS (
	         SELECT DISTINCT ON(e.edge_id) e.edge_id, g.column_data_as_json
	         FROM 
	         %1$s as g, 
	         %2$s as e 
	         where ST_DWithin( e.geom, g.geom, %4$L) and
	         ST_DWithin( ST_StartPoint(e.geom), g.geom, %4$L) and
	         ST_DWithin( ST_EndPoint(e.geom), g.geom, %4$L)
	         )
	         INSERT INTO %5$s(%6$s,%3$s)  
	         SELECT x.*,
	         topology.CreateTopoGeom(%7$L,2,%8$L,ARRAY[ARRAY[ee.edge_id,2]]::topology.topoelementarray ) as %3$s
	         FROM lines_addes ee, 
	         jsonb_to_record(ee.column_data_as_json) AS x(%9$s)',
	         has_edges_temp_table_name,
	         (_topology_info).topology_name||'.edge_data',
	         (_input_data).line_table_geo_collumn,
	         (_topology_info).topology_snap_tolerance,
	         (_topology_info).topology_name||'.edge_attributes',
	         (_input_data).line_table_other_collumns_list,
	         (_topology_info).topology_name,
	         (_topology_info).topology_attrbute_tables_border_layer_id,
	         (_input_data).line_table_other_collumns_def
	         );
	     	 EXECUTE command_string;

         END IF;

         command_string := Format('DROP TABLE IF EXISTS %s',has_edges_temp_table_name);
	     EXECUTE command_string;
	
    END IF;

    
 
  ELSIF _cell_job_type = 2 THEN
  -- Add border lines for small grids
  
		    command_string := Format('WITH s1 AS (SELECT * FROM %4$s r 
		    where r.geo && %5$L and ST_CoveredBy(r.geo, %5$L) and r.added_to_master = false 
		    ORDER BY ST_X(ST_Centroid(r.geo)), ST_Y(ST_Centroid(r.geo))
		    ), 
		    update_step AS (update %4$s su set added_to_master = true FROM s1 WHERE s1.id = su.id ) 
		    select topo_update.add_border_lines(%1$L,s1.geo,%2$s,%3$L,TRUE) from s1', 
		    (_topology_info).topology_name, 
		    (_topology_info).topology_snap_tolerance, 
		    _table_name_result_prefix,
		    _table_name_result_prefix||'_border_line_segments',
		    _bb);
		    EXECUTE command_string;

--		    _table_name_result_prefix||'_border_line_many_points',
		    command_string := Format('WITH s1 AS (SELECT * FROM %4$s r 
		    where r.geo && %5$L and ST_CoveredBy(r.geo, %5$L) and r.added_to_master = false 
		    ORDER BY ST_X(ST_Centroid(r.geo)), ST_Y(ST_Centroid(r.geo))
		    ), 
		    update_step AS (update %4$s su set added_to_master = true FROM s1 WHERE s1.id = su.id ) 
		    select topo_update.add_border_lines(%1$L,s1.geo,%2$s,%3$L,TRUE) from s1', 
		    (_topology_info).topology_name, 
		    (_topology_info).topology_snap_tolerance, 
		    _table_name_result_prefix,
		    _table_name_result_prefix||'_border_line_many_points',
		    _bb);
		    EXECUTE command_string;
		
         IF (_topology_info).create_topology_attrbute_tables = true and (_input_data).line_table_name is not null THEN

         	 command_string := Format('WITH lines_addes AS (
	         SELECT DISTINCT ON(e.edge_id) e.edge_id, g.column_data_as_json
	         FROM 
	         %1$s as g, 
	         %2$s as e 
	         where ST_CoveredBy(g.geo, %10$L) and g.added_to_master = false and
             ST_DWithin( e.geom, g.geo, %4$L) and
	         ST_DWithin( ST_StartPoint(e.geom), g.geo, %4$L) and
	         ST_DWithin( ST_EndPoint(e.geom), g.geo, %4$L)
	         )
	         INSERT INTO %5$s(%6$s,%3$s)  
	         SELECT x.*,
	         topology.CreateTopoGeom(%7$L,2,%8$L,ARRAY[ARRAY[ee.edge_id,2]]::topology.topoelementarray ) as %3$s
	         FROM lines_addes ee, 
	         jsonb_to_record(ee.column_data_as_json) AS x(%9$s)',
	         _table_name_result_prefix||'_border_line_segments',
	         (_topology_info).topology_name||'.edge_data',
	         (_input_data).line_table_geo_collumn,
	         (_topology_info).topology_snap_tolerance,
	         (_topology_info).topology_name||'.edge_attributes',
	         (_input_data).line_table_other_collumns_list,
	         (_topology_info).topology_name,
	         (_topology_info).topology_attrbute_tables_border_layer_id,
	         (_input_data).line_table_other_collumns_def,
	         _bb
	         );
	     	 EXECUTE command_string;
	     	 
	     	 command_string := Format('WITH lines_addes AS (
	         SELECT DISTINCT ON(e.edge_id) e.edge_id, g.column_data_as_json
	         FROM 
	         %1$s as g, 
	         %2$s as e 
	         where ST_CoveredBy(g.geo, %10$L) and g.added_to_master = false and
             ST_DWithin( e.geom, g.geo, %4$L) and
	         ST_DWithin( ST_StartPoint(e.geom), g.geo, %4$L) and
	         ST_DWithin( ST_EndPoint(e.geom), g.geo, %4$L)
	         )
	         INSERT INTO %5$s(%6$s,%3$s)  
	         SELECT x.*,
	         topology.CreateTopoGeom(%7$L,2,%8$L,ARRAY[ARRAY[ee.edge_id,2]]::topology.topoelementarray ) as %3$s
	         FROM lines_addes ee, 
	         jsonb_to_record(ee.column_data_as_json) AS x(%9$s)',
	         _table_name_result_prefix||'_border_line_many_points',
	         (_topology_info).topology_name||'.edge_data',
	         (_input_data).line_table_geo_collumn,
	         (_topology_info).topology_snap_tolerance,
	         (_topology_info).topology_name||'.edge_attributes',
	         (_input_data).line_table_other_collumns_list,
	         (_topology_info).topology_name,
	         (_topology_info).topology_attrbute_tables_border_layer_id,
	         (_input_data).line_table_other_collumns_def,
	         _bb
	         );
	     	 EXECUTE command_string;

	  END IF;


  ELSIF _cell_job_type = 3 THEN

 
     
  ELSIF _cell_job_type = 4 THEN
    -- heal border edges
    
     IF _loop_number < 1 THEN 
       -- In first loop only block by egdes
       command_string := Format('SELECT ST_Union(geom) from (SELECT ST_Expand(ST_Envelope(%1$s),%2$s) as geom from %3$s where ST_intersects(%1$s,%4$L) ) as r', 
       'geom', (_topology_info).topology_snap_tolerance, (_topology_info).topology_name||'.edge_data', _bb);
     ELSE
       -- In second loop block by input geo size
       command_string := Format('SELECT ST_Expand(ST_Envelope(ST_collect(%1$s)),%2$s) from %3$s where ST_intersects(%1$s,%4$L);', 
       (_input_data).polygon_table_geo_collumn, (_topology_info).topology_snap_tolerance, (_input_data).polygon_table_name, _bb);
       
     END IF;

    
    EXECUTE command_string INTO area_to_block;
    
    IF area_to_block is NULL or ST_Area(area_to_block) = 0.0 THEN
      area_to_block := _bb;
    END IF;

    command_string := Format('select count(*) from (select * from %1$s where cell_geo && %2$L and ST_intersects(cell_geo,%2$L) for update SKIP LOCKED) as r;', _job_list_name, area_to_block);
    EXECUTE command_string INTO num_boxes_free;
    
    command_string := Format('select count(*) from %1$s where cell_geo && %2$L and ST_intersects(cell_geo,%2$L);', _job_list_name, area_to_block);
    EXECUTE command_string INTO num_boxes_intersect;
    
    IF num_boxes_intersect != num_boxes_free THEN
      RAISE NOTICE 'Wait to handle add cell border edges for _cell_job_type %, num_boxes_intersect %, num_boxes_free %, for area_to_block % ',  
      _cell_job_type, num_boxes_intersect, num_boxes_free, area_to_block;
      RETURN;
    END IF;



    start_time_delta_job := Clock_timestamp();

    command_string := Format('SELECT topo_update.do_healedges_no_block(%1$L,%2$L)', 
      (_topology_info).topology_name, _bb);
    EXECUTE command_string;

    RAISE NOTICE 'Did Heal lines for topo % and bb % at % after added edges for border lines used_time %', 
    (_topology_info).topology_name, _bb, Clock_timestamp(), (Extract(EPOCH FROM (Clock_timestamp() - start_time_delta_job)));


  ELSIF _cell_job_type = 5 THEN

    command_string := Format('SELECT ST_Union(geom) from (select ST_Expand(ST_Envelope(%1$s),%2$s) as geom from %3$s where ST_intersects(%1$s,%4$L) ) as r', 
    'geom', (_topology_info).topology_snap_tolerance, (_topology_info).topology_name||'.edge_data', _bb);

    EXECUTE command_string INTO area_to_block;


    IF area_to_block is NULL or ST_Area(area_to_block) = 0.0 THEN
      area_to_block := _bb;
    END IF;

    command_string := Format('select count(*) from (select * from %1$s where cell_geo && %2$L and ST_intersects(cell_geo,%2$L) for update SKIP LOCKED) as r;', _job_list_name, area_to_block);
    EXECUTE command_string INTO num_boxes_free;

    command_string := Format('select count(*) from %1$s where cell_geo && %2$L and ST_intersects(cell_geo,%2$L);', _job_list_name, area_to_block);
    EXECUTE command_string INTO num_boxes_intersect;
    
    IF num_boxes_intersect != num_boxes_free THEN
      RAISE NOTICE 'Wait to handle add cell border edges for _cell_job_type %, num_boxes_intersect %, num_boxes_free %, for area_to_block % ',  
      _cell_job_type, num_boxes_intersect, num_boxes_free, area_to_block;
      RETURN;
    END IF;


    BEGIN 
    
    command_string := Format('CREATE TEMP table temp_left_over_borders as select geo FROM
    (select geo from topo_update.get_left_over_borders(%1$L,%2$L,%3$L,%4$L) as r) as r', 
    overlapgap_grid, (_input_data).polygon_table_geo_collumn, _bb, _table_name_result_prefix,(_topology_info).topology_snap_tolerance*inner_cell_distance);
    EXECUTE command_string;
    
    GET DIAGNOSTICS v_cnt_left_over_borders = ROW_COUNT;

     IF v_cnt_left_over_borders > 0 AND (_clean_info).simplify_tolerance > 0  THEN

      start_time_delta_job := Clock_timestamp();

      command_string := Format('SELECT ARRAY(SELECT e.edge_id   
        FROM (
        SELECT distinct e1.edge_id 
        FROM 
          %1$s.edge_data e1,
          temp_left_over_borders lb
        WHERE
   		ST_Intersects(lb.geo,e1.geom)) as e )',
        (_topology_info).topology_name);
        EXECUTE command_string into edgelist_to_change;

        
        IF edgelist_to_change IS NOT NULL AND (Array_length(edgelist_to_change, 1)) IS NOT NULL THEN 
          FOREACH edge_id_heal IN ARRAY edgelist_to_change 
          LOOP
            heal_edge_retry_num := 1;
            LOOP
              command_string := FORMAT('SELECT topo_update.try_ST_ChangeEdgeGeom(e.geom,%1$L,%4$L,%5$L,e.edge_id,ST_simplifyPreserveTopology(e.geom,%2$s)) 
              from %1$s.edge_data e where e.edge_id = %3$s',
              (_topology_info).topology_name, (_clean_info).simplify_tolerance/heal_edge_retry_num, edge_id_heal, (_clean_info).simplify_max_average_vertex_length, (_input_data).utm);
              EXECUTE command_string into heal_edge_status;
              EXIT WHEN heal_edge_status in (0,1) or heal_edge_retry_num > 5;
              heal_edge_retry_num := heal_edge_retry_num  + 1;
            END LOOP;
            IF heal_edge_status = -1 THEN
              RAISE NOTICE 'Failed to run topo_update.try_ST_ChangeEdgeGeom using ST_simplifyPreserveTopologyfor edge_id % for topology % using tolerance % .' , 
              edge_id_heal, border_topo_info.topology_name, (_clean_info).simplify_tolerance;
            END IF;
          END LOOP;
        END IF;

        RAISE NOTICE 'Did ST_simplifyPreserveTopology for topo % and bb % at % used_time %', 
       (_topology_info).topology_name, _bb, Clock_timestamp(), (Extract(EPOCH FROM (Clock_timestamp() - start_time_delta_job)));

    END IF;

    
    IF v_cnt_left_over_borders > 0 AND (_clean_info).chaikins_nIterations > 0 THEN
      start_time_delta_job := Clock_timestamp();

      command_string := Format('SELECT topo_update.try_ST_ChangeEdgeGeom(e.geom,%1$L,%6$L,%3$L,e.edge_id, 
      ST_simplifyPreserveTopology(topo_update.chaikinsAcuteAngle(e.geom,%3$L,%4$L), %5$s )) 
      FROM (
      SELECT distinct e1.edge_id, e1.geom 
        FROM 
          %1$s.edge_data e1,
          temp_left_over_borders lb
        WHERE
   		ST_Intersects(lb.geo,e1.geom)) e',
      (_topology_info).topology_name, _bb, (_input_data).utm, _clean_info, (_topology_info).topology_snap_tolerance/2,(_clean_info).simplify_max_average_vertex_length);
      EXECUTE command_string;
      RAISE NOTICE 'Did chaikinsAcuteAngle for topo % and bb % at % used_time %', 
      (_topology_info).topology_name, _bb, Clock_timestamp(), (Extract(EPOCH FROM (Clock_timestamp() - start_time_delta_job)));

    END IF;
    
       -- remove 
    face_table_name = (_topology_info).topology_name || '.face';
    start_time_delta_job := Clock_timestamp();
    RAISE NOTICE 'Start clean small polygons for border plygons face_table_name % at %', face_table_name, Clock_timestamp();
    -- remove small polygons in temp
    -- TODO 6 sould be based on other values
    call topo_update.do_remove_small_areas_no_block ((_topology_info).topology_name, (_clean_info).min_area_to_keep, face_table_name, ST_Expand(_bb,((_topology_info).topology_snap_tolerance * -6)),
      (_input_data).utm);
    used_time := (Extract(EPOCH FROM (Clock_timestamp() - start_time_delta_job)));
    RAISE NOTICE 'clean small polygons for after adding to main face_table_name % at % used_time: %', face_table_name, Clock_timestamp(), used_time;

    --drop table temp_left_over_borders;
    
    EXCEPTION
      WHEN OTHERS THEN
  RAISE NOTICE 'Do rollback at timeofday:% for layer %, with _cell_job_type % and box id % .', 
  Timeofday(), (_topology_info).topology_name || '_', _cell_job_type, box_id;

    ROLLBACK;
      RETURN;
    END;	    

  ELSIF _cell_job_type = 6 THEN
  
    command_string := Format('SELECT ST_Expand(ST_Envelope(ST_collect(%1$s)),%2$s) from %3$s where ST_intersects(%1$s,%4$L);', 
    (_input_data).polygon_table_geo_collumn, (_topology_info).topology_snap_tolerance, (_input_data).polygon_table_name, _bb);
    EXECUTE command_string INTO area_to_block;
    
    IF area_to_block is NULL or ST_Area(area_to_block) = 0.0 THEN
      area_to_block := _bb;
    END IF;

    command_string := Format('select count(*) from %1$s where cell_geo && %2$L and ST_intersects(cell_geo,%2$L);', _job_list_name, area_to_block);
    EXECUTE command_string INTO num_boxes_intersect;
    command_string := Format('select count(*) from (select * from %1$s where cell_geo && %2$L and ST_intersects(cell_geo,%2$L) for update SKIP LOCKED) as r;', _job_list_name, area_to_block);
    EXECUTE command_string INTO num_boxes_free;
    IF num_boxes_intersect != num_boxes_free THEN
      RAISE NOTICE 'Wait to handle add cell border edges for _cell_job_type %, num_boxes_intersect %, num_boxes_free %, for area_to_block % ',  
      _cell_job_type, num_boxes_intersect, num_boxes_free, area_to_block;
      RETURN;
    END IF;

    -- remove 
    face_table_name = (_topology_info).topology_name || '.face';
    start_time_delta_job := Clock_timestamp();
    RAISE NOTICE 'Start clean small polygons for cell plygons face_table_name % at %', face_table_name, Clock_timestamp();
    -- remove small polygons in temp
    -- TODO 6 sould be based on other values
    call topo_update.do_remove_small_areas_no_block ((_topology_info).topology_name, (_clean_info).min_area_to_keep, face_table_name, ST_Expand(_bb,((_topology_info).topology_snap_tolerance * -6)),
      (_input_data).utm);
    used_time := (Extract(EPOCH FROM (Clock_timestamp() - start_time_delta_job)));
    RAISE NOTICE 'clean small polygons for after adding to main face_table_name % at % used_time: %', face_table_name, Clock_timestamp(), used_time;


  ELSIF _cell_job_type = 7 THEN
    -- Create a temp table name
    temp_table_name := '_result_temp_' || box_id;
    temp_table_id_column := '_id' || temp_table_name;
    final_result_table_name := _table_name_result_prefix || '_result';

    -- Drop/Create a temp to hold data temporay for job
    -- EXECUTE Format('DROP TABLE IF EXISTS %s', temp_table_name);
    -- Create the temp for result simple feature result table  as copy of the input table
    EXECUTE Format('CREATE TEMP TABLE %s AS TABLE %s with NO DATA', temp_table_name, final_result_table_name);
    -- Add an extra column to hold a list of other intersections surfaces
    EXECUTE Format('ALTER TABLE %s ADD column %s serial', temp_table_name, temp_table_id_column);
    
    -- TODO How to find a unique column name ??
    EXECUTE Format('ALTER TABLE %s ADD column face_id int', temp_table_name, temp_table_id_column);
    -- Update cl
    command_string := Format('select 
 	  	array_to_string(array_agg(quote_ident(update_column)),%L) AS update_fields,
 	  	array_to_string(array_agg(%L||quote_ident(update_column)),%L) as update_fields_source
 		  FROM (
 		   SELECT distinct(json_object_keys) AS update_column
 		   FROM json_object_keys(to_json(json_populate_record(NULL::%s, %L::Json))) 
 		   where json_object_keys != %L and json_object_keys != %L and json_object_keys != %L and json_object_keys != %L and json_object_keys != %L  
 		  ) as keys', ',', 'r.', ',', 
 		  temp_table_name, '{}', 
 		  temp_table_id_column, (_input_data).polygon_table_geo_collumn, '_other_intersect_id_list', 'face_id','_input_geo_is_valid');
    RAISE NOTICE '% ', command_string;
    EXECUTE command_string INTO update_fields, update_fields_source;
    
    -- Insert new geos based on all face id do not check on input table
    command_string := Format('insert into %3$s(%5$s,face_id)
 	select * from (select (ST_Dump(topo_update.get_face_geo(%1$L,face_id,%7$s))).geom as %5$s, face_id from (
 	SELECT f.face_id, min(jl.id) as cell_id  FROM
 	%1$s.face f, 
 	%4$s jl 
 	WHERE f.mbr && %2$L and jl.cell_geo && f.mbr
 	GROUP BY f.face_id
 	) as r where cell_id = %6$s 
    ) as r where ST_IsValid(r.%5$s)', 
    (_topology_info).topology_name, _bb, temp_table_name, _table_name_result_prefix || '_job_list', (_input_data).polygon_table_geo_collumn, box_id,snap_tolerance_fixed);
    RAISE NOTICE 'command_string %', command_string;
    EXECUTE command_string;
    -- update/add primary key and _other_intersect_id_list based on geo
    command_string := Format('update %1$s t
 set (%3$s,_other_intersect_id_list,_input_geo_is_valid) = (r.%3$s,r._other_intersect_id_list,true) 
 from (
 	SELECT r.*, r.intersect_id_list[2:] as _other_intersect_id_list , r.intersect_id_list[1] as %3$s  from (
 		select distinct %5$s, array_agg(%3$s) OVER (PARTITION BY %5$s) as intersect_id_list from (
 		select %5$s, %3$s 
 			from (
              SELECT * FROM (   
                select %5$s, i.%3$s, abs(intersection_coverarge/new_geo_area) as area_coverarge
                FROM
                (
 				  SELECT %5$s, i.%3$s, ST_Area(ST_Collect(ST_Intersection(f.%4$s,i.%4$s))) as intersection_coverarge, ST_area(f.%4$s) as new_geo_area 
 				  FROM 
 				  %1$s f,
 				  %2$s i
 				  where f.%4$s && i.%4$s and ST_IsValid(f.%4$s) and ST_IsValid(i.%4$s) and ST_Intersects(f.%4$s,i.%4$s)
                  group by %5$s, i.%3$s, new_geo_area
                ) ii,
                %2$s i
                WHERE i.%3$s = ii.%3$s
              ) as r1
  			  order by r1.area_coverarge desc
 			) as r where area_coverarge > 0.5
 			order by %5$s, area_coverarge desc
 		) as r
 	) as r
 ) r where r.%5$s = t.%5$s', temp_table_name, (_input_data).polygon_table_name, (_input_data).polygon_table_pk_column, (_input_data).polygon_table_geo_collumn, temp_table_id_column);
 
 RAISE NOTICE 'upate attributes % ', command_string;
    
    EXECUTE command_string;
    
    
    -- Remove extra column column to hold a list of other intersections surfaces
    EXECUTE Format('ALTER TABLE %s DROP column %s', temp_table_name, temp_table_id_column);
    -- update/add primary key and _other_intersect_id_list based on geo
    command_string := Format('update %1$s t
 set (%4$s) = (%5$s) 
 from %2$s r
 where r.%3$s = t.%3$s', temp_table_name, (_input_data).polygon_table_name, (_input_data).polygon_table_pk_column, update_fields, update_fields_source);
    EXECUTE command_string;

    
    
    IF (_topology_info).create_topology_attrbute_tables = true and (_input_data).polygon_table_name is not null THEN

         command_string := Format('WITH lines_addes AS (
         SELECT face_id, to_jsonb(g)::jsonb - %3$s as column_data_as_json
         FROM 
         %1$s as g 
         )
         INSERT INTO %5$s(%6$s,%3$s)  
         SELECT x.*,
         topology.CreateTopoGeom(%7$L,3,%8$L,ARRAY[ARRAY[ee.face_id,3]]::topology.topoelementarray ) as %3$s
         FROM lines_addes ee, 
         jsonb_to_record(ee.column_data_as_json) AS x(%9$s)',
         temp_table_name,
         (_topology_info).topology_name||'.face',
         (_input_data).polygon_table_geo_collumn,
         (_topology_info).topology_snap_tolerance,
         (_topology_info).topology_name||'.face_attributes',
         (_input_data).polygon_table_other_collumns_list,
         (_topology_info).topology_name,
         (_topology_info).topology_attrbute_tables_surface_layer_id,
         (_input_data).polygon_table_other_collumns_def
         );
     	 EXECUTE command_string;
       
       
 
    ELSE
      EXECUTE Format('ALTER TABLE %s drop column face_id', temp_table_name, temp_table_id_column);

      command_string := Format('insert into %1$s select * from %2$s', final_result_table_name, temp_table_name);
      EXECUTE command_string;
    END IF;

    -- Drop/Create a temp to hold data temporay for job
    -- EXECUTE Format('DROP TABLE IF EXISTS %s', temp_table_name);
  ELSE
    RAISE EXCEPTION 'Invalid _cell_job_type % ', _cell_job_type;
  END IF;

  command_string := Format('update %1$s set block_bb = %2$L where cell_geo = %3$L', _job_list_name, _bb, _bb);
  EXECUTE command_string;
  
  done_time := Clock_timestamp();
  used_time := (Extract(EPOCH FROM (done_time - start_time)));
  RAISE NOTICE 'Work done for cell % (%) at % topologgy % and cell_job_type % , using % sec', 
  box_id, _bb, done_time, (_topology_info).topology_name , _cell_job_type, used_time;
  -- This is a list of lines that fails
  -- this is used for debug
  IF used_time > 60 THEN
    EXECUTE Format('INSERT INTO %s (execute_time, info, sql, geo) VALUES (%s, %L, %L, %L)', _table_name_result_prefix || '_long_time_log2', used_time, 'simplefeature_c2_topo_surface_border_retry', command_string, _bb);
  END IF;
  
  -- do analyse
  IF _cell_job_type < 3 THEN
    command_string := Format('select worker_id from %1$s where id = %2$s', _job_list_name, box_id);
    EXECUTE command_string INTO this_worker_id;
  
    -- 1599830100.882998 | 
    -- 1599835466.322007
    
--    SELECT (EXTRACT(EPOCH FROM TRANSACTION_TIMESTAMP()))- EXTRACT(EPOCH FROM (d.analyze_time)),d.analyze_time, d.done_time from  test_topo_jm_t2.jm_ukomm_flate_job_list_donejobs d

    IF this_worker_id = 1 THEN 
      command_string := Format('select count(d.*), 
      ((EXTRACT(EPOCH FROM now())-EXTRACT(EPOCH FROM max(d.analyze_time)))/60)::int time_diff 
      from 
      %1$s l,
      %2$s d
      where d.id = l.id and l.worker_id = %3$s',
      _job_list_name, 
      _job_list_name||'_donejobs', 
      this_worker_id,
      box_id);
      
      EXECUTE command_string INTO num_jobs_worker_id, num_min_since_last_analyze;
   
      -- Maybe find a better way for this, Do analyze the first 3 rounds and the at least once hour or more more of slowing down
      IF num_jobs_worker_id  < 3 or num_jobs_worker_id is null or
         num_min_since_last_analyze > num_jobs_worker_id*2 or
         num_min_since_last_analyze > 60
         THEN
        RAISE NOTICE 'Do analyze for % for num_jobs_worker_id % and box_id % snd _cell_job_type % and num_min_since_last_analyze % at %', 
        (_topology_info).topology_name, num_jobs_worker_id, box_id, _cell_job_type, num_min_since_last_analyze, now();

        EXECUTE Format('ANALYZE %s.node', (_topology_info).topology_name);
        EXECUTE Format('ANALYZE %s.relation', (_topology_info).topology_name);
        EXECUTE Format('ANALYZE %s.edge_data',(_topology_info).topology_name);
        EXECUTE Format('ANALYZE %s.face', (_topology_info).topology_name);
        
        analyze_done_at := TRANSACTION_TIMESTAMP();
      ELSE
        RAISE NOTICE 'Not Do analyze for % for num_jobs_worker_id % and box_id % snd _cell_job_type % and num_min_since_last_analyze % at %', 
        (_topology_info).topology_name, num_jobs_worker_id, box_id, _cell_job_type, num_min_since_last_analyze, now();
      END IF;
    END IF;
  END IF;


  command_string := Format('insert into %1$s(id,analyze_time) select gt.id, %4$L from %2$s as gt
       where ST_Equals(gt.cell_geo,%3$L)', 
       _job_list_name || '_donejobs',
       _job_list_name, 
       _bb,
       analyze_done_at);
  EXECUTE command_string;
 
  RAISE NOTICE 'leave work at timeofday:% for layer %, with _cell_job_type % for cell %', Timeofday(), border_topo_info.topology_name, _cell_job_type, box_id;
 
  
END
$$;


--truncate table test_topo_ar50_t11.ar50_utvikling_flate_job_list_donejobs;
--
--CALL resolve_overlap_gap_single_cell(
--  'sl_esh.ar50_utvikling_flate','geo','sl_sdeid','test_topo_ar50_t11.ar50_utvikling_flate',
--  'test_topo_ar50_t11',1,25833,'true',
--  '(300,9,500,3,140,120,240,3,35)',
--  'test_topo_ar50_t11.ar50_utvikling_flate_job_list','test_topo_ar50_t11.ar50_utvikling_flate_grid',
--  '0103000020E9640000010000000500000000000000B0A6074100000000F9F05A4100000000B0A607410000008013F75A4100000000006A08410000008013F75A4100000000006A084100000000F9F05A4100000000B0A6074100000000F9F05A41'
--  ,1,1);
--  
--CALL resolve_overlap_gap_single_cell(
--  'sl_esh.ar50_utvikling_flate','geo','sl_sdeid','test_topo_ar50_t11.ar50_utvikling_flate',
--  'test_topo_ar50_t11',1,25833,'true',
--  '(300,9,500,3,140,120,240,3,35)',
--  'test_topo_ar50_t11.ar50_utvikling_flate_job_list','test_topo_ar50_t11.ar50_utvikling_flate_grid',
--  '0103000020E9640000010000000500000000000000B0A60741000000C0EBED5A4100000000B0A6074100000000F9F05A41000000005808084100000000F9F05A410000000058080841000000C0EBED5A4100000000B0A60741000000C0EBED5A41'
--  ,1,1);


--truncate table test_topo_ar50_t11.ar50_utvikling_flate_job_list_donejobs;
--
--CALL resolve_overlap_gap_single_cell(
--  'sl_esh.ar50_utvikling_flate','geo','sl_sdeid','test_topo_ar50_t11.ar50_utvikling_flate',
--  'test_topo_ar50_t11',1,25833,'true',
--  '(300,9,500,3,140,120,240,3,35)',
--  'test_topo_ar50_t11.ar50_utvikling_flate_job_list','test_topo_ar50_t11.ar50_utvikling_flate_grid',
--  '0103000020E9640000010000000500000000000000B0A6074100000000F9F05A4100000000B0A607410000008013F75A4100000000006A08410000008013F75A4100000000006A084100000000F9F05A4100000000B0A6074100000000F9F05A41'
--  ,2,1);
--  
--CALL resolve_overlap_gap_single_cell(
--  'sl_esh.ar50_utvikling_flate','geo','sl_sdeid','test_topo_ar50_t11.ar50_utvikling_flate',
--  'test_topo_ar50_t11',1,25833,'true',
--  '(300,9,500,3,140,120,240,3,35)',
--  'test_topo_ar50_t11.ar50_utvikling_flate_job_list','test_topo_ar50_t11.ar50_utvikling_flate_grid',
--  '0103000020E9640000010000000500000000000000B0A60741000000C0EBED5A4100000000B0A6074100000000F9F05A41000000005808084100000000F9F05A410000000058080841000000C0EBED5A4100000000B0A60741000000C0EBED5A41'
--  ,2,1);
--
--truncate table test_topo_ar50_t11.ar50_utvikling_flate_job_list_donejobs;
--
--CALL resolve_overlap_gap_single_cell(
--  'sl_esh.ar50_utvikling_flate','geo','sl_sdeid','test_topo_ar50_t11.ar50_utvikling_flate',
--  'test_topo_ar50_t11',1,25833,'true',
--  '(300,9,500,3,140,120,240,3,35)',
--  'test_topo_ar50_t11.ar50_utvikling_flate_job_list','test_topo_ar50_t11.ar50_utvikling_flate_grid',
--  '0103000020E964000001000000050000000000000050A6074100000000F6F05A410000000050A6074100000000FCF05A4100000000B808084100000000FCF05A4100000000B808084100000000F6F05A410000000050A6074100000000F6F05A41'
--  ,3,1);
--  
--SELECT topo_update.do_healedges_no_block('test_topo_ar50_t11','0103000020E964000001000000050000000000000050A6074100000000F6F05A410000000050A6074100000000FCF05A4100000000B808084100000000FCF05A4100000000B808084100000000F6F05A410000000050A6074100000000F6F05A41');


    --perform pg_sleep(1);


--truncate table test_topo_ar50_t11.ar50_utvikling_flate_job_list_donejobs;

--CALL resolve_overlap_gap_single_cell(
--   'sl_kbj.trl_2019_test_segmenter_mindredata','geo','gid','topo_sr16_mdata_05.trl_2019_test_segmenter_mindredata',                                                                                                                                                                                                                                                   
--    'topo_sr16_mdata_05',1,25833,'true',                                                                                                                                                                                                                                                                                                                               
--    '(49,5,10000,3,25,120,240,3,35)',                                                                                                                                                                                                                                                                                                                                  
--    'topo_sr16_mdata_05.trl_2019_test_segmenter_mindredata_job_list','topo_sr16_mdata_05.trl_2019_test_segmenter_mindredata_grid',
--    '0103000020E9640000010000000500000000000000A0EA0A4162A964474BDD5A4100000000A0EA0A4142C6ED8430E25A4100000000B49E0B4142C6ED8430E25A4100000000B49E0B4162A964474BDD5A4100000000A0EA0A4162A964474BDD5A41'
--  ,1,1);
  
