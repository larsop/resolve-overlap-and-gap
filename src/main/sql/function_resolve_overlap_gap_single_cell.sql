CREATE OR REPLACE PROCEDURE resolve_overlap_gap_single_cell (
input_table_name character varying, 
input_table_geo_column_name character varying, 
input_table_pk_column_name character varying, 
_table_name_result_prefix varchar, 
_topology_name character varying, 
_topology_snap_tolerance float, -- this is tolerance used as base when creating the the postgis topolayer
_srid int, 
_utm boolean, 
_clean_info resolve_overlap_data_clean_type, -- different parameters used if need to clean up your data
--(_clean_info).simplify_tolerance float, -- is this is more than zero simply will called with
--(_clean_info).do_chaikins boolean, -- here we will use chaikins togehter with simply to smooth lines
--(_clean_info).min_area_to_keep float, -- if this a polygon  is below this limit it will merge into a neighbour polygon. The area is sqare meter. 
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
  snap_tolerance_fixed float = _topology_snap_tolerance;
  
  glue_snap_tolerance_fixed float = 0;
  
  min_length_line float = (_clean_info).min_area_to_keep/1000;
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


BEGIN
	
  command_string := Format('select id from %1$s where cell_geo = %2$L', _job_list_name, _bb);
  RAISE NOTICE '% ', command_string;
  EXECUTE command_string INTO box_id;

  RAISE NOTICE 'enter at timeofday:% for layer %, with _cell_job_type % and box id % .', 
  Timeofday(), _topology_name || '_', _cell_job_type, box_id;
  
  
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
      SELECT ST_ExteriorRing (ST_Expand (_bb, ((_topology_snap_tolerance) * -inner_cell_distance))) AS outer_ring));

  outer_cell_boundary_geom := ST_MakePolygon ((
      SELECT ST_ExteriorRing (ST_Expand (_bb, _topology_snap_tolerance/1.5)) AS outer_ring), ARRAY (
        SELECT ST_ExteriorRing (inner_cell_geom) AS inner_rings));
  -- this cause missing faces so we expand the boubdery     
  -- outer_cell_boundary_geom := ST_MakePolygon ((
  --    SELECT ST_ExteriorRing (_bb) AS outer_ring), ARRAY (
  --      SELECT ST_ExteriorRing (inner_cell_geom) AS inner_rings));

  inner_cell_boundary_geom := ST_MakePolygon ((
      SELECT ST_ExteriorRing (ST_Expand (_bb, ((_topology_snap_tolerance) * -(inner_cell_distance/2)))) AS outer_ring), ARRAY (
        SELECT ST_ExteriorRing (ST_Expand (_bb, ((_topology_snap_tolerance) * -(inner_cell_distance*2)))) AS inner_rings));

  -- get area to block and set
  -- I don't see why we need this code ??????????? why cant we just the _bb as it is so I test thi snow
  area_to_block := _bb;
  -- area_to_block := resolve_overlap_gap_block_cell(input_table_name, input_table_geo_column_name, input_table_pk_column_name, _job_list_name, _bb);
  -- RAISE NOTICE 'area to block:% ', area_to_block;
  border_topo_info.snap_tolerance := _topology_snap_tolerance;
  
  RAISE NOTICE 'start work at timeofday:% for layer %, _topology_snap_tolerance %, with _cell_job_type % and min_length_line %s, (_clean_info).chaikins_max_degrees) %', 
  Timeofday(), _topology_name || '_' || box_id, _topology_snap_tolerance, _cell_job_type, min_length_line, (_clean_info).chaikins_max_degrees;
  
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
    
   
    
  IF _cell_job_type = 1 THEN
    -- get the siple feature data both the line_types and the inner lines.
    -- the boundery linnes are saved in a table for later usage
    
    command_string := Format('create temp table tmp_simplified_border_lines as 
    (select g.geo, g.outer_border_line FROM topo_update.get_simplified_border_lines(%L,%L,%L,%L,%L) g)', 
    input_table_name, input_table_geo_column_name, _bb, _topology_snap_tolerance, _table_name_result_prefix);
    EXECUTE command_string ;
    
    command_string := Format('SELECT ST_SetSRID(ST_Multi(ST_Union(geo)),%1$s)::Geometry(MultiPoint, %1$s) from tmp_simplified_border_lines g where outer_border_line = true',
    _srid);
    EXECUTE command_string into outer_cell_boundary_lines ;
    
    IF ST_NumGeometries(outer_cell_boundary_lines) = 0 THEN
     outer_cell_boundary_lines  := null;
    END IF; 
    

    
    IF (_clean_info).simplify_tolerance > 0  THEN
        command_string := Format('UPDATE tmp_simplified_border_lines l
        SET geo = ST_simplifyPreserveTopology(l.geo,%1$s)
        WHERE NOT ST_DWithin(%2$L,l.geo,%3$s)',
        (_clean_info).simplify_tolerance , outer_cell_boundary_lines,snap_tolerance_fixed);
        EXECUTE command_string;
    END IF;
    
    IF (_clean_info).chaikins_nIterations > 0 THEN
        command_string := Format('UPDATE tmp_simplified_border_lines l
        SET geo = ST_simplifyPreserveTopology(topo_update.chaikinsAcuteAngle(l.geo,%1$L,%2$L), %3$s)
        WHERE NOT ST_DWithin(%4$L,l.geo,%5$s)',
        _utm,
        _clean_info,
        _topology_snap_tolerance/2,
        outer_cell_boundary_lines,
        snap_tolerance_fixed);
        EXECUTE command_string;
    END IF;

    
    border_topo_info.topology_name := _topology_name || '_' || box_id;
    RAISE NOTICE 'use border_topo_info.topology_name %', border_topo_info.topology_name;
    
    IF ((SELECT Count(*) FROM topology.topology WHERE name = border_topo_info.topology_name) = 1) THEN
       EXECUTE Format('SELECT topology.droptopology(%s)', Quote_literal(border_topo_info.topology_name));
    END IF;
    --drop this schema in case it exists
    EXECUTE Format('DROP SCHEMA IF EXISTS %s CASCADE', border_topo_info.topology_name);
 
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

    
    -- using the input tolreance for adding
    border_topo_info.snap_tolerance := snap_tolerance_fixed;
    --command_string := Format('SELECT topo_update.create_nocutline_edge_domain_obj_retry(json::Text, %L) from tmp_simplified_border_lines g where line_type = 0 order by is_closed desc, num_points desc', border_topo_info);
    --RAISE NOTICE 'command_string %', command_string;
    command_string := Format('SELECT topo_update.add_border_lines(%1$L,r.geom,%2$s,%3$L,TRUE) FROM (select geo as geom from tmp_simplified_border_lines g where outer_border_line = false) as r',
    border_topo_info.topology_name, border_topo_info.snap_tolerance, _table_name_result_prefix);
    EXECUTE command_string;

 

    EXECUTE Format('ANALYZE %s.node', border_topo_info.topology_name);
    EXECUTE Format('ANALYZE %s.relation', border_topo_info.topology_name);
    EXECUTE Format('ANALYZE %s.edge_data', border_topo_info.topology_name);


      
    -- Heal egdes  
    command_string := Format('SELECT topo_update.do_healedges_no_block(%1$L,%2$L,%3$L)', 
    border_topo_info.topology_name,_bb,outer_cell_boundary_lines);
    EXECUTE command_string;
    command_string = null;
    
         
    -- Remome small polygons
    -- TODO make check that do not intersect any cell border lines
      face_table_name = border_topo_info.topology_name || '.face';
      start_time_delta_job := Clock_timestamp();
      RAISE NOTICE 'Start clean small polygons for face_table_name % at %', face_table_name, Clock_timestamp();
      -- remove small polygons in temp
      num_rows_removed := topo_update.do_remove_small_areas_no_block (
      border_topo_info.topology_name, (_clean_info).min_area_to_keep, face_table_name, _bb,_utm,outer_cell_boundary_lines);
    
      used_time := (Extract(EPOCH FROM (Clock_timestamp() - start_time_delta_job)));
      RAISE NOTICE 'Removed % clean small polygons for face_table_name % at % used_time: %', num_rows_removed, face_table_name, Clock_timestamp(), used_time;
    
      --heal border edges removing small small polygins
      --Do we need to this??????, only if we do simplify later, no we do simplify before, if change the code to do simplify in the topplogy layer we may need to do this.
      --command_string := Format('SELECT topo_update.do_healedges_no_block(%1$L,%2$L,%3$L)', 
      --border_topo_info.topology_name,_bb,outer_cell_boundary_lines);
      --EXECUTE command_string;
    
    
    command_string := Format('SELECT EXISTS(SELECT 1 from  %1$s.edge limit 1)',
    border_topo_info.topology_name);

    EXECUTE command_string into has_edges;
    IF (has_edges) THEN
      has_edges_temp_table_name := _topology_name||'.edge_data_tmp_' || box_id;

      IF (_utm = false) THEN
       command_string := Format('create unlogged table %1$s as 
       (SELECT geom, ST_IsClosed(geom) as is_closed, ST_NPoints(geom) as num_points 
       from  %2$s.edge_data where ST_Length(geom,true) >= %3$s)',
       has_edges_temp_table_name,
       border_topo_info.topology_name,
       min_length_line);
      ELSE
       command_string := Format('create unlogged table %1$s as 
       (SELECT geom, ST_IsClosed(geom) as is_closed, ST_NPoints(geom) as num_points 
       from  %2$s.edge_data where ST_Length(geom) >= %3$s)',
       has_edges_temp_table_name,
       border_topo_info.topology_name,
       min_length_line);
      END IF;

      
      EXECUTE command_string;
      
      EXECUTE Format('CREATE INDEX ON %s(is_closed,num_points)', has_edges_temp_table_name);

      
    END IF;
    execute Format('SET CONSTRAINTS ALL IMMEDIATE');
    PERFORM topology.DropTopology (border_topo_info.topology_name);
    

    -- DROP TABLE IF EXISTS tmp_simplified_border_lines;
 
  ELSIF _cell_job_type = 2 THEN
   -- add lines from each cell to final Topology layer
   -- this lines will not connect to any line outside each cell

   -- This where typically get core dump on this tread. 
   -- On vroom2 runing with 18 threads it works OK, but if we increase this to 28 threads the server core dumps 90% of times, 
   -- when it starts a this stage 
   -- Added a block command to se if coould help on Segmentation fault, but it does seem to help that much, maybe some
   
   --I also cheked that no threads area handling the same cell by greping in hte logs
   --start work at timeofday:Wed May 13 07:56:51.546346 2020 CEST for X topo_sr16_mdata_05_136, _topology_snap_tolerance 1, with _cell_job_type 2 and min_length_line 0.049s, (_clean_info).chaikins_max_degrees) 240
   --start work at timeofday:Wed May 13 07:56:51.555409 2020 CEST for X topo_sr16_mdata_05_139, _topology_snap_tolerance 1, with _cell_job_type 2 and min_length_line 0.049s, (_clean_info).chaikins_max_degrees) 240
   --start work at timeofday:Wed May 13 07:56:51.572900 2020 CEST for X topo_sr16_mdata_05_141, _topology_snap_tolerance 1, with _cell_job_type 2 and min_length_line 0.049s, (_clean_info).chaikins_max_degrees) 240
   --start work at timeofday:Wed May 13 07:56:51.475363 2020 CEST for X topo_sr16_mdata_05_14, _topology_snap_tolerance 1, with _cell_job_type 2 and min_length_line 0.049s, (_clean_info).chaikins_max_degrees) 240
   --start work at timeofday:Wed May 13 07:56:51.585137 2020 CEST for X topo_sr16_mdata_05_182, _topology_snap_tolerance 1, with _cell_job_type 2 and min_length_line 0.049s, (_clean_info).chaikins_max_degrees) 240
   --start work at timeofday:Wed May 13 07:56:51.493934 2020 CEST for X topo_sr16_mdata_05_185, _topology_snap_tolerance 1, with _cell_job_type 2 and min_length_line 0.049s, (_clean_info).chaikins_max_degrees) 240
   --start work at timeofday:Wed May 13 07:56:51.569853 2020 CEST for X topo_sr16_mdata_05_191, _topology_snap_tolerance 1, with _cell_job_type 2 and min_length_line 0.049s, (_clean_info).chaikins_max_degrees) 240
   --start work at timeofday:Wed May 13 07:56:51.536768 2020 CEST for X topo_sr16_mdata_05_198, _topology_snap_tolerance 1, with _cell_job_type 2 and min_length_line 0.049s, (_clean_info).chaikins_max_degrees) 240
   --start work at timeofday:Wed May 13 07:56:51.558216 2020 CEST for X topo_sr16_mdata_05_199, _topology_snap_tolerance 1, with _cell_job_type 2 and min_length_line 0.049s, (_clean_info).chaikins_max_degrees) 240
   --start work at timeofday:Wed May 13 07:56:51.534759 2020 CEST for X topo_sr16_mdata_05_204, _topology_snap_tolerance 1, with _cell_job_type 2 and min_length_line 0.049s, (_clean_info).chaikins_max_degrees) 240
   --start work at timeofday:Wed May 13 07:56:51.527720 2020 CEST for X topo_sr16_mdata_05_224, _topology_snap_tolerance 1, with _cell_job_type 2 and min_length_line 0.049s, (_clean_info).chaikins_max_degrees) 240
   --start work at timeofday:Wed May 13 07:56:51.501900 2020 CEST for X topo_sr16_mdata_05_237, _topology_snap_tolerance 1, with _cell_job_type 2 and min_length_line 0.049s, (_clean_info).chaikins_max_degrees) 240
   --start work at timeofday:Wed May 13 07:56:51.485634 2020 CEST for X topo_sr16_mdata_05_258, _topology_snap_tolerance 1, with _cell_job_type 2 and min_length_line 0.049s, (_clean_info).chaikins_max_degrees) 240
   --start work at timeofday:Wed May 13 07:56:51.535268 2020 CEST for X topo_sr16_mdata_05_288, _topology_snap_tolerance 1, with _cell_job_type 2 and min_length_line 0.049s, (_clean_info).chaikins_max_degrees) 240
   --start work at timeofday:Wed May 13 07:56:51.500788 2020 CEST for X topo_sr16_mdata_05_346, _topology_snap_tolerance 1, with _cell_job_type 2 and min_length_line 0.049s, (_clean_info).chaikins_max_degrees) 240
   --start work at timeofday:Wed May 13 07:56:51.575592 2020 CEST for X topo_sr16_mdata_05_355, _topology_snap_tolerance 1, with _cell_job_type 2 and min_length_line 0.049s, (_clean_info).chaikins_max_degrees) 240
   --start work at timeofday:Wed May 13 07:56:51.478700 2020 CEST for X topo_sr16_mdata_05_366, _topology_snap_tolerance 1, with _cell_job_type 2 and min_length_line 0.049s, (_clean_info).chaikins_max_degrees) 240
   --start work at timeofday:Wed May 13 07:56:51.505508 2020 CEST for X topo_sr16_mdata_05_369, _topology_snap_tolerance 1, with _cell_job_type 2 and min_length_line 0.049s, (_clean_info).chaikins_max_degrees) 240
   --start work at timeofday:Wed May 13 07:56:51.496298 2020 CEST for X topo_sr16_mdata_05_371, _topology_snap_tolerance 1, with _cell_job_type 2 and min_length_line 0.049s, (_clean_info).chaikins_max_degrees) 240
   --start work at timeofday:Wed May 13 07:56:51.510623 2020 CEST for X topo_sr16_mdata_05_376, _topology_snap_tolerance 1, with _cell_job_type 2 and min_length_line 0.049s, (_clean_info).chaikins_max_degrees) 240
   --start work at timeofday:Wed May 13 07:56:51.475735 2020 CEST for X topo_sr16_mdata_05_435, _topology_snap_tolerance 1, with _cell_job_type 2 and min_length_line 0.049s, (_clean_info).chaikins_max_degrees) 240
   --start work at timeofday:Wed May 13 07:56:51.541407 2020 CEST for X topo_sr16_mdata_05_451, _topology_snap_tolerance 1, with _cell_job_type 2 and min_length_line 0.049s, (_clean_info).chaikins_max_degrees) 240
   --start work at timeofday:Wed May 13 07:56:51.488792 2020 CEST for X topo_sr16_mdata_05_485, _topology_snap_tolerance 1, with _cell_job_type 2 and min_length_line 0.049s, (_clean_info).chaikins_max_degrees) 240
   --start work at timeofday:Wed May 13 07:56:51.484015 2020 CEST for X topo_sr16_mdata_05_50, _topology_snap_tolerance 1, with _cell_job_type 2 and min_length_line 0.049s, (_clean_info).chaikins_max_degrees) 240
   --start work at timeofday:Wed May 13 07:56:51.537174 2020 CEST for X topo_sr16_mdata_05_531, _topology_snap_tolerance 1, with _cell_job_type 2 and min_length_line 0.049s, (_clean_info).chaikins_max_degrees) 240
   --start work at timeofday:Wed May 13 07:56:51.480665 2020 CEST for X topo_sr16_mdata_05_539, _topology_snap_tolerance 1, with _cell_job_type 2 and min_length_line 0.049s, (_clean_info).chaikins_max_degrees) 240
   --start work at timeofday:Wed May 13 07:56:51.496305 2020 CEST for X topo_sr16_mdata_05_543, _topology_snap_tolerance 1, with _cell_job_type 2 and min_length_line 0.049s, (_clean_info).chaikins_max_degrees) 240
   --start work at timeofday:Wed May 13 07:56:51.528266 2020 CEST for X topo_sr16_mdata_05_6, _topology_snap_tolerance 1, with _cell_job_type 2 and min_length_line 0.049s, (_clean_info).chaikins_max_degrees) 240
   --start work at timeofday:Wed May 13 07:56:51.558211 2020 CEST for X topo_sr16_mdata_05_92, _topology_snap_tolerance 1, with _cell_job_type 2 and min_length_line 0.049s, (_clean_info).chaikins_max_degrees) 240
   --start work at timeofday:Wed May 13 07:56:51.475735 2020 CEST for X topo_sr16_mdata_05_94, _topology_snap_tolerance 1, with _cell_job_type 2 and min_length_line 0.049s, (_clean_info).chaikins_max_degrees) 240
   
   -- And I cheked that no lines where crossing the cell borders
   --SELECT f.* FROM 
   --topo_sr16_mdata_05.face f,
   --(SELECT ST_ExteriorRing(geo) as  geo from topo_sr16_mdata_05.trl_2019_test_segmenter_mindredata_grid) g
   --where ST_Intersects(f.mbr,g.geo)
   --;
   -- face_id | mbr 
   ---------+-----
   --(0 rows)
   
    area_to_block := _bb;

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

    has_edges_temp_table_name := _topology_name||'.edge_data_tmp_' || box_id;
    command_string := Format('SELECT EXISTS(SELECT 1 from to_regclass(%L) where to_regclass is not null)',has_edges_temp_table_name);
    
    EXECUTE command_string into has_edges;
    RAISE NOTICE 'cell % cell_job_type %, has_edges %, _loop_number %', box_id, _cell_job_type, has_edges, _loop_number;
    IF (has_edges) THEN
     IF _loop_number = 1 THEN 
       -- TODO fix added edges to be correct
       command_string := Format('SELECT ARRAY(SELECT topology.TopoGeo_addLinestring(%3$L,r.geom,%1$s)) FROM 
                                 (SELECT geom from %2$s order by is_closed desc, num_points desc) as r', _topology_snap_tolerance, has_edges_temp_table_name, _topology_name);
     ELSE
       --< postgres, 2020-03-20 13:38:33 CET, resolve_cha, 2020-03-20 13:38:33.920 CET >ERROR:  cannot accumulate null arrays
       --command_string := Format('SELECT ARRAY_AGG(topo_update.add_border_lines(%4$L,r.geom,%1$s,%5$L)) FROM (SELECT geom from %2$s order by is_closed desc, num_points desc) as r', _topology_snap_tolerance, has_edges_temp_table_name, ST_ExteriorRing (_bb), _topology_name, _table_name_result_prefix);
       command_string := Format('SELECT topo_update.add_border_lines(%4$L,r.geom,%1$s,%5$L,FALSE) FROM (SELECT geom from %2$s order by is_closed desc, num_points desc) as r',
       _topology_snap_tolerance, has_edges_temp_table_name, ST_ExteriorRing (_bb), _topology_name, _table_name_result_prefix);
       
     END IF;
     EXECUTE command_string into line_edges_added;

     command_string := Format('DROP TABLE IF EXISTS %s',has_edges_temp_table_name);
     EXECUTE command_string;

     command_string := Format('SELECT topo_update.do_healedges_no_block(%1$L,%2$L)', 
     _topology_name, inner_cell_boundary_geom);
--     EXECUTE command_string;

-- >WARNING:  terminating connection because of crash of another server process at character 29
--< postgres, 2020-03-25 09:41:04 CET, resolve_cha, 2020-03-25 09:41:04.879 CET >DETAIL:  The postmaster has commanded this server process to roll back the current transaction and exit, because another server process exited abnormally and possibly corrupted shared memory.
--   Failes with      
--     command_string := Format('SELECT topo_update.heal_cellborder_edges_no_block(%1$L,%2$L,%3$L)', 
--      _topology_name, inner_cell_geom,null);
--     EXECUTE command_string;

    END IF;
    
        EXCEPTION
      WHEN OTHERS THEN
  RAISE NOTICE 'Do rollback at timeofday:% for layer %, with _cell_job_type % and box id % .', 
  Timeofday(), _topology_name || '_', _cell_job_type, box_id;

    ROLLBACK;
      RETURN;
    END;	    


  ELSIF _cell_job_type = 3 THEN
  
  ---- test smooth border lines before adding them
---- Todo find a way smooth the rest off the lines
 
--  IF (_clean_info).simplify_tolerance > 0  THEN
--       command_string := Format('UPDATE temp_left_over_borders l
--       SET geo = ST_simplifyPreserveTopology(l.geo,%1$s)',
--       (_clean_info).simplify_tolerance);
--       EXECUTE command_string;
--   END IF;
    
--  IF (_clean_info).chaikins_nIterations > 0 THEN
--       command_string := Format('UPDATE temp_left_over_borders l
--       SET geo = ST_simplifyPreserveTopology(topo_update.chaikinsAcuteAngle(l.geo,%1$L,%2$L), %3$s)',
--       _utm,
--       _clean_info,
--       _topology_snap_tolerance/2);
--        EXECUTE command_string;
--    END IF;
    -- add lines that connects each cell to each other
    
    -- on cell border
    -- test with  area to block like _bb
    -- area_to_block := _bb;
    -- count the number of rows that intersects


    command_string := Format('CREATE TEMP table temp_left_over_borders as select geo FROM
    (select geo from topo_update.get_left_over_borders(%1$L,%2$L,%3$L,%4$L) as r) as r', 
    overlapgap_grid, input_table_geo_column_name, _bb, _table_name_result_prefix,_topology_snap_tolerance*inner_cell_distance);
    
    RAISE NOTICE 'command_string1 % ',  command_string;
    
    EXECUTE command_string;


     -- Anohter test block base on mbr Sandro https://trac.osgeo.org/postgis/ticket/4684
     -- Causes topology errors most of the times
     -- This is just keep because the best be able bøock this based data in postgis topology layer
     command_string := Format('WITH 
                              edge_line AS 
                              (
                                SELECT distinct e.geom as geom 
                                FROM 
                                temp_left_over_borders i, 
                                %1$s.edge_data e
                                WHERE ST_DWithin(i.geo,e.geom,%2$s) 
                                UNION 
                                SELECT i.geo AS geom
                                FROM temp_left_over_borders i
                              ),
                              edge_bb AS
                              (
                                SELECT ST_Union(ST_Envelope(e.geom)) as geom FROM edge_line e
                              ),
                              face_1 AS 
                              (
                                SELECT distinct f.mbr as geom 
                                FROM 
                                edge_bb i, 
                                %1$s.face f
                                WHERE ST_DWithin(i.geom,f.mbr,%2$s) 
                                UNION 
                                SELECT i.geom
                                FROM edge_bb i
                              ),
                              face_2 AS 
                              (
                                SELECT distinct f.mbr as geom 
                                FROM 
                                face_1 i, 
                                %1$s.face f
                                WHERE ST_DWithin(i.geom,f.mbr,%2$s) 
                                UNION 
                                SELECT i.geom
                                FROM edge_bb i
                              ),
                              edge_2 AS 
                              (
                                SELECT distinct e.geom as geom 
                                FROM 
                                face_2 i, 
                                %1$s.edge_data e
                                WHERE ST_DWithin(i.geom,e.geom,%2$s) 
                                UNION 
                                SELECT i.geom
                                FROM face_2 i
                              ),
                              final_block AS
                              (
                                SELECT ST_Envelope(ST_Collect(ST_Expand(e.geom,%2$s))) as geom FROM edge_2 e
                              )
                              SELECT ST_Multi(geom) FROM final_block i', 
    _topology_name,snap_tolerance_fixed,_bb);

   
    -- We try to input as blocking
    command_string := Format('WITH 
                              direct_intersect AS 
                              (
                                SELECT ST_Envelope(ST_Collect(geom)) as geom FROM 
                                (
                                  SELECT distinct od.%5$s as geom 
                                  FROM 
                                  temp_left_over_borders i, 
                                  %4$s od
                                  WHERE ST_DWithin(i.geo,od.%5$s,%2$s) 
                                  UNION 
                                  SELECT i.geo AS geom
                                  FROM temp_left_over_borders i
                                ) as r
                              ),
                              in_direct_intersect AS
                              (
                                SELECT ST_Envelope(ST_Collect(geom)) as geom FROM 
                                (
                                  SELECT distinct od.%5$s as geom 
                                  FROM 
                                  direct_intersect i, 
                                  %4$s od
                                  WHERE ST_DWithin(i.geom,od.%5$s,%2$s) 
                                  UNION 
                                  SELECT i.geo AS geom
                                  FROM temp_left_over_borders i
                                ) as r
                              ),
                              final_block AS
                              (
                                SELECT ST_Envelope(ST_Collect(ST_Expand(e.geom,%2$s))) as geom FROM in_direct_intersect e
                              )
                              SELECT ST_Multi(geom) FROM final_block i', 
    _topology_name,snap_tolerance_fixed,_bb,
    input_table_name, input_table_geo_column_name);


	-- RAISE NOTICE 'command_string2 % ',  command_string;
    -- EXECUTE command_string INTO area_to_block;
    
     
    IF area_to_block is NULL or ST_Area(area_to_block) = 0.0 THEN
       RAISE NOTICE 'Failed to make block for _cell_job_type %, num_boxes_intersect %, num_boxes_free %',  
      _cell_job_type, num_boxes_intersect, num_boxes_free;
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

    
    
 
 
     -- add rowlevel lock based info from Sandro https://trac.osgeo.org/postgis/ticket/4684
     --A pessimistic approach might lock:
     --EVERY FACE whos MBR intersects the input line
     --EVERY EDGE having any of those faces on its right or left side
     --EVERY ISOLATED NODE within tolerance distance from the input line


--Lock all edges intersecting the incoming input line
--Lock all faces found on both sides of the above edges
--Lock all edges having any of the above faces on their side

      command_string := Format('WITH face_01 AS --EVERY FACE whos MBR intersects the input line  (tested with ST_DWithin and tolerance and then execution time increase may be 30%)
                               (
                                 SELECT f.* 
                                    FROM temp_left_over_borders i,
                                    %1$s.face f 
                                 WHERE ST_Intersects(i.geo,f.mbr)
                                 FOR UPDATE
                               ),
                               edge_01 AS ( --EVERY EDGE having any of those faces on its right or left side
                                 SELECT e.* 
                                   FROM face_01 f, 
                                   %1$s.edge_data e 
                                 WHERE (e.left_face = f.face_id OR e.right_face = f.face_id)
                                 for update
                               ),
                               node_01 AS ( --EVERY ISOLATED NODE within tolerance distance from the input line
                                 SELECT n.* 
                                   FROM temp_left_over_borders i, %1$s.node n 
                                 where ST_DWithin(i.geo,n.geom,%2$s)
                                 for update
                               ), 
                               edge_02 AS ( --Lock all edges intersecting the incoming input line (tested with ST_DWithin and tolerance and then execution time increase may be 30%)
                                 SELECT e.* 
                                   FROM temp_left_over_borders i, 
                                   %1$s.edge_data e 
                                 WHERE ST_Intersects(i.geo,e.geom)
                                 for update
                              ),
                              face_02 AS ( --Lock all faces found on both sides of the above edges
                                 SELECT f.* 
                                    FROM edge_02 e,
                                    %1$s.face f 
                                 WHERE (e.left_face = f.face_id OR e.right_face = f.face_id)
                                 FOR UPDATE
                              )
-- With this block we got deadlocks all the time which kills the performace, 
-- when trying to around 1200 cellborder crossing lines we got more than 1000 deadlocks and then we where done with only around 1/4 of the lines.
-- A jobs that shoud take 2 minutes I just killed after 10 miuntes. 
--                              ,
--                              edge_03 AS ( --Lock all edges having any of the above faces on their side
--                                 SELECT e.* 
--                                   FROM face_02 f, 
--                                   %1$s.edge_data e 
--                                 WHERE (e.left_face = f.face_id OR e.right_face = f.face_id)
--                                 for update
--                              )
                              SELECT ( (SELECT count(*) from edge_01) + 
                                       (SELECT count(*) from node_01) +
                                       (SELECT count(*) from face_02)
                                     )', 
     _topology_name,snap_tolerance_fixed);
     EXECUTE command_string INTO num_locked;
     RAISE NOTICE 'Locked %  rows for update top toplogy % and _cell_job_type %, for area_to_block % ',  
     num_locked, _topology_name, _cell_job_type, area_to_block;
     
    border_topo_info.topology_name := _topology_name;

    BEGIN 
      -- add border smale border lines
      command_string := Format('SELECT topo_update.add_border_lines(%1$L,geo,%2$s,%3$L,FALSE) from temp_left_over_borders group by geo order by ST_Length(geo) asc', 
      _topology_name, snap_tolerance_fixed, _table_name_result_prefix);

      EXECUTE command_string into line_edges_added;
      RAISE NOTICE 'Added edges for border lines for box % into line_edges_added %',  box_id, line_edges_added;
      
      

    --drop table temp_left_over_borders;
    
    EXCEPTION
      WHEN OTHERS THEN
  RAISE NOTICE 'Do rollback at timeofday:% for layer %, with _cell_job_type % and box id % .', 
  Timeofday(), _topology_name || '_', _cell_job_type, box_id;

    ROLLBACK;
      RETURN;
    END;	    

     
   ELSIF _cell_job_type = 4 THEN
    -- heal border edges
    
     IF _loop_number < 1 THEN 
       -- In first loop only block by egdes
       command_string := Format('SELECT ST_Union(geom) from (SELECT ST_Expand(ST_Envelope(%1$s),%2$s) as geom from %3$s where ST_intersects(%1$s,%4$L) ) as r', 
       'geom', _topology_snap_tolerance, _topology_name||'.edge_data', _bb);
     ELSE
       -- In second loop block by input geo size
       command_string := Format('SELECT ST_Expand(ST_Envelope(ST_collect(%1$s)),%2$s) from %3$s where ST_intersects(%1$s,%4$L);', 
       input_table_geo_column_name, _topology_snap_tolerance, input_table_name, _bb);
       
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
      _topology_name, _bb);
    EXECUTE command_string;

    RAISE NOTICE 'Did Heal lines for topo % and bb % at % after added edges for border lines used_time %', 
    _topology_name, _bb, Clock_timestamp(), (Extract(EPOCH FROM (Clock_timestamp() - start_time_delta_job)));


  ELSIF _cell_job_type = 5 THEN

    command_string := Format('SELECT ST_Union(geom) from (select ST_Expand(ST_Envelope(%1$s),%2$s) as geom from %3$s where ST_intersects(%1$s,%4$L) ) as r', 
    'geom', _topology_snap_tolerance, _topology_name||'.edge_data', _bb);

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
    overlapgap_grid, input_table_geo_column_name, _bb, _table_name_result_prefix,_topology_snap_tolerance*inner_cell_distance);
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
        _topology_name);
        EXECUTE command_string into edgelist_to_change;

        
        IF edgelist_to_change IS NOT NULL AND (Array_length(edgelist_to_change, 1)) IS NOT NULL THEN 
          FOREACH edge_id_heal IN ARRAY edgelist_to_change 
          LOOP
            heal_edge_retry_num := 1;
            LOOP
              command_string := FORMAT('SELECT topo_update.try_ST_ChangeEdgeGeom(e.geom,%1$L,%4$L,%5$L,e.edge_id,ST_simplifyPreserveTopology(e.geom,%2$s)) 
              from %1$s.edge_data e where e.edge_id = %3$s',
              _topology_name, (_clean_info).simplify_tolerance/heal_edge_retry_num, edge_id_heal, (_clean_info).simplify_max_average_vertex_length, _utm);
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
       _topology_name, _bb, Clock_timestamp(), (Extract(EPOCH FROM (Clock_timestamp() - start_time_delta_job)));

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
      _topology_name, _bb, _utm, _clean_info, _topology_snap_tolerance/2,(_clean_info).simplify_max_average_vertex_length);
      EXECUTE command_string;
      RAISE NOTICE 'Did chaikinsAcuteAngle for topo % and bb % at % used_time %', 
      _topology_name, _bb, Clock_timestamp(), (Extract(EPOCH FROM (Clock_timestamp() - start_time_delta_job)));

    END IF;
    
       -- remove 
    face_table_name = _topology_name || '.face';
    start_time_delta_job := Clock_timestamp();
    RAISE NOTICE 'Start clean small polygons for border plygons face_table_name % at %', face_table_name, Clock_timestamp();
    -- remove small polygons in temp
    -- TODO 6 sould be based on other values
    num_rows_removed := topo_update.do_remove_small_areas_no_block (_topology_name, (_clean_info).min_area_to_keep, face_table_name, ST_Expand(_bb,(_topology_snap_tolerance * -6)),
      _utm);
    used_time := (Extract(EPOCH FROM (Clock_timestamp() - start_time_delta_job)));
    RAISE NOTICE 'Removed % clean small polygons for after adding to main face_table_name % at % used_time: %', num_rows_removed, face_table_name, Clock_timestamp(), used_time;

    --drop table temp_left_over_borders;
    
    EXCEPTION
      WHEN OTHERS THEN
  RAISE NOTICE 'Do rollback at timeofday:% for layer %, with _cell_job_type % and box id % .', 
  Timeofday(), _topology_name || '_', _cell_job_type, box_id;

    ROLLBACK;
      RETURN;
    END;	    

  ELSIF _cell_job_type = 6 THEN
  
    command_string := Format('SELECT ST_Expand(ST_Envelope(ST_collect(%1$s)),%2$s) from %3$s where ST_intersects(%1$s,%4$L);', 
    input_table_geo_column_name, _topology_snap_tolerance, input_table_name, _bb);
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
    face_table_name = _topology_name || '.face';
    start_time_delta_job := Clock_timestamp();
    RAISE NOTICE 'Start clean small polygons for cell plygons face_table_name % at %', face_table_name, Clock_timestamp();
    -- remove small polygons in temp
    -- TODO 6 sould be based on other values
    num_rows_removed := topo_update.do_remove_small_areas_no_block (_topology_name, (_clean_info).min_area_to_keep, face_table_name, ST_Expand(_bb,(_topology_snap_tolerance * -6)),
      _utm);
    used_time := (Extract(EPOCH FROM (Clock_timestamp() - start_time_delta_job)));
    RAISE NOTICE 'Removed % clean small polygons for after adding to main face_table_name % at % used_time: %', num_rows_removed, face_table_name, Clock_timestamp(), used_time;


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
    
    -- Insert new geos based on all face id do not check on input table
    command_string := Format('insert into %3$s(%5$s)
 	select * from (select (ST_Dump(topo_update.get_face_geo(%1$L,face_id,%7$s))).geom as %5$s from (
 	SELECT f.face_id, min(jl.id) as cell_id  FROM
 	%1$s.face f, 
 	%4$s jl 
 	WHERE f.mbr && %2$L and jl.cell_geo && f.mbr
 	GROUP BY f.face_id
 	) as r where cell_id = %6$s 
    ) as r where ST_IsValid(r.%5$s)', 
    _topology_name, _bb, temp_table_name, _table_name_result_prefix || '_job_list', input_table_geo_column_name, box_id,snap_tolerance_fixed);
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
 ) r where r.%5$s = t.%5$s', temp_table_name, input_table_name, input_table_pk_column_name, input_table_geo_column_name, temp_table_id_column);
 
 RAISE NOTICE 'upate attributes % ', command_string;
    
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
    -- EXECUTE Format('DROP TABLE IF EXISTS %s', temp_table_name);
  ELSE
    RAISE EXCEPTION 'Invalid _cell_job_type % ', _cell_job_type;
  END IF;
  RAISE NOTICE 'done work at timeofday:% for layer %, with _cell_job_type %', Timeofday(), border_topo_info.topology_name, _cell_job_type;
  command_string := Format('update %1$s set block_bb = %2$L where cell_geo = %3$L', _job_list_name, _bb, _bb);
  EXECUTE command_string;
  
  done_time := Clock_timestamp();
  used_time := (Extract(EPOCH FROM (done_time - start_time)));
  RAISE NOTICE 'work done for cell % at % border_layer_id %, using % sec', box_id, done_time, border_topo_info.border_layer_id, used_time;
  -- This is a list of lines that fails
  -- this is used for debug
  IF used_time > 10 THEN
    RAISE NOTICE 'very long time used for lines, % time with geo for _bb % ', used_time, box_id;
    EXECUTE Format('INSERT INTO %s (execute_time, info, sql, geo) VALUES (%s, %L, %L, %L)', _table_name_result_prefix || '_long_time_log2', used_time, 'simplefeature_c2_topo_surface_border_retry', command_string, _bb);
  END IF;
  PERFORM topo_update.clear_blocked_area (_bb, _job_list_name);
  RAISE NOTICE 'leave work at timeofday:% for layer %, with _cell_job_type % for cell %', Timeofday(), border_topo_info.topology_name, _cell_job_type, box_id;
  --RETURN added_rows;
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
  

