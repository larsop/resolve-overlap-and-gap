-- This is the main funtion used resolve overlap and gap
CREATE OR REPLACE PROCEDURE resolve_overlap_gap_run (
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

_clean_info resolve_overlap_data_clean_type, -- different parameters used if need to clean up your data
--(_clean_info).simplify_tolerance float, -- is this is more than zero simply will called with
--(_clean_info).do_chaikins boolean, -- here we will use chaikins togehter with simply to smooth lines
--(_clean_info).min_area_to_keep float, -- if this a polygon  is below this limit it will merge into a neighbour polygon. The area is sqare meter. 

_max_parallel_jobs int, -- this is the max number of paralell jobs to run. There must be at least the same number of free connections
_max_rows_in_each_cell int, -- this is the max number rows that intersects with box before it's split into 4 new boxes, default is 5000
_debug_options resolve_overlap_data_debug_options_type -- this used to set different debug parameters 
)
LANGUAGE plpgsql
AS $$
DECLARE
  command_string text;
  num_rows int;
  part text;
  id_list_tmp int[];
  this_list_id int;
  -- Holds the list of func_call to run
  stmts text[];

  -- Holds the sql for a functin to call
  func_call text;
  -- Holds the reseult from paralell calls
  call_result int;
  -- the number of cells created in the grid
  num_cells int;
  -- the table name prefix to be used for results tables
  table_name_result_prefix varchar;
  -- the name of the content based grid table
  overlapgap_grid varchar;
  -- The schema.table name of the grid that will be created and used to break data up in to managle pieces
  -- the name of job_list table, this table is ued to track of done jobs
  job_list_name varchar;
  -- the sql used for blocking cells
  sql_to_block_cmd varchar;
  -- just to create sql
  command_string_var varchar;
  -- this is the tolerance used when creating the topo layer
  cell_job_type int;
  -- add lines 1 inside cell, 2 boderlines, 3 exract simple
  topology_schema_name varchar = (_topology_info).topology_name;
  -- for now we use the same schema as the topology structure
  loop_number int default 1;
  
  i_stmts int;
  analyze_stmts int;
  
  last_run_stmts int;
  
  num_topo_error_in_final_layer int;
  v_state text;
  v_msg text;
  v_detail text;
  v_hint text;
  v_context text;
  
  
  start_time timestamp WITH time zone;
  
  -- Used for debug
  contiune_after_stat_exception boolean DEFAULT true; -- DEFAULT true, -- if set to false, it will do topology.ValidateTopology and stop to if the this call returns any rows 
  validate_topoplogy_for_each_run boolean DEFAULT false; -- if set to true, it will do topology.ValidateTopology at each loop return if it's error 
  run_add_border_line_as_single_thread boolean default false; --  if set to false, it will in many cases generate topo errors beacuse of running in many parralell threads
  start_at_job_type int default 1; -- if set to more than 1 it will skip init procces and start at given input
  start_at_loop_nr int default 1; -- many of jobs are ran in loops beacuse because if get an exception or cell is not allowed handle because cell close to is also started to work , this cell will gandled in the next loop.
  stop_at_job_type int default 0; -- if set to more than 0 the job will stop  when this job type is reday to run and display a set sql to run
  stop_at_loop_nr int default 0; -- if set to more than 0 the job will stop  when this job type is reday to run and display a set sql to run




BEGIN
  IF _debug_options IS NOT NULL THEN
     contiune_after_stat_exception = (_debug_options).contiune_after_stat_exception;
     validate_topoplogy_for_each_run = (_debug_options).validate_topoplogy_for_each_run;
     run_add_border_line_as_single_thread = (_debug_options).run_add_border_line_as_single_thread;
     
     start_at_job_type  = (_debug_options).start_at_job_type;
     start_at_loop_nr = (_debug_options).start_at_loop_nr;
     stop_at_job_type = (_debug_options).stop_at_job_type;
     stop_at_loop_nr = (_debug_options).stop_at_loop_nr;

     loop_number = start_at_loop_nr;
  END IF;
  
  table_name_result_prefix := (_topology_info).topology_name || Substring((_input_data).polygon_table_name FROM (Position('.' IN (_input_data).polygon_table_name)));
  -- This is table name prefix including schema used for the result tables
  -- || '_overlap'; -- The schema.table name for the overlap/intersects found in each cell
  -- || '_gap'; -- The schema.table name for the gaps/holes found in each cell
  -- || '_grid'; -- The schema.table name of the grid that will be created and used to break data up in to managle pieces
  -- || '_boundery'; -- The schema.table name the outer boundery of the data found in each cell
  -- NB. Any exting data will related to this table names will be deleted
  -- the name of the content based grid table
  overlapgap_grid := table_name_result_prefix || '_grid';
  -- The schema.table name of the grid that will be created and used to break data up in to managle pieces
  -- the name of job_list table, this table is ued to track of done jobs
  job_list_name := table_name_result_prefix || '_job_list';
  -- Call init method to create content based create and main topology schema
  

  
  IF (_topology_info).create_topology_attrbute_tables = true and (_input_data).line_table_name is not null 
     and (_input_data).line_table_other_collumns_def is null THEN
     EXECUTE Format('select 
	  Array_to_string(array_agg(column_name||%L||data_type),%L) as column_def,
	  Array_to_string(array_agg(column_name),%L) as column_name
	  from INFORMATION_SCHEMA.COLUMNS where  table_schema = %L and table_name = %L and data_type != %L and column_name != %L',
	  ' ',',',',',
	  split_part((_input_data).line_table_name, '.', 1),
	  split_part((_input_data).line_table_name, '.', 2),
	  'USER-DEFINED',
	  (_input_data).line_table_geo_collumn) INTO _input_data.line_table_other_collumns_def, _input_data.line_table_other_collumns_list;
	  
  END IF;

  
  
  IF start_at_job_type = 1 THEN 
    command_string := Format('SELECT resolve_overlap_gap_init(%L,%L,%L,%s,%s)', 
    _input_data,
    _topology_info,
    table_name_result_prefix, 
    _max_rows_in_each_cell, 
    Quote_literal(overlapgap_grid), 
    _topology_info);

  -- execute the string
    EXECUTE command_string INTO num_cells;
  END IF;

  
  IF (_topology_info).create_topology_attrbute_tables = true and (_input_data).line_table_name is not null THEN
 	command_string := FORMAT('SELECT layer_id 
	FROM topology.layer l, topology.topology t 
	WHERE t.name = %L AND
	t.id = l.topology_id AND
	l.schema_name = %L AND
	l.table_name = %L AND
	l.feature_column = %L',
    (_topology_info).topology_name,
    (_topology_info).topology_name,
    'edge_attributes',
    (_input_data).line_table_geo_collumn);
	EXECUTE command_string INTO _topology_info.topology_attrbute_tables_border_layer_id;
		
	RAISE NOTICE 'command_string % set _topology_info.topology_attrbute_tables_border_layer_id to %',command_string , (_topology_info).topology_attrbute_tables_border_layer_id;
      
  END IF;

  
  
  FOR cell_job_type IN start_at_job_type..7 LOOP

    IF cell_job_type = 3 and loop_number = 1 THEN
      -- add very long lines feature in single thread
      -- Most parts of this will not be healed and smooting if we keep it this way
      
      IF (_topology_info).create_topology_attrbute_tables = true and (_input_data).line_table_name is not null THEN
     	
	       command_string := Format('WITH lines_addes AS (
	       SELECT topology.toTopoGeom(r.geo, %1$L, %2$L , %3$L) as topogeometry, column_data_as_json  
	       FROM %4$s as r where r.added_to_master = false
	       ORDER BY ST_X(ST_Centroid(r.geo)), ST_Y(ST_Centroid(r.geo)))
	       INSERT INTO %5$s(%7$s,%6$s)  
	       SELECT x.*, ee.topogeometry as %6$s FROM lines_addes ee, jsonb_to_record(ee.column_data_as_json) AS x(%8$s)',
	       (_topology_info).topology_name,
	       (_topology_info).topology_attrbute_tables_border_layer_id,
	       (_topology_info).topology_snap_tolerance,
	       table_name_result_prefix||'_border_line_many_points',
	       (_topology_info).topology_name||'.edge_attributes',
	       (_input_data).line_table_geo_collumn,
	       (_input_data).line_table_other_collumns_list,
	       (_input_data).line_table_other_collumns_def
	       );
	       EXECUTE command_string;

	       command_string := Format('WITH lines_addes AS (
	       SELECT topology.toTopoGeom(r.geo, %1$L, %2$L , %3$L) as topogeometry, column_data_as_json  
	       FROM %4$s as r where r.added_to_master = false
	       ORDER BY ST_X(ST_Centroid(r.geo)), ST_Y(ST_Centroid(r.geo)))
	       INSERT INTO %5$s(%7$s,%6$s)  
	       SELECT x.*, ee.topogeometry as %6$s FROM lines_addes ee, jsonb_to_record(ee.column_data_as_json) AS x(%8$s)',
	       (_topology_info).topology_name,
	       (_topology_info).topology_attrbute_tables_border_layer_id,
	       (_topology_info).topology_snap_tolerance,
	       table_name_result_prefix||'_border_line_segments',
	       (_topology_info).topology_name||'.edge_attributes',
	       (_input_data).line_table_geo_collumn,
	       (_input_data).line_table_other_collumns_list,
	       (_input_data).line_table_other_collumns_def
	       );
	       EXECUTE command_string;

       ELSE
	      command_string := Format('SELECT topo_update.add_border_lines(%1$L,r.geo,%2$s,%3$L,FALSE) from %4$s r where r.added_to_master = false', 
	      (_topology_info).topology_name, 
	      (_topology_info).topology_snap_tolerance, 
	      table_name_result_prefix,
	      table_name_result_prefix||'_border_line_many_points');
	      EXECUTE command_string;
	
	      command_string := Format('update %1$s set added_to_master = true
	      where added_to_master = false', 
	      table_name_result_prefix||'_border_line_many_points');
	      EXECUTE command_string;
	
	      command_string := Format('SELECT topo_update.add_border_lines(%1$L,r.geo,%2$s,%3$L,FALSE) 
	      from 
	      %4$s r,
	      %5$s l
	      where ST_Intersects(l.%6$s,r.geo) and r.added_to_master = false',
	      (_topology_info).topology_name, 
	      (_topology_info).topology_snap_tolerance, 
	      table_name_result_prefix,
	      table_name_result_prefix||'_border_line_segments',
	      overlapgap_grid||'_metagrid_'||to_char(1, 'fm0000')||'_lines',
	      (_input_data).polygon_table_geo_collumn);
	      EXECUTE command_string;
	      
	
	      command_string := Format('update %1$s set added_to_master = true
	      where added_to_master = false', 
	      table_name_result_prefix||'_border_line_segments');
	      EXECUTE command_string;
    	END IF;

      COMMIT;

    END IF;

    
   IF cell_job_type = 4 and loop_number = 1 THEN
      -- try fixed failed lines before make simple feature in single thread
      -- this code is messy needs to fixed in anither way     
      command_string := Format('UPDATE %1$s u 
      SET line_geo_lost = false
      FROM %1$s o
      where ST_DWithin(u.geo,u.geo,%2$s) and u.line_geo_lost = false',
      table_name_result_prefix||'_no_cut_line_failed',
      (_topology_info).topology_snap_tolerance);
      EXECUTE command_string;

      command_string := Format('WITH topo_updated AS (
      SELECT topo_update.add_border_lines(%1$L,r.geo,%2$s,%3$L,TRUE), geo 
        FROM (
          SELECT distinct (ST_Dump(ST_Multi(ST_LineMerge(ST_union(r.geo))))).geom as geo 
            FROM (
              select r.geo from %4$s r where line_geo_lost = true
            ) as r
          ) as r
      )
      update %4$s u 
      set line_geo_lost = false
      FROM topo_updated tu
      where ST_DWithin(tu.geo,u.geo,%2$s) and (SELECT bool_or(x IS NOT NULL) FROM unnest(tu.add_border_lines) x)' , 
      (_topology_info).topology_name, 
      (_topology_info).topology_snap_tolerance, 
      table_name_result_prefix, 
      table_name_result_prefix||'_no_cut_line_failed');

      RAISE NOTICE 'Try to add failed lines with retry to master topo layer %', command_string;
      EXECUTE command_string;

      
      COMMIT;

    END IF;

    IF loop_number = 1 and cell_job_type != 2 THEN
      command_string := Format('SELECT resolve_overlap_gap_job_list(%L,%L,%L,%L,%L,%L,%s,%s,%s)', 
      _input_data, 
      _topology_info,
      overlapgap_grid, 
      table_name_result_prefix, 
      job_list_name, 
      _clean_info, 
      _max_parallel_jobs, 
      cell_job_type,loop_number);
      EXECUTE command_string;
      COMMIT;
    END IF;

    --EXIT WHEN cell_job_type = 4;
    
    last_run_stmts := 0;
    LOOP

      EXIT WHEN cell_job_type = 3;

      IF cell_job_type = 2 THEN
        command_string := Format('SELECT resolve_overlap_gap_job_list(%L,%L,%L,%L,%L,%L,%s,%s,%s)', 
        _input_data, _topology_info, overlapgap_grid, table_name_result_prefix, job_list_name, _clean_info, _max_parallel_jobs, cell_job_type,loop_number);
        EXECUTE command_string;
        COMMIT;
      END IF;

      stmts := '{}';

        command_string := Format('SELECT ARRAY(SELECT j.sql_to_run||%1$L as func_call 
        FROM %2$s j
        WHERE j.block_bb is null
        ORDER BY ST_X(ST_Centroid(j.%3$s)), ST_Y(ST_Centroid(j.%3$s)) ) ',  
        loop_number||');',
        job_list_name,
        'cell_geo');
               
        EXECUTE command_string INTO stmts;
     
  
      
      EXIT WHEN 
        Array_length(stmts, 1) IS NULL OR
        stmts IS NULL ;
        
      RAISE NOTICE 'Kicking off % jobs for cell_job_type % at loop_number % for topology % at % ', 
           Array_length(stmts, 1), cell_job_type, loop_number, (_topology_info).topology_name, now();
      



      BEGIN
  
	    start_time := Clock_timestamp();
    	IF cell_job_type = 3 and run_add_border_line_as_single_thread = true THEN
          -- run in single thread to avoid topo errors
          SELECT execute_parallel (stmts, 1,true,null,contiune_after_stat_exception) INTO call_result;
        ELSE 
          SELECT execute_parallel (stmts, _max_parallel_jobs,true,null,contiune_after_stat_exception) INTO call_result;
        END IF;

        RAISE NOTICE 'Done running % jobs for cell_job_type % at loop_number % for topology % in % secs', 
        Array_length(stmts, 1), cell_job_type, loop_number, (_topology_info).topology_name, (Extract(EPOCH FROM (Clock_timestamp() - start_time)));
  

      EXCEPTION WHEN OTHERS THEN
        GET STACKED DIAGNOSTICS v_state = RETURNED_SQLSTATE, v_msg = MESSAGE_TEXT, v_detail = PG_EXCEPTION_DETAIL, v_hint = PG_EXCEPTION_HINT, v_context = PG_EXCEPTION_CONTEXT;
        RAISE NOTICE 'Failed run execute_parallel cell_job_type: % , in loop_number %, state  : % message: % detail : % hint : % context: %', 
            cell_job_type, loop_number, v_state, v_msg, v_detail, v_hint, v_context;
        IF contiune_after_stat_exception = false THEN
          -- Do a validation is any erros found stop to execute
          start_time := Clock_timestamp();

          command_string := Format('SELECT count(*) FROM topology.ValidateTopology(%L)',(_topology_info).topology_name );
          RAISE NOTICE 'Start to ValidateTopology for cell_job_type % at loop_number % running % ', 
          cell_job_type, loop_number, command_string;
          execute command_string into num_topo_error_in_final_layer;

          RAISE NOTICE 'Found % errors when ValidateTopology for cell_job_type % at loop_number % for topology % in % secs', 
          num_topo_error_in_final_layer, cell_job_type, loop_number, (_topology_info).topology_name, (Extract(EPOCH FROM (Clock_timestamp() - start_time)));

          IF num_topo_error_in_final_layer > 0 THEN
         	 -- If any erros found break 
         	 RAISE EXCEPTION 'Failed run execute_parallel cell_job_type and error found : % , in loop_number %, state  : % message: % detail : % hint : % context: %', 
             cell_job_type, loop_number, v_state, v_msg, v_detail, v_hint, v_context;
          END IF;
        END IF;
      END;      

      	      
      IF validate_topoplogy_for_each_run THEN
          start_time := Clock_timestamp();
          
          command_string := Format('SELECT count(*) FROM topology.ValidateTopology(%L)',(_topology_info).topology_name );
          RAISE NOTICE 'Start to ValidateTopology because validate_topoplogy_for_each_run is true for cell_job_type % at loop_number % running % ', 
          cell_job_type, loop_number, command_string;
          execute command_string into num_topo_error_in_final_layer;

          RAISE NOTICE 'Found % errors when ValidateTopology for cell_job_type % at loop_number % for topology % in % secs', 
          num_topo_error_in_final_layer, cell_job_type, loop_number, (_topology_info).topology_name, (Extract(EPOCH FROM (Clock_timestamp() - start_time)));

          IF num_topo_error_in_final_layer > 0 THEN
         	 -- If any erros found break 
         	 RAISE EXCEPTION 'error found when topology.ValidateTopology for job_type : % , in loop_number %', 
             cell_job_type, loop_number;
          END IF;
      
      END IF;
  

      IF (call_result = 0 AND last_run_stmts = Array_length(stmts, 1)) THEN
        RAISE EXCEPTION 'FFailed to run overlap and gap for % at loop_number % for the following statement list %', 
        (_input_data).polygon_table_name, loop_number, stmts;
      END IF;

      last_run_stmts := Array_length(stmts, 1); 
      loop_number := loop_number + 1;

      IF stop_at_job_type >= cell_job_type AND loop_number >= stop_at_loop_nr  THEN  
	      RAISE WARNING 'EXIT with % jobs for cell_job_type % at loop_number % for topology % ', 
          Array_length(stmts, 1), cell_job_type, loop_number, (_topology_info).topology_name;
          RAISE WARNING 'stmts to run --> %', stmts;
          return ;
	  END IF;


    END LOOP;
    loop_number := 1;

  END LOOP;
END
$$;



-- This is the main funtion used resolve overlap and gap
CREATE OR REPLACE PROCEDURE resolve_overlap_gap_run(

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

_clean_info resolve_overlap_data_clean_type, -- different parameters used if need to clean up your data
--(_clean_info).simplify_tolerance float, -- is this is more than zero simply will called with
--(_clean_info).do_chaikins boolean, -- here we will use chaikins togehter with simply to smooth lines
--(_clean_info).min_area_to_keep float, -- if this a polygon  is below this limit it will merge into a neighbour polygon. The area is sqare meter. 

_max_parallel_jobs int, -- this is the max number of paralell jobs to run. There must be at least the same number of free connections
_max_rows_in_each_cell int
)
LANGUAGE plpgsql
AS $$
DECLARE
debug_options resolve_overlap_data_debug_options_type;
BEGIN

CALL resolve_overlap_gap_run(_input_data , 
_topology_info, 
_clean_info,
_max_parallel_jobs,
_max_rows_in_each_cell,
debug_options);

END
$$;

