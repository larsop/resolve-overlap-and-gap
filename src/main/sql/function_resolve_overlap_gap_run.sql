-- This is the main funtion used resolve overlap and gap
CREATE OR REPLACE PROCEDURE resolve_overlap_gap_run (_table_to_resolve varchar, -- The table to resolv, imcluding schema name
_table_pk_column_name varchar, -- The primary of the input table
_table_geo_collumn_name varchar, -- the name of geometry column on the table to analyze
_table_srid int, -- the srid for the given geo column on the table analyze
_utm boolean, _topology_name varchar, -- The topology schema name where we store store sufaces and lines from the simple feature dataset and th efinal result
-- NB. Any exting data will related to topology_name will be deleted
_tolerance double precision, -- this is tolerance used as base when creating the the top layer
_max_parallel_jobs int, -- this is the max number of paralell jobs to run. There must be at least the same number of free connections
_max_rows_in_each_cell int -- this is the max number rows that intersects with box before it's split into 4 new boxes, default is 5000
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
  stmts_final text[];

  -- Holds the sql for a functin to call
  func_call text;
  -- Holds the reseult from paralell calls
  call_result boolean;
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
  -- TODO send as parameter or compute
  -- TODO send as parameter or fix in another way
  simplify_tolerance double precision = (_tolerance * 2);
  -- this is the tolerance used when adding new lines s
  snap_tolerance double precision = _tolerance;
  -- this is the tolerance used when creating the topo layer
  cell_job_type int;
  -- add lines 1 inside cell, 2 boderlines, 3 exract simple
  topology_schema_name varchar = _topology_name;
  -- for now we use the same schema as the topology structure
  -- TODO add a paarameter
  _do_chaikins boolean = FALSE;
  _min_area_to_keep float = 49.0;
  loop_number int;
  
  i_stmts int;
  analyze_stmts int;
  

BEGIN
  table_name_result_prefix := _topology_name || Substring(_table_to_resolve FROM (Position('.' IN _table_to_resolve)));
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
  command_string := Format('SELECT resolve_overlap_gap_init(%L,%s,%s,%s,%s,%s,%s,%s)', 
  table_name_result_prefix, Quote_literal(_table_to_resolve), Quote_literal(_table_geo_collumn_name), _table_srid, _max_rows_in_each_cell, Quote_literal(overlapgap_grid), Quote_literal(_topology_name), snap_tolerance);
  -- execute the string
  EXECUTE command_string INTO num_cells;
  
  
  FOR cell_job_type IN 1..5 LOOP
    -- 1 ############################# START # add lines inside box and cut lines and save then in separate table,
    -- 2 ############################# START # add border lines saved in last run, we will here connect data from the different cell using he border lines.
    command_string := Format('SELECT resolve_overlap_gap_job_list(%L,%L,%s,%L,%L,%L,%L,%L,%L,%s,%s,%L,%L,%s)', 
    _table_to_resolve, _table_geo_collumn_name, _table_srid, _utm, overlapgap_grid, table_name_result_prefix, _topology_name, job_list_name, _table_pk_column_name, simplify_tolerance, snap_tolerance, _do_chaikins, _min_area_to_keep, cell_job_type);
    EXECUTE command_string;
    COMMIT;

    loop_number := 1;
    LOOP

      command_string := Format('SELECT ARRAY(SELECT sql_to_run||%L as func_call FROM %s WHERE block_bb is null 
        ORDER BY inside_cell desc, row_number, num_polygons desc )',  
      loop_number||');',job_list_name);
      RAISE NOTICE 'command_string %', command_string;
      EXECUTE command_string INTO stmts;
      EXIT
      WHEN Array_length(stmts, 1) IS NULL
        OR stmts IS NULL;
      
      stmts_final := '{}';
      analyze_stmts  := 0;
      FOR i_stmts IN 1 .. Array_length(stmts, 1)
      LOOP
         stmts_final[i_stmts+analyze_stmts] = stmts[i_stmts];
         IF MOD(i_stmts,200) = 0 AND cell_job_type > 1 AND cell_job_type < 4 THEN
           analyze_stmts := analyze_stmts + 1;
           stmts_final[i_stmts+analyze_stmts] := Format('ANALYZE %s.edge_data;', _topology_name);
           analyze_stmts := analyze_stmts + 1;
           stmts_final[i_stmts+analyze_stmts] := Format('ANALYZE %s.node;', _topology_name);
           analyze_stmts := analyze_stmts + 1;
           stmts_final[i_stmts+analyze_stmts] := Format('ANALYZE %s.face;', _topology_name);
           analyze_stmts := analyze_stmts + 1;
           stmts_final[i_stmts+analyze_stmts] := Format('ANALYZE %s.relation;', _topology_name);
         END IF;
      END LOOP;
      
      stmts := '{}';


      
      RAISE NOTICE 'Start to run overlap for % stmts_final and gap for table % cell_job_type % at loop_number %', 
      Array_length(stmts_final, 1), _table_to_resolve, cell_job_type, loop_number;

      SELECT execute_parallel (stmts_final, _max_parallel_jobs,true) INTO call_result;
      IF (call_result = FALSE AND loop_number > 1) THEN
        RAISE EXCEPTION 'FFailed to run overlap and gap for % at loop_number % for the following statement list %', 
        _table_to_resolve, loop_number, stmts_final;
      END IF;
      
      loop_number := loop_number + 1;


    END LOOP;
  END LOOP;
END
$$;

