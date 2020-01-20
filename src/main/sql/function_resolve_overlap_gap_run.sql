-- This is the main funtion used resolve overlap and gap

CREATE OR REPLACE PROCEDURE resolve_overlap_gap_run (
_table_to_resolve varchar, -- The table to resolve
_table_pk_column_name varchar, -- The primary of the input table
_table_geo_collumn_name varchar, -- the name of geometry column on the table to analyze
_table_srid int, -- the srid for the given geo column on the table analyze
_table_name_result_prefix varchar, -- This is table name prefix including schema used for the result tables
-- || '_overlap'; -- The schema.table name for the overlap/intersects found in each cell
-- || '_gap'; -- The schema.table name for the gaps/holes found in each cell
-- || '_grid'; -- The schema.table name of the grid that will be created and used to break data up in to managle pieces
-- || '_boundery'; -- The schema.table name the outer boundery of the data found in each cell
-- NB. Any exting data will related to this table names will be deleted
_topology_name varchar, -- The topology schema name where we store store sufaces and lines from the simple feature dataset.
-- NB. Any exting data will related to topology_name will be deleted
_tolerance  double precision, -- this is tolerance used as base when creating the the top layer
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
  -- Holds the sql for a functin to call
  func_call text;
  -- Holds the reseult from paralell calls
  call_result boolean;
  -- the number of cells created in the grid
  num_cells int;
  -- the name of the content based grid table
  overlapgap_grid varchar = _table_name_result_prefix || '_grid';
  -- The schema.table name of the grid that will be created and used to break data up in to managle pieces
  -- the name of job_list table, this table is ued to track of done jobs
  job_list_name varchar = _table_name_result_prefix || '_job_list';
  -- the sql used for blocking cells
  sql_to_block_cmd varchar;
  -- just to create sql
  command_string_var varchar;
  -- TODO send as parameter or compute
  
  -- TODO send as parameter or fix in another way
  simplify_tolerance double precision = (_tolerance*2); -- this is the tolerance used when adding new lines s
  snap_tolerance double precision = _tolerance; -- this is the tolerance used when creating the topo layer 
  inside_cell_data boolean = TRUE;
  topology_schema_name varchar = _topology_name; -- for now we use the same schema as the topology structure
  
  -- TODO add a paarameter
  _do_chaikins boolean = FALSE;
  _min_area_to_keep float = 49.0;

BEGIN
	
  -- Call init method to create content based create and main topology schema
  command_string := Format('SELECT resolve_overlap_gap_init(%s,%s,%s,%s,%s,%s,%s)', Quote_literal(_table_to_resolve), Quote_literal(_table_geo_collumn_name), _table_srid, _max_rows_in_each_cell, Quote_literal(overlapgap_grid), Quote_literal(_topology_name), snap_tolerance);
  -- execute the string
  EXECUTE command_string INTO num_cells;
  -- ############################# START # create jobList tables
  command_string := Format('SELECT resolve_overlap_gap_job_list(%L,%L,%s,%L,%L,%L,%L,%L,%s,%s,%L,%L,%L)', 
  _table_to_resolve, _table_geo_collumn_name, _table_srid, overlapgap_grid, topology_schema_name, _topology_name, job_list_name, _table_pk_column_name, simplify_tolerance, snap_tolerance, _do_chaikins, _min_area_to_keep, inside_cell_data);
  EXECUTE command_string;
  -- ----------------------------- DONE - create jobList tables
  COMMIT;
  -- ############################# START # add lines inside box and cut lines and save then in separate table,
  -- lines maybe simplified in this process also, but not the lines that are close to a border
  -- TODO REMOVE LOOP
  LOOP
    command_string := Format('SELECT ARRAY(SELECT sql_to_run as func_call FROM %s WHERE block_bb is null ORDER BY md5(cell_geo::Text) desc)', job_list_name);
    RAISE NOTICE 'command_string %', command_string;
    EXECUTE command_string INTO stmts;
    EXIT
    WHEN Array_length(stmts, 1) IS NULL
      OR stmts IS NULL;
    RAISE NOTICE 'array_length(stmts,1) %, stmts %', Array_length(stmts, 1), stmts;
    SELECT execute_parallel (stmts, _max_parallel_jobs) INTO call_result;
    IF (call_result = FALSE) THEN
      RAISE EXCEPTION 'Failed to run overlap and gap for % with the following statement list %', _table_to_resolve, stmts;
    END IF;
  END LOOP;
  -- ----------------------------- DONE # add lines inside box and cut lines and save then in separate table,
  -- ############################# START # add border lines saved in last run, we will here connect data from the different cell using he border lines.
  inside_cell_data := FALSE;
  command_string := Format('SELECT resolve_overlap_gap_job_list(%L,%L,%s,%L,%L,%L,%L,%L,%s,%s,%L,%L, %L)', 
  _table_to_resolve, _table_geo_collumn_name, _table_srid, overlapgap_grid, topology_schema_name, _topology_name, job_list_name, _table_pk_column_name, simplify_tolerance, snap_tolerance, _do_chaikins, _min_area_to_keep,inside_cell_data);
  EXECUTE command_string;
  COMMIT;
  -- ############################# START # add lines inside box and cut lines and save then in separate table,
  -- lines maybe simplified in this process also, but not the lines that are close to a border
  -- TODO REMOVE LOOP
  LOOP
    command_string := Format('SELECT ARRAY(SELECT sql_to_run as func_call FROM %s WHERE block_bb is null ORDER BY md5(cell_geo::Text) desc)', job_list_name);
    RAISE NOTICE 'command_string %', command_string;
    EXECUTE command_string INTO stmts;
    EXIT
    WHEN Array_length(stmts, 1) IS NULL
      OR stmts IS NULL;
    RAISE NOTICE 'array_length(stmts,1) %, stmts %', Array_length(stmts, 1), stmts;
    SELECT execute_parallel (stmts, _max_parallel_jobs) INTO call_result;
    IF (call_result = FALSE) THEN
      RAISE EXCEPTION 'Failed to run overlap and gap for % with the following statement list %', _table_to_resolve, stmts;
    END IF;
  END LOOP;
END
$$;
