-- This is the main funtion used resolve overlap and gap

DROP PROCEDURE IF EXISTS resolve_overlap_gap_run(
table_to_resolve_ varchar, -- The table to resolve 
geo_collumn_name_ varchar, 	-- the name of geometry column on the table to analyze	
srid_ int, -- the srid for the given geo column on the table analyze
table_name_result_prefix_ varchar, -- This is table name prefix including schema used for the result tables
-- || '_overlap'; -- The schema.table name for the overlap/intersects found in each cell 
-- || '_gap'; -- The schema.table name for the gaps/holes found in each cell 
-- || '_grid'; -- The schema.table name of the grid that will be created and used to break data up in to managle pieces
-- || '_boundery'; -- The schema.table name the outer boundery of the data found in each cell 
-- NB. Any exting data will related to this table names will be deleted 


topology_name_ varchar,  -- The topology schema name where we store store sufaces and lines from the simple feature dataset.
-- NB. Any exting data will related to topology_name will be deleted 

max_parallel_jobs_ int, -- this is the max number of paralell jobs to run. There must be at least the same number of free connections
max_rows_in_each_cell_ int -- this is the max number rows that intersects with box before it's split into 4 new boxes, default is 5000
);

CREATE OR REPLACE PROCEDURE resolve_overlap_gap_run(
table_to_resolve_ varchar, -- The table to resolve 
geo_collumn_name_ varchar, 	-- the name of geometry column on the table to analyze	
srid_ int, -- the srid for the given geo column on the table analyze
table_name_result_prefix_ varchar, -- This is table name prefix including schema used for the result tables
-- || '_overlap'; -- The schema.table name for the overlap/intersects found in each cell 
-- || '_gap'; -- The schema.table name for the gaps/holes found in each cell 
-- || '_grid'; -- The schema.table name of the grid that will be created and used to break data up in to managle pieces
-- || '_boundery'; -- The schema.table name the outer boundery of the data found in each cell 
-- NB. Any exting data will related to this table names will be deleted 


topology_name_ varchar,  -- The topology schema name where we store store sufaces and lines from the simple feature dataset.
-- NB. Any exting data will related to topology_name will be deleted 

max_parallel_jobs_ int, -- this is the max number of paralell jobs to run. There must be at least the same number of free connections
max_rows_in_each_cell_ int -- this is the max number rows that intersects with box before it's split into 4 new boxes, default is 5000
) LANGUAGE plpgsql 
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
	overlapgap_grid varchar  = table_name_result_prefix_ || '_grid'; -- The schema.table name of the grid that will be created and used to break data up in to managle pieces
	
	-- the name of job_list table, this table is ued to track of done jobs
	job_list_name varchar  = table_name_result_prefix_ || '_job_list';
	
	-- the sql used for blocking cells
	sql_to_block_cmd varchar;
	
	-- just to create sql
	command_string_var varchar;
	
	-- TODO send as parameter or compute
	input_table_pk_column_name varchar = 'c1';
	
	-- TODO send as parameter or fix in another way
	_simplify_tolerance double precision = 0.00001;
	snap_tolerance double precision = 0.00001; 
	_do_chaikins boolean = false;
	inside_cell_data boolean = true;


BEGIN
		
	
	-- Call init method to create content based create and main topology schema
	command_string := FORMAT('SELECT resolve_overlap_gap_init(%s,%s,%s,%s,%s,%s,%s)',
	quote_literal(table_to_resolve_),
	quote_literal(geo_collumn_name_),
	srid_,
	max_rows_in_each_cell_,
	quote_literal(overlapgap_grid),
	quote_literal(topology_name_),
	snap_tolerance
	);
	-- execute the string
	EXECUTE command_string INTO num_cells;
	
	
	-- ############################# START # create jobList tables

	command_string := FORMAT('SELECT resolve_overlap_gap_job_list(%L,%L,%s,%L,%L,%L,%L,%s,%s,%L,%L)',
	table_to_resolve_,
	geo_collumn_name_,
	srid_,
	overlapgap_grid,
	topology_name_,
	job_list_name,
	input_table_pk_column_name,
	_simplify_tolerance,
	snap_tolerance,
	_do_chaikins,
	inside_cell_data);
	
	EXECUTE command_string;

	-- ----------------------------- DONE - create jobList tables

	
	COMMIT;

	-- ############################# START # add lines inside box and cut lines and save then in separate table, 
	-- lines maybe simplified in this process also, but not the lines that are close to a border
	-- TODO REMOVE LOOP
	LOOP

		command_string := FORMAT('SELECT ARRAY(SELECT sql_to_run as func_call FROM %s WHERE block_bb is null ORDER BY md5(cell_geo::text) DESC)',job_list_name);
		RAISE NOTICE 'command_string %', command_string;
		execute command_string INTO stmts;
	
		EXIT WHEN array_length(stmts,1) is NULL OR stmts IS null;
	
		RAISE NOTICE 'array_length(stmts,1) %, stmts %', array_length(stmts,1), stmts ;
	
		select execute_parallel(stmts,max_parallel_jobs_) into call_result;
	
		IF (call_result = false) THEN 
			RAISE EXCEPTION 'Failed to run overlap and gap for % with the following statement list %', table_to_resolve_, stmts;
		END IF;
	END LOOP;
	-- ----------------------------- DONE # add lines inside box and cut lines and save then in separate table, 


	-- ############################# START # add border lines saved in last run, we will here connect data from the different cell using he border lines. 

	inside_cell_data := false;
	command_string := FORMAT('SELECT resolve_overlap_gap_job_list(%L,%L,%s,%L,%L,%L,%L,%s,%s,%L,%L)',
	table_to_resolve_,
	geo_collumn_name_,
	srid_,
	overlapgap_grid,
	topology_name_,
	job_list_name,
	input_table_pk_column_name,
	_simplify_tolerance,
	snap_tolerance,
	_do_chaikins,
	inside_cell_data);
	
	EXECUTE command_string;

	COMMIT;


	-- ############################# START # add lines inside box and cut lines and save then in separate table, 
	-- lines maybe simplified in this process also, but not the lines that are close to a border
	-- TODO REMOVE LOOP
	LOOP

		command_string := FORMAT('SELECT ARRAY(SELECT sql_to_run as func_call FROM %s WHERE block_bb is null ORDER BY md5(cell_geo::text) DESC)',job_list_name);
		RAISE NOTICE 'command_string %', command_string;
		execute command_string INTO stmts;
	
		EXIT WHEN array_length(stmts,1) is NULL OR stmts IS null;
	
		RAISE NOTICE 'array_length(stmts,1) %, stmts %', array_length(stmts,1), stmts ;
	
		select execute_parallel(stmts,max_parallel_jobs_) into call_result;
	
		IF (call_result = false) THEN 
			RAISE EXCEPTION 'Failed to run overlap and gap for % with the following statement list %', table_to_resolve_, stmts;
		END IF;
	END LOOP;


END $$;

GRANT EXECUTE on PROCEDURE resolve_overlap_gap_run(
table_to_resolve_ varchar, -- The table to resolve 
geo_collumn_name_ varchar, 	-- the name of geometry column on the table to analyze	
srid_ int, -- the srid for the given geo column on the table analyze
table_name_result_prefix_ varchar, -- This is table name prefix including schema used for the result tables
-- || '_overlap'; -- The schema.table name for the overlap/intersects found in each cell 
-- || '_gap'; -- The schema.table name for the gaps/holes found in each cell 
-- || '_grid'; -- The schema.table name of the grid that will be created and used to break data up in to managle pieces
-- || '_boundery'; -- The schema.table name the outer boundery of the data found in each cell 
-- NB. Any exting data will related to this table names will be deleted 


topology_name_ varchar,  -- The topology schema name where we store store sufaces and lines from the simple feature dataset.
-- NB. Any exting data will related to topology_name will be deleted 

max_parallel_jobs_ int, -- this is the max number of paralell jobs to run. There must be at least the same number of free connections
max_rows_in_each_cell_ int -- this is the max number rows that intersects with box before it's split into 4 new boxes, default is 5000
) TO public;



