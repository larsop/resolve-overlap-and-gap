
-- example of how to use
-- select ST_Area(cbg_get_table_extent(ARRAY['org_esri_union.table_1 geo_1', 'org_esri_union.table_2 geo_2']));
-- select ST_Area(cbg_get_table_extent(ARRAY['org_ar5.ar5_flate geo']));

-- Return the bounding box for given list of arrayes with table name and geo column name 
-- The table name must contain both schema and tablename 
-- The geo column name must follow with one single space after the table name.
-- Does not handle tables with different srid

CREATE OR REPLACE FUNCTION cbg_get_table_extent (schema_table_name_column_name_array VARCHAR[]) RETURNS geometry  AS
$body$
DECLARE
	grid_geom geometry;
	grid_geom_tmp geometry;	
	grid_geom_estimated box2d;	
	line VARCHAR;
	line_values VARCHAR[];
	line_schema_table VARCHAR[];
	geo_column_name VARCHAR;
	schema_table_name VARCHAR;
	source_srid int;
	schema_name VARCHAR := 'org_ar5';
	table_name VARCHAR := 'ar5_flate';
	sql VARCHAR;

BEGIN

	
	FOR i IN ARRAY_LOWER(schema_table_name_column_name_array,1)..ARRAY_UPPER(schema_table_name_column_name_array,1) LOOP
		line := schema_table_name_column_name_array[i];
--		raise NOTICE 'line : %', line;

		SELECT string_to_array(line, ' ') INTO line_values; 
		schema_table_name := line_values[1];
		geo_column_name := line_values[2];
		

		select string_to_array(schema_table_name, '.') into line_schema_table;

		schema_name := line_schema_table[1];
		table_name := line_schema_table[2];
		raise NOTICE 'schema_table_name : %, geo_column_name : %', schema_table_name, geo_column_name;

		sql := 'SELECT Find_SRID('''|| 	schema_name || ''', ''' || table_name || ''', ''' || geo_column_name || ''')';
--		raise NOTICE 'execute sql: %',sql;
		EXECUTE sql INTO source_srid ;

--		BEGIN
--			sql := format('ANALYZE %s',schema_table_name);
--			raise NOTICE 'execute sql: %',sql;
--			EXECUTE sql;
--			sql := 'SELECT ST_EstimatedExtent('''|| 	schema_name || ''', ''' || table_name || ''', ''' || geo_column_name || ''')';
--			raise NOTICE 'execute sql: %',sql;
--			EXECUTE sql INTO grid_geom_estimated ;
--			raise NOTICE 'grid_geom_estimated: %',grid_geom_estimated;
--        EXCEPTION WHEN internal_error THEN
        -- ERROR:  XX000: stats for "edge_data.geom" do not exist
        -- Catch error and return a return null ant let application decide what to do
--        END;

  
		IF grid_geom_estimated IS null THEN
			sql :=  'SELECT ST_SetSRID(ST_Extent(' || geo_column_name ||'),' || source_srid || ') FROM ' || schema_table_name; 
	    	raise NOTICE 'execute sql: %',sql;
			EXECUTE sql INTO  grid_geom_tmp;
		ELSE
			grid_geom_tmp :=  ST_SetSRID(box2d(grid_geom_estimated)::geometry, source_srid);
			--SELECT ST_SetSRID(ST_Extent(grid_geom_tmp), source_srid) INTO grid_geom_tmp ;

		END IF;

		-- first time grid_geom is null
		IF grid_geom IS null THEN
			grid_geom := ST_SetSRID(ST_Extent(grid_geom_tmp), source_srid);
		ELSE
		-- second time take in account tables before
			grid_geom := ST_SetSRID(ST_Extent(ST_Union(grid_geom, grid_geom_tmp)), source_srid);
		END IF;
		
		raise NOTICE 'grid_geom: %',ST_AsText(grid_geom);
		
	END LOOP;

	
	RETURN grid_geom;

END;
$body$
LANGUAGE 'plpgsql';

-- Grant so all can use it
GRANT EXECUTE ON FUNCTION cbg_get_table_extent (schema_table_name_column_name_array VARCHAR[]) to PUBLIC;


--DROP FUNCTION cbg_content_based_balanced_grid(table_name_column_name_array VARCHAR[], 
--													grid_geom_in geometry,
--													min_distance integer,
--													max_rows integer);

-- Create a content balanced grid based on number of rows in given cell.

-- Parameter 1 :
-- table_name_column_name_array a list of tables and collums to involve  on the form 
-- The table name must contain both schema and tablename 
-- The geo column name must follow with one single space after the table name.
-- Does not handle tables with different srid
-- ARRAY['org_esri_union.table_1 geo_1', 'org_esri_union.table_2 geo_2']

-- Parameter 2 :
-- grid_geom_in if this is point it ises the boundry from the tables as a start

-- Parameter 3 :
-- min_distance this is the default min distance in meter (no box will be smaller that 5000 meter

-- Parameter 4 :
-- max_rows this is the max number rows that intersects with box before it's split into 4 new boxes 


CREATE OR REPLACE FUNCTION cbg_content_based_balanced_grid (	
													table_name_column_name_array VARCHAR[], 
													grid_geom_in geometry,
													min_distance integer,
													max_rows integer) RETURNS geometry  AS
$body$
DECLARE
	x_min float;
	x_max float;
	y_min float;
	y_max float;

	x_delta float;
	y_delta float;

	x_center float;
	y_center float;

	sectors geometry[];

	grid_geom_meter geometry;
	
	-- this may be adjusted to your case
	metric_srid integer = 3035;

	x_length_meter float;
	y_length_meter float;

	num_rows_table integer = 0;
	num_rows_table_tmp integer = 0;

	
	line VARCHAR;
	line_values VARCHAR[];
	geo_column_name VARCHAR;
	table_name VARCHAR;

	sql VARCHAR;
	
	source_srid int; 
	grid_geom geometry;


BEGIN

	-- if now extent is craeted for given table just do it.
	IF ST_Area(grid_geom_in) = 0 THEN 
		grid_geom := cbg_get_table_extent(table_name_column_name_array);
		--RAISE NOTICE 'Create new grid geom  %', ST_AsText(grid_geom);
	ELSE 
		grid_geom := grid_geom_in;
	END IF;
	
	source_srid = ST_Srid(grid_geom);

	x_min := ST_XMin(grid_geom);
	x_max := ST_XMax(grid_geom);
	y_min := ST_YMin(grid_geom); 
	y_max := ST_YMax(grid_geom);

	grid_geom_meter := ST_Transform(grid_geom, metric_srid); 
	x_length_meter := ST_XMax(grid_geom_meter) - ST_XMin(grid_geom_meter);
	y_length_meter := ST_YMax(grid_geom_meter) - ST_YMin(grid_geom_meter);

	FOR i IN ARRAY_LOWER(table_name_column_name_array,1)..ARRAY_UPPER(table_name_column_name_array,1) LOOP
		line := table_name_column_name_array[i];
		raise NOTICE '%',line;
		
		SELECT string_to_array(line, ' ') INTO line_values; 

		table_name := line_values[1];
		geo_column_name := line_values[2];
	
		-- Use the && operator 
		-- We could here use any gis operation we vould like
		
		sql := 'SELECT count(*) FROM ' || table_name || ' WHERE ' || geo_column_name || ' && ' 
		|| 'ST_MakeEnvelope(' || x_min || ',' || y_min || ',' || x_max || ',' || y_max || ',' || source_srid || ')';


		raise NOTICE 'execute sql: %',sql;
		EXECUTE sql INTO num_rows_table_tmp ;
		
		num_rows_table := num_rows_table +  num_rows_table_tmp;

	END LOOP;

	IF 	x_length_meter < min_distance OR 
		y_length_meter < min_distance OR 
		num_rows_table < max_rows
	THEN
		sectors[0] := grid_geom;
		RAISE NOTICE 'x_length_meter, y_length_meter   %, % ', x_length_meter, y_length_meter ; 
	ELSE 
		x_delta := (x_max - x_min)/2;
		y_delta := (y_max - y_min)/2;  
		x_center := x_min + x_delta;
		y_center := y_min + y_delta;


		-- sw
		sectors[0] := cbg_content_based_balanced_grid(table_name_column_name_array,ST_MakeEnvelope(x_min,y_min,x_center,y_center, ST_SRID(grid_geom)), min_distance, max_rows);

		-- se
		sectors[1] := cbg_content_based_balanced_grid(table_name_column_name_array,ST_MakeEnvelope(x_center,y_min,x_max,y_center, ST_SRID(grid_geom)), min_distance, max_rows);
	
		-- ne
		sectors[2] := cbg_content_based_balanced_grid(table_name_column_name_array,ST_MakeEnvelope(x_min,y_center,x_center,y_max, ST_SRID(grid_geom)), min_distance, max_rows);

		-- se
		sectors[3] := cbg_content_based_balanced_grid(table_name_column_name_array,ST_MakeEnvelope(x_center,y_center,x_max,y_max, ST_SRID(grid_geom)), min_distance, max_rows);

	END IF;

  RETURN ST_Collect(sectors);

END;
$body$
LANGUAGE 'plpgsql';

-- Grant so all can use it
GRANT EXECUTE ON FUNCTION cbg_content_based_balanced_grid (	
													table_name_column_name_array VARCHAR[], 
													grid_geom_in geometry,
													min_distance integer,
													max_rows integer) to public;


-- Function with default values called with 2 parameters
-- Parameter 1 : An array of tables names and the name of geometry columns.
-- The table name must contain both schema and table name, The geometry column name must follow with one single space after the table name.
-- Parameter 2 : max_rows this is the max number rows that intersects with box before it's split into 4 new boxes 


CREATE OR REPLACE FUNCTION cbg_content_based_balanced_grid (
													table_name_column_name_array VARCHAR[],
													max_rows integer) 
													RETURNS geometry  AS
$body$
DECLARE

-- sending in a point will cause the table to use table extent
grid_geom geometry := ST_GeomFromText('POINT(0 0)');
-- set default min distance to 1000 meter
min_distance integer := 1000;

BEGIN
	return cbg_content_based_balanced_grid(
		table_name_column_name_array,
		grid_geom, 
		min_distance,
		max_rows);
END;
$body$
LANGUAGE 'plpgsql';


-- Grant so all can use it
GRANT EXECUTE ON FUNCTION cbg_content_based_balanced_grid (table_name_column_name_array VARCHAR[],max_rows integer) to public;

/**
 * Based on code from Joe Conway <mail@joeconway.com>
 * https://www.postgresql-archive.org/How-to-run-in-parallel-in-Postgres-td6114510.html
 * 
 */

DROP FUNCTION IF EXISTS execute_parallel(stmts text[]);
DROP FUNCTION IF EXISTS execute_parallel(stmts text[], num_parallel_thread int);

-- TODO add test return value
-- TODO catch error on main loop to be sure connenctinos are closed

CREATE OR REPLACE FUNCTION execute_parallel(stmts text[], num_parallel_thread int DEFAULT 3)
RETURNS boolean AS
$$
declare
  i int = 1;
  current_stmt_index int = 1;
  current_stmt_sent int = 0;
  new_stmt text;
  num_stmts_executed int = 0;
  num_stmts_failed int = 0;
  num_conn_opened int = 0;
  num_conn_notify int = 0;
  retv text;
  retvnull text;
  conn_status int;
  conntions_array text[];
  conn_stmts text[];
  connstr text;
  rv int;
  new_stmts_started boolean; 
  v_state text;
  v_msg text;
  v_detail text;
  v_hint text;
  v_context text;
  

  db text := current_database();
begin
	
	IF (Array_length(stmts, 1) IS NULL OR stmts IS NULL) THEN
       RAISE NOTICE 'No statements to execute';
       RETURN TRUE;
    ELSE
       RAISE NOTICE '% statements to execute in % threads', Array_length(stmts, 1), num_parallel_thread;
    END IF;
 	
	
	-- Check if num parallel theads if bugger than num stmts
	IF (num_parallel_thread > array_length(stmts,1)) THEN
  	  	num_parallel_thread = array_length(stmts,1);
  	END IF;

  	connstr := 'dbname=' || db;

  	
  	-- Open connections for num_parallel_thread
	BEGIN
	  	for i in 1..num_parallel_thread loop
		    conntions_array[i] := 'conn' || i::text;
		    perform dblink_connect(conntions_array[i], connstr);
		    num_conn_opened := num_conn_opened + 1;
		    conn_stmts[i] := null;
		end loop;
	EXCEPTION WHEN OTHERS THEN
	  	
	    GET STACKED DIAGNOSTICS v_state = RETURNED_SQLSTATE, v_msg = MESSAGE_TEXT, v_detail = PG_EXCEPTION_DETAIL, v_hint = PG_EXCEPTION_HINT,
                    v_context = PG_EXCEPTION_CONTEXT;
        RAISE NOTICE 'Failed to open all requested connections % , reduce to  % state  : %  message: % detail : % hint   : % context: %', 
        num_parallel_thread, num_conn_opened, v_state, v_msg, v_detail, v_hint, v_context;
		
		-- Check if num parallel theads if bugger than num stmts
		IF (num_conn_opened < num_parallel_thread) THEN
	  	  	num_parallel_thread = num_conn_opened;
	  	END IF;

	END;


	IF (num_conn_opened > 0) THEN
	  	-- Enter main loop
	  	LOOP 
	  	  new_stmts_started = false;
	  
		 -- check if connections are not used
		 FOR i IN 1..num_parallel_thread loop
		    IF (conn_stmts[i] is not null) THEN 
		      --select count(*) from dblink_get_notify(conntions_array[i]) into num_conn_notify;
		      --IF (num_conn_notify is not null and num_conn_notify > 0) THEN
		      SELECT dblink_is_busy(conntions_array[i]) into conn_status;
		      IF (conn_status = 0) THEN
			    conn_stmts[i] := null;
			    num_stmts_executed := num_stmts_executed + 1;
		    	BEGIN

			    	LOOP 
			    	  select val into retv from dblink_get_result(conntions_array[i]) as d(val text);
			    	  EXIT WHEN retv is null;
			    	END LOOP ;

				EXCEPTION WHEN OTHERS THEN
				    GET STACKED DIAGNOSTICS v_state = RETURNED_SQLSTATE, v_msg = MESSAGE_TEXT, v_detail = PG_EXCEPTION_DETAIL, v_hint = PG_EXCEPTION_HINT,
                    v_context = PG_EXCEPTION_CONTEXT;
                    RAISE NOTICE 'Failed get value for stmt: %s , using conn %, state  : % message: % detail : % hint : % context: %', conn_stmts[i], conntions_array[i], v_state, v_msg, v_detail, v_hint, v_context;
					num_stmts_failed := num_stmts_failed + 1;
		   	 	    perform dblink_disconnect(conntions_array[i]);
		            perform dblink_connect(conntions_array[i], connstr);
				END;
		      END IF;
		    END IF;
	        IF conn_stmts[i] is null AND current_stmt_index <= array_length(stmts,1) THEN
	            -- start next job
	            -- TODO remove duplicate job
		        new_stmt := stmts[current_stmt_index];
		        conn_stmts[i] :=  new_stmt;
		   		RAISE NOTICE 'New stmt (%) on connection %', new_stmt, conntions_array[i];
	    	    BEGIN
			    --rv := dblink_send_query(conntions_array[i],'BEGIN; '||new_stmt|| '; COMMIT;');
			    rv := dblink_send_query(conntions_array[i],new_stmt);
--		   	 	    perform dblink_disconnect(conntions_array[i]);
--		            perform dblink_connect(conntions_array[i], connstr);
			    new_stmts_started = true;
			    EXCEPTION WHEN OTHERS THEN
			      GET STACKED DIAGNOSTICS v_state = RETURNED_SQLSTATE, v_msg = MESSAGE_TEXT, v_detail = PG_EXCEPTION_DETAIL, v_hint = PG_EXCEPTION_HINT,
                  v_context = PG_EXCEPTION_CONTEXT;
                  RAISE NOTICE 'Failed to send stmt: %s , using conn %, state  : % message: % detail : % hint : % context: %', conn_stmts[i], conntions_array[i], v_state, v_msg, v_detail, v_hint, v_context;
				  num_stmts_failed := num_stmts_failed + 1;
		   	 	  perform dblink_disconnect(conntions_array[i]);
		          perform dblink_connect(conntions_array[i], connstr);
			    END;
				current_stmt_index = current_stmt_index + 1;
			END IF;
		    
		    
		  END loop;
		  
		  EXIT WHEN num_stmts_executed = Array_length(stmts, 1); 
		  
		  -- Do a slepp if nothings happens to reduce CPU load 
		  IF (new_stmts_started = false) THEN 
		  	--RAISE NOTICE 'Do sleep at num_stmts_executed %s current_stmt_index =% , array_length= %, new_stmts_started = %', 
		  	--num_stmts_executed,current_stmt_index, array_length(stmts,1), new_stmts_started;
			perform pg_sleep(0.0001);
		  END IF;
		END LOOP ;
	
		-- cose connections for num_parallel_thread
	  	for i in 1..num_parallel_thread loop
		    perform dblink_disconnect(conntions_array[i]);
		end loop;
  END IF;


  IF num_stmts_failed = 0 AND (current_stmt_index -1)= array_length(stmts,1) THEN
  	return true;
  else
  	return false;
  END IF;
  
END;
$$ language plpgsql;

GRANT EXECUTE on FUNCTION execute_parallel(stmts text[], num_parallel_thread int) TO public;


-- this is internal helper function
-- this is a function that creates unlogged tables and the the grid neeed when later checking this table for overlap and gaps. 
 
DROP FUNCTION IF EXISTS find_overlap_gap_init(
table_to_analyze_ varchar, -- The schema.table name with polygons to analyze for gaps and intersects
geo_collumn_name_ varchar, 	-- the name of geometry column on the table to analyze	
srid_ int, -- the srid for the given geo column on the table analyze
max_rows_in_each_cell int, -- this is the max number rows that intersects with box before it's split into 4 new boxes 
overlapgap_overlap_ varchar, -- The schema.table name for the overlap/intersects found in each cell 
overlapgap_gap_ varchar, -- The schema.table name for the gaps/holes found in each cell 
overlapgap_grid_ varchar, -- The schema.table name of the grid that will be created and used to break data up in to managle pieces
overlapgap_boundery_ varchar -- The schema.table name the outer boundery of the data found in each cell 
);

CREATE OR REPLACE FUNCTION find_overlap_gap_init(
table_to_analyze_ varchar, -- The schema.table name with polygons to analyze for gaps and intersects
geo_collumn_name_ varchar, 	-- the name of geometry column on the table to analyze	
srid_ int, -- the srid for the given geo column on the table analyze
max_rows_in_each_cell int, -- this is the max number rows that intersects with box before it's split into 4 new boxes 
overlapgap_overlap_ varchar, -- The schema.table name for the overlap/intersects found in each cell 
overlapgap_gap_ varchar, -- The schema.table name for the gaps/holes found in each cell 
overlapgap_grid_ varchar, -- The schema.table name of the grid that will be created and used to break data up in to managle pieces
overlapgap_boundery_ varchar -- The schema.table name the outer boundery of the data found in each cell 
)
    RETURNS INTEGER
AS $$DECLARE

	-- used to run commands
	command_string text;
	
	-- the number of cells created in the grid
	num_cells int;
	
	-- drop result tables
	drop_result_tables_ boolean = true;
	

	-- test table geo columns name
	geo_collumn_on_test_table_ varchar;

	
BEGIN

	geo_collumn_on_test_table_ := geo_collumn_name_;
	
	IF (drop_result_tables_ = true) THEN
		EXECUTE FORMAT('DROP TABLE IF EXISTS %s',overlapgap_grid_);
	END IF;

	-- create a content based grid
	EXECUTE FORMAT('CREATE TABLE %s( id serial, %s geometry(Geometry,%s))',overlapgap_grid_,geo_collumn_name_,srid_);
	
	command_string := FORMAT('INSERT INTO %s(%s) 
	SELECT q_grid.cell::geometry(geometry,%s)  as %s 
	FROM (
	SELECT(ST_Dump(
	cbg_content_based_balanced_grid(ARRAY[ %s],%s))
	).geom AS cell) AS q_grid',
	overlapgap_grid_,
	geo_collumn_name_,
	srid_,
	geo_collumn_name_,
	quote_literal(table_to_analyze_ || ' ' || geo_collumn_on_test_table_)::text,
	max_rows_in_each_cell
	);
	-- display
	RAISE NOTICE 'command_string %.', command_string;
	-- execute the sql command
	EXECUTE command_string;

	-- Add more attributes to content based grid

	-- Number of rows in this in box
	EXECUTE FORMAT('ALTER table  %s add column num_rows_data int',overlapgap_grid_);

	-- Total number of overlaps that is line found in this box
	EXECUTE FORMAT('ALTER table  %s add column num_overlap int',overlapgap_grid_);

	-- Number of overlaps with surface found this box
	EXECUTE FORMAT('ALTER table  %s add column num_overlap_poly int',overlapgap_grid_);

	-- Total number of gaps that is a point found in this box
	EXECUTE FORMAT('ALTER table  %s add column num_gap int',overlapgap_grid_);

	-- Number of gaps with surface found this box
	EXECUTE FORMAT('ALTER table  %s add column num_gap_poly int',overlapgap_grid_);

	-- Just a check to see if the a exeception
	EXECUTE FORMAT('ALTER table  %s add column ok_exit boolean default false',overlapgap_grid_);
	
	
	
	command_string := FORMAT('CREATE INDEX ON %s USING GIST (%s)',overlapgap_grid_,geo_collumn_on_test_table_);
	-- display
	RAISE NOTICE 'command_string % .', command_string;
	-- execute the sql command
	EXECUTE command_string;

	
	-- count number of cells
	command_string := FORMAT('SELECT count(*) from %s',overlapgap_grid_);
	-- display
	RAISE NOTICE 'command_string % .', command_string;
	-- execute the sql command
	EXECUTE command_string  INTO num_cells;

	-- create a table to keep the boundery of the data found in the data table
	IF (drop_result_tables_ = true) THEN
		EXECUTE FORMAT('DROP TABLE IF EXISTS %s',overlapgap_boundery_);
	END IF;
	EXECUTE FORMAT('CREATE UNLOGGED TABLE %s( id serial, cell_id int, %s geometry(Geometry,%s))',overlapgap_boundery_,geo_collumn_name_,srid_);

	-- create table where intersected data from ar5 are stored
	IF (drop_result_tables_ = true) THEN
		EXECUTE FORMAT('DROP TABLE IF EXISTS %s',overlapgap_overlap_);
	END IF;
	EXECUTE FORMAT('CREATE UNLOGGED TABLE %s( id serial, cell_id int, %s geometry(Geometry,%s))',overlapgap_overlap_,geo_collumn_name_,srid_);

	-- create table where for to find gaps for ar5 
	IF (drop_result_tables_ = true) THEN
		EXECUTE FORMAT('DROP TABLE IF EXISTS %s',overlapgap_gap_);
	END IF;
	EXECUTE FORMAT('CREATE UNLOGGED TABLE %s( id serial, cell_id int, outside_data_boundery boolean default true, %s geometry(Geometry,%s))',overlapgap_gap_,geo_collumn_name_,srid_);

	return num_cells;

END;
$$
LANGUAGE plpgsql;

GRANT EXECUTE on FUNCTION  find_overlap_gap_init(
table_to_analyze_ varchar, -- The schema.table name with polygons to analyze for gaps and intersects
geo_collumn_name_ varchar, 	-- the name of geometry column on the table to analyze	
srid_ int, -- the srid for the given geo column on the table analyze
max_rows_in_each_cell int, -- this is the max number rows that intersects with box before it's split into 4 new boxes 
overlapgap_overlap_ varchar, -- The schema.table name for the overlap/intersects found in each cell 
overlapgap_gap_ varchar, -- The schema.table name for the gaps/holes found in each cell 
overlapgap_grid_ varchar, -- The schema.table name of the grid that will be created and used to break data up in to managle pieces
overlapgap_boundery_ varchar -- The schema.table name the outer boundery of the data found in each cell 
) TO public;



DROP PROCEDURE IF EXISTS find_overlap_gap_run(
table_to_analyze_ varchar, -- The table to analyze 
geo_collumn_name_ varchar, 	-- the name of geometry column on the table to analyze	
srid_ int, -- the srid for the given geo column on the table analyze
table_name_result_prefix_ varchar, -- This is table name prefix including schema used for the result tables
max_rows_in_each_cell_ int -- this is the max number rows that intersects with box before it's split into 4 new boxes, default is 5000
);

DROP PROCEDURE IF EXISTS find_overlap_gap_run(
table_to_analyze_ varchar, -- The table to analyze 
geo_collumn_name_ varchar, 	-- the name of geometry column on the table to analyze	
srid_ int, -- the srid for the given geo column on the table analyze
table_name_result_prefix_ varchar, -- This is the prefix used for the result tables
max_parallel_jobs_ int, -- this is the max number of paralell jobs to run. There must be at least the same number of free connections
max_rows_in_each_cell_ int -- this is the max number rows that intersects with box before it's split into 4 new boxes, default is 5000
);

CREATE OR REPLACE PROCEDURE find_overlap_gap_run(
table_to_analyze_ varchar, -- The table to analyze 
geo_collumn_name_ varchar, 	-- the name of geometry column on the table to analyze	
srid_ int, -- the srid for the given geo column on the table analyze
table_name_result_prefix_ varchar, -- This is table name prefix including schema used for the result tables
-- || '_overlap'; -- The schema.table name for the overlap/intersects found in each cell 
-- || '_gap'; -- The schema.table name for the gaps/holes found in each cell 
-- || '_grid'; -- The schema.table name of the grid that will be created and used to break data up in to managle pieces
-- || '_boundery'; -- The schema.table name the outer boundery of the data found in each cell 
-- NB. Any exting data will related to this table names will be deleted 

max_parallel_jobs_ int, -- this is the max number of paralell jobs to run. There must be at least the same number of free connections
max_rows_in_each_cell_ int DEFAULT 5000 -- this is the max number rows that intersects with box before it's split into 4 new boxes, default is 5000
) LANGUAGE plpgsql 
AS $$
DECLARE
	command_string text;
	num_rows int;

	part text;	
	id_list_tmp int[];
	this_list_id int;
	
	stmts text[];

	func_call text;
	
	
	-- the number of cells created in the grid
	num_cells int;

	overlapgap_overlap varchar = table_name_result_prefix_ || '_overlap'; -- The schema.table name for the overlap/intersects found in each cell 
	overlapgap_gap varchar = table_name_result_prefix_ || '_gap'; -- The schema.table name for the gaps/holes found in each cell 
	overlapgap_grid varchar  = table_name_result_prefix_ || '_grid'; -- The schema.table name of the grid that will be created and used to break data up in to managle pieces
	overlapgap_boundery varchar = table_name_result_prefix_ || '_boundery'; -- The schema.table name the outer boundery of the data found in each cell 
	
	call_result boolean;

BEGIN

	
	--select * from geometry_columns;
	
	--Generate command to create grid
	command_string := FORMAT('SELECT find_overlap_gap_init(%s,%s,%s,%s,%s,%s,%s,%s)',
	quote_literal(table_to_analyze_),
	quote_literal(geo_collumn_name_),
	srid_,
	max_rows_in_each_cell_,
	quote_literal(overlapgap_overlap),
	quote_literal(overlapgap_gap),
	quote_literal(overlapgap_grid),
	quote_literal(overlapgap_boundery)
	);
		
	-- display the string
	RAISE NOTICE '%', command_string;
	-- execute the string
	EXECUTE command_string INTO num_cells;

	-- Get list id from grid and make id list
	command_string := FORMAT('SELECT array_agg(DISTINCT id) from %s',overlapgap_grid);
		-- display the string
	RAISE NOTICE '%', command_string;
	-- execute the string
	EXECUTE command_string INTO id_list_tmp;


	-- create a table to hold call stack
	DROP TABLE IF EXISTS return_call_list;
	CREATE TEMP TABLE return_call_list (func_call text);

	-- create call for each cell
	FOREACH this_list_id IN ARRAY id_list_tmp
	LOOP 
		func_call := FORMAT('SELECT find_overlap_gap_single_cell(%s,%s,%s,%s,%s,%s)',quote_literal(table_to_analyze_),quote_literal(geo_collumn_name_),srid_,
		quote_literal(table_name_result_prefix_),this_list_id,num_cells);
		INSERT INTO return_call_list(func_call) VALUES (func_call);
		stmts[this_list_id] = func_call;
	END loop;


	COMMIT;
	
	select execute_parallel(stmts,max_parallel_jobs_) into call_result;
	
	IF (call_result = false) THEN 
		RAISE EXCEPTION 'Failed to run overlap and gap for % with the following statement list %', table_to_analyze_, stmts;
	END IF;
	

END $$;

GRANT EXECUTE on PROCEDURE find_overlap_gap_run( 
table_to_analyze_ varchar, -- The table to analyze 
geo_collumn_name_ varchar, 	-- the name of geometry column on the table to analyze	
srid_ int, -- the srid for the given geo column on the table analyze
table_name_result_prefix_ varchar, -- This is table name prefix including schema used for the result tables
-- || '_overlap'; -- The schema.table name for the overlap/intersects found in each cell 
-- || '_gap'; -- The schema.table name for the gaps/holes found in each cell 
-- || '_grid'; -- The schema.table name of the grid that will be created and used to break data up in to managle pieces
-- || '_boundery'; -- The schema.table name the outer boundery of the data found in each cell 
-- NB. Any exting data will related to this table names will be deleted 

max_parallel_jobs_ int, -- this is the max number of paralell jobs to run. There must be at least the same number of free connections
max_rows_in_each_cell_ int  -- this is the max number rows that intersects with box before it's split into 4 new boxes, default is 5000
) TO public;





DROP FUNCTION IF EXISTS find_overlap_gap_single_cell(
 	table_to_analyze_ varchar, -- The table to analyze 
 	geo_collumn_name_ varchar, 	-- the name of geometry column on the table to analyze	
 	srid_ int, -- the srid for the given geo column on the table analyze
 	table_name_result_prefix_ varchar, -- This is the prefix used for the result tables
	this_list_id int, num_cells int
);

CREATE OR REPLACE FUNCTION find_overlap_gap_single_cell(
		table_to_analyze_ varchar, -- The table to analyze 
	geo_collumn_name_ varchar, 	-- the name of geometry column on the table to analyze	
	srid_ int, -- the srid for the given geo column on the table analyze
	table_name_result_prefix_ varchar, -- This is the prefix used for the result tables
	this_list_id int, num_cells int)
    RETURNS text
AS $$DECLARE
	command_string text;
	
	num_rows_data int;

	num_rows_overlap int;
	num_rows_gap int;
	num_rows_overlap_area int;
	num_rows_gap_area int;
	
	
	id_list_tmp int[];
	
	overlapgap_overlap varchar = table_name_result_prefix_ || '_overlap'; -- The schema.table name for the overlap/intersects found in each cell 
	overlapgap_gap varchar = table_name_result_prefix_ || '_gap'; -- The schema.table name for the gaps/holes found in each cell 
	overlapgap_grid varchar  = table_name_result_prefix_ || '_grid'; -- The schema.table name of the grid that will be created and used to break data up in to managle pieces
	overlapgap_boundery varchar = table_name_result_prefix_ || '_boundery'; -- The schema.table name the outer boundery of the data found in each cell 

BEGIN

	-- create table where intersected data from ar5 are stored
	EXECUTE FORMAT('DROP TABLE IF EXISTS overlapgap_cell_data');
	EXECUTE FORMAT('CREATE TEMP TABLE overlapgap_cell_data( id serial, %s geometry(Geometry,%s))',geo_collumn_name_,srid_);

	-- get data from ar5 and intersect with current box
	command_string := FORMAT(
	'INSERT INTO overlapgap_cell_data(%s)
	SELECT * FROM 
	( SELECT 
		(ST_Dump(ST_intersection(cc.%s,a1.%s))).geom as %s
		FROM 
		%s a1,
		%s cc
		WHERE 
		cc.id = %s AND
		cc.%s && a1.%s AND
		ST_Intersects(cc.%s,a1.%s)
	) AS r
	WHERE ST_area(r.%s) > 0',
	geo_collumn_name_,
	geo_collumn_name_,
	geo_collumn_name_,
	geo_collumn_name_,
	table_to_analyze_,
	overlapgap_grid,
	this_list_id,
	geo_collumn_name_,
	geo_collumn_name_,
	geo_collumn_name_,
	geo_collumn_name_,
	geo_collumn_name_
);

	execute command_string ;
	
	EXECUTE FORMAT('CREATE INDEX geoidx_overlapgap_cell_data_flate ON overlapgap_cell_data USING GIST (%s)',geo_collumn_name_); 

	-- count total number of rows
	command_string := FORMAT('SELECT count(*) FROM overlapgap_cell_data');
	-- display
	RAISE NOTICE 'command_string % .', command_string;
	-- execute the sql command
	EXECUTE command_string  INTO num_rows_data;
	RAISE NOTICE 'Total number of % rows for cell %(%)', num_rows_data, this_list_id,num_cells;


	command_string := FORMAT(
	'INSERT INTO %s(cell_id,%s) 
	SELECT  %L as cell_id, ST_union(r.%s) AS %s FROM 
	( SELECT 
		a1.%s
		FROM 
		overlapgap_cell_data a1
	) AS r',
	overlapgap_boundery,
	geo_collumn_name_,
	this_list_id,
	geo_collumn_name_,
	geo_collumn_name_,
	geo_collumn_name_);

--	RAISE NOTICE '%', command_string;
	
	execute command_string;
	

	-- get data from overlapp objects
	command_string := FORMAT(
	'INSERT INTO %s(cell_id,%s)
	SELECT %L as cell_id, %s FROM 
	(
		SELECT DISTINCT ST_Intersection(a1.%s,a2.%s) AS %s
		FROM 
		overlapgap_cell_data a1,
		overlapgap_cell_data a2
		WHERE 
		a1.%s && a2.%s AND
		ST_Overlaps(a1.%s,a2.%s) AND
		NOT ST_Equals(a1.%s,a2.%s)
	) as r WHERE ST_area(%s) > 0',
	overlapgap_overlap,
	geo_collumn_name_,
	this_list_id,
	geo_collumn_name_,
	geo_collumn_name_,
	geo_collumn_name_,
	geo_collumn_name_,
	geo_collumn_name_,
	geo_collumn_name_,
	geo_collumn_name_,
	geo_collumn_name_,
	geo_collumn_name_,
	geo_collumn_name_,
	geo_collumn_name_	
	);

	--RAISE NOTICE '%', command_string;
	execute command_string ;

	-- find gaps (where it no data)
	command_string := FORMAT(
	'INSERT INTO %s(cell_id,%s)
	SELECT %L as cell_id, r.%s FROM
	( SELECT %s FROM 
		(	
			SELECT DISTINCT (ST_Dump(ST_Difference(cc.%s,r.%s))).geom AS %s
			FROM 
			(
				SELECT %s FROM (
					SELECT ST_Union(r.%s) AS %s FROM
					( SELECT 
						(ST_Dump(ST_Union(a1.%s))).geom as %s
						FROM 
						overlapgap_cell_data a1
					) AS r
					WHERE ST_area(r.%s) > 0
				) AS r
			) AS r,
			%s cc
			WHERE cc.id = %s
		) AS r
	) AS r',
	overlapgap_gap,
	geo_collumn_name_,
	this_list_id,
	geo_collumn_name_,
	geo_collumn_name_,
	geo_collumn_name_,
	geo_collumn_name_,
	geo_collumn_name_,
	geo_collumn_name_,
	geo_collumn_name_,
	geo_collumn_name_,
	geo_collumn_name_,
	geo_collumn_name_,
	geo_collumn_name_,
	overlapgap_grid,
	this_list_id);

	--RAISE NOTICE '%', command_string;
	execute command_string ;

	-- count total number of overlaps
	command_string := FORMAT('SELECT  count(*) 
	FROM ( SELECT  (ST_dump(%s)).geom as geom from %s where cell_id = %s) as r',geo_collumn_name_,overlapgap_overlap,this_list_id);
	-- display
	RAISE NOTICE 'command_string % .', command_string;
	-- execute the sql command
	EXECUTE command_string  INTO num_rows_overlap;
	RAISE NOTICE 'Total overlaps is % for cell number %(%)', num_rows_overlap, this_list_id,num_cells;

	-- count number of overlaps with area
	command_string := FORMAT('SELECT  count(*) 
	FROM ( SELECT  (ST_dump(%s)).geom as geom from %s where cell_id = %s) as r 
	WHERE ST_Area(r.geom) > 0',geo_collumn_name_,overlapgap_overlap,this_list_id);
	-- display
	RAISE NOTICE 'command_string % .', command_string;
	-- execute the sql command
	EXECUTE command_string  INTO num_rows_overlap_area;
	RAISE NOTICE 'Total overlaps is % for cell number %(%)', num_rows_overlap, this_list_id,num_cells;

	-- count total number of gaps
	command_string := FORMAT('SELECT  count(*) 
	FROM ( SELECT  (ST_dump(%s)).geom as geom from %s where cell_id = %s) as r',geo_collumn_name_,overlapgap_gap,this_list_id);
	-- display
	RAISE NOTICE 'command_string % .', command_string;
	-- execute the sql command
	EXECUTE command_string  INTO num_rows_gap;
	RAISE NOTICE 'Total gaps is % for cell number %(%)', num_rows_gap, this_list_id,num_cells;

	-- count number of gaps with area
	command_string := FORMAT('SELECT  count(*) 
	FROM ( SELECT  (ST_dump(%s)).geom as geom from %s where cell_id = %s) as r 
	WHERE ST_Area(r.geom) > 0',geo_collumn_name_,overlapgap_gap,this_list_id);
	-- display
	RAISE NOTICE 'command_string % .', command_string;
	-- execute the sql command
	EXECUTE command_string  INTO num_rows_gap_area;
	RAISE NOTICE 'Total gaps is % for cell number %(%)', num_rows_gap, this_list_id,num_cells;
 
	command_string := FORMAT('UPDATE %s 
	set ok_exit=true,
	num_overlap=%s,
	num_overlap_poly=%s,
	num_gap=%s,
	num_gap_poly=%s, 
	num_rows_data=%s 
	WHERE id = %s',
	overlapgap_grid,
	num_rows_overlap,
	num_rows_overlap_area,
	num_rows_gap,
	num_rows_gap_area,
	num_rows_data,
	this_list_id);

	EXECUTE command_string;
	
	return 'num_rows_overlap:' || num_rows_overlap || ', num_rows_gap:' || num_rows_gap;


END;
$$
LANGUAGE plpgsql PARALLEL SAFE COST 1;

GRANT EXECUTE on FUNCTION find_overlap_gap_single_cell(
	table_to_analyze_ varchar, -- The table to analyze 
	geo_collumn_name_ varchar, 	-- the name of geometry column on the table to analyze	
	srid_ int, -- the srid for the given geo column on the table analyze
	table_name_result_prefix_ varchar, -- This is the prefix used for the result tables
	this_list_id int, num_cells int
) TO public;
 


-- TODO remove this code and make it generic
-- create schema for topo_rein data, tables, ....

CREATE SCHEMA topo_rein;

-- give puclic access
GRANT USAGE ON SCHEMA topo_rein TO public;

-- This function is used to create indexes
CREATE OR REPLACE FUNCTION topo_rein.get_relation_id (geo TopoGeometry)
  RETURNS integer
  AS $$
DECLARE
  relation_id integer;
BEGIN
  relation_id := (geo).id;
  RETURN relation_id;
END;
$$
LANGUAGE plpgsql
IMMUTABLE;

COMMENT ON FUNCTION topo_rein.get_relation_id (TopoGeometry) IS 'Return the id used to find the row in the relation for polygons). Needed to create function based indexs.';

-- A composite type to hold sosi kopi_data
CREATE TYPE topo_rein.sosi_kopidata AS (
  omradeid smallint, originaldatavert Varchar ( 50), kopidato DATE);

-- A composite type to hold sosi registreringsversjon
CREATE TYPE topo_rein.sosi_registreringsversjon AS (
  produkt varchar, versjon varchar
);

-- A composite type to hold sosi kvalitet
-- beskrivelse av kvaliteten på stedfestingen

CREATE TYPE topo_rein.sosi_kvalitet AS (
  -- metode for måling i grunnriss (x,y), og høyde (z) når metoden er den samme som ved måling i grunnriss
  -- TODO Hentes fra kode tabell eller bruke en constraint ???

  maalemetode smallint,
  -- punktstandardavviket i grunnriss for punkter samt tverravvik for linjer
  -- Merknad: Oppgitt i cm

  noyaktighet integer,
  -- hvor godt den kartlagte detalj var synbar ved kartleggingen
  -- TODO Hentes fra kode tabell eller bruke en constraint ???

  synbarhet smallint
);

-- A composite type to hold sosi sosi felles egenskaper
CREATE TYPE topo_rein.sosi_felles_egenskaper AS (
  -- identifikasjondato når data ble registrert/observert/målt første gang, som utgangspunkt for første digitalisering
  -- Merknad:førsteDatafangstdato brukes hvis det er av interesse å forvalte informasjon om når en ble klar over objektet. Dette kan for eksempel gjelde datoen for første flybilde som var utgangspunkt for registrering i en database.
  -- lage regler for hvordan den skal brukes, kan i mange tilfeller arves
  -- henger sammen med UUID, ny UUID ny datofangst dato

  forstedatafangstdato DATE,
  -- Unik identifikasjon av et objekt, ivaretatt av den ansvarlige produsent/forvalter, som kan benyttes av eksterne applikasjoner som referanse til objektet.
  -- NOTE1 Denne eksterne objektidentifikasjonen må ikke forveksles med en tematisk objektidentifikasjon, slik som f.eks bygningsnummer.
  -- NOTE 2 Denne unike identifikatoren vil ikke endres i løpet av objektets levetid.
  -- TODO Test if we can use this as a unique id.

  identifikasjon varchar,
  -- bygd opp navnerom/lokalid/versjon
  -- navnerom: NO_LDIR_REINDRIFT_VAARBEITE
  -- versjon: 0
  -- lokalid:  rowid
  -- eks identifikasjon = "NO_LDIR_REINDRIFT_VAARBEITE 0 199999999"
  -- beskrivelse av kvaliteten på stedfestingen
  -- Merknad: Denne er identisk med ..KVALITET i tidligere versjoner av SOSI.

  kvalitet topo_rein.sosi_kvalitet,
  -- dato for siste endring på objektetdataene
  -- Merknad: Oppdateringsdato kan være forskjellig fra Datafangsdato ved at data som er registrert kan bufres en kortere eller lengre periode før disse legges inn i datasystemet (databasen).
  -- Definition: Date and time at which this version of the spatial object was inserted or changed in the spatial data set.

  oppdateringsdato DATE,
  -- referanse til opphavsmaterialet, kildematerialet, organisasjons/publiseringskilde
  -- Merknad: Kan også beskrive navn på person og årsak til oppdatering

  opphav Varchar ( 255),
  -- dato når dataene er fastslått å være i samsvar med virkeligheten
  -- Merknad: Verifiseringsdato er identisk med ..DATO i tidligere versjoner av SOSI	verifiseringsdato DATE
  -- lage regler for hvordan den skal brukes
  -- flybilde fra 2008 vil gi data 2008, må være input fra brukeren

  verifiseringsdato DATE,
  -- Hva gjør vi med disse verdiene som vi har brukt tidligere brukte  i AR5 ?
  -- Er vi sikre på at vi ikke trenger de
  -- datafangstdato DATE,
  -- Vet ikke om vi skal ha med den, må tenke litt
  -- Skal ikke være med hvis Knut og Ingvild ikke sier noe annet
  -- vil bli et produktspek til ???
  -- taes med ikke til slutt brukere

  informasjon Varchar(255)
  ARRAY,
  -- trengs ikke i følge Knut og Ingvild
  -- kopidata topo_rein.sosi_kopidata,
  -- trengs ikke i følge Knut og Ingvild
  -- prosess_historie VARCHAR(255) ARRAY,
  -- kan være forskjellige verdier ut fra når data ble lagt f.eks null verdier for nye attributter eldre enn 4.0
  -- bør være med

  registreringsversjon topo_rein.sosi_registreringsversjon);

-- this is type used extrac data from json
CREATE TYPE topo_rein.simple_sosi_felles_egenskaper AS (
  "fellesegenskaper.forstedatafangstdato" date, "fellesegenskaper.verifiseringsdato" date, "fellesegenskaper.oppdateringsdato" date, "fellesegenskaper.opphav" varchar,
  "fellesegenskaper.kvalitet.maalemetode" int, "fellesegenskaper.kvalitet.noyaktighet" int, "fellesegenskaper.kvalitet.synbarhet" smallint
);

-- A composite type to hold key value that will recoreded before a update
-- and compared after the update, used be sure no changes hapends out side
-- the area that should be updated
-- DROP TYPE topo_rein.closeto_values_type cascade;

CREATE TYPE topo_rein.closeto_values_type AS (
  -- line length that intersects reinflate
  closeto_length_reinflate_inter numeric,
  -- line count that intersects the edge
  closeto_count_edge_inter int,
  -- line count that intersetcs reinlinje
  closeto_count_reinlinje_inter int,
  -- used to check that attribute value close has not changed a close to
  artype_and_length_as_text text,
  -- used to check that the area is ok after update
  -- as we use today we do not remove any data we just add new polygins or change exiting
  -- the layer should always be covered

  envelope_area_inter numeric
);

-- TODO add more comments
COMMENT ON COLUMN topo_rein.sosi_felles_egenskaper.verifiseringsdato IS 'Sosi common meta attribute';

COMMENT ON COLUMN topo_rein.sosi_felles_egenskaper.opphav IS 'Sosi common meta attribute';

COMMENT ON COLUMN topo_rein.sosi_felles_egenskaper.informasjon IS 'Sosi common meta attribute';

-- create schema for topo_update data, tables, .... 
CREATE SCHEMA topo_update;

-- make comment this schema  
COMMENT ON SCHEMA topo_update IS 'Is a schema for topo_update attributes and ref to topolygy data. Don´t do any direct update on tables in this schema, all changes should be done using stored proc.';

-- make the scema public
GRANT USAGE ON SCHEMA topo_update to public;


-- craeted to make it possible to return a set of objects from the topo function
-- Todo find a better way to du this 
DROP TABLE IF EXISTS topo_update.topogeometry_def; 
CREATE TABLE topo_update.topogeometry_def(topo topogeometry);


---------------------------------------------------------------------------------

-- A composite type to hold infor about the currrent layers that will be updated 
-- this will be used to pick up meta info from the topolgy layer doing a update
CREATE TYPE topo_update.input_meta_info 
AS (
	-- refferes to topology.topology
	topology_name varchar,
	
	-- reffers to topology.layer
	layer_schema_name varchar,
	layer_table_name varchar,
	layer_feature_column varchar,

	-- For a edge this is 2 and for a surface this is 3 
	element_type int,

	-- this is the snapp to tolerance used for snap to when adding new vector data 
	-- a typical value used for degrees is 0.0000000001
	snap_tolerance float8,
	
	-- this is computed by using function topo_update.get_topo_layer_id
	border_layer_id int,
	
	-- refferes to topology.topology
	srid  int
	

);



---------------------------------------------------------------------------------

-- A composite type to hold infor about the currrent layers that will be updated 
-- this will be used to pick up meta info from the topolgy layer doing a update
CREATE TYPE topo_update.json_input_structure 
AS (

-- the input geo picked from the client properties
input_geo geometry,

-- JSON that is sent from the client combained with the server json properties
json_properties json,

-- this build up based on the input json  this used for both line and  point
sosi_felles_egenskaper topo_rein.sosi_felles_egenskaper,

-- this only used for the surface objectand does not contain any info about drawing
sosi_felles_egenskaper_flate topo_rein.sosi_felles_egenskaper

);

-- This is a common method to parse all input data
-- It returns a struture that is adjusted reindrift that depends on sosi felles eganskaper


DROP FUNCTION IF EXISTS topo_update.handle_input_json_props(json, json, int,boolean) ;
DROP FUNCTION IF EXISTS topo_update.handle_input_json_props(json, json, int) ;

CREATE OR REPLACE FUNCTION  topo_update.handle_input_json_props(client_json_feature json,  server_json_feature json, srid_out int) 
RETURNS topo_update.json_input_structure AS $$DECLARE

DECLARE 
use_default_dates boolean = true;
BEGIN
return  topo_update.handle_input_json_props(client_json_feature,  server_json_feature, srid_out, use_default_dates); 
END;
$$ LANGUAGE plpgsql IMMUTABLE;



-- This is a common method to parse all input data
-- It returns a struture that is adjusted reindrift that depends on sosi felles eganskaper

CREATE OR REPLACE FUNCTION  topo_update.handle_input_json_props(client_json_feature json,  server_json_feature json, srid_out int, use_default_dates boolean) 
RETURNS topo_update.json_input_structure AS $$DECLARE

DECLARE 
-- holds the value for felles egenskaper from input
simple_sosi_felles_egenskaper topo_rein.simple_sosi_felles_egenskaper;

-- JSON that is sent from the cleint
client_json_properties json;

-- JSON produced on the server side
server_json_properties json;

-- Keys in the server JSON properties
server_json_keys text;
keys_to_set   TEXT[];
values_to_set json[];

-- holde the computed value for json input reday to use
json_input_structure topo_update.json_input_structure;  

BEGIN

	RAISE NOTICE 'client_json_feature %, server_json_feature % use_default_dates %',  client_json_feature, server_json_feature , use_default_dates;
	
	-- geth the geometry may be null
	json_input_structure.input_geo := topo_rein.get_geom_from_json(client_json_feature::json,srid_out);

	-- get json from the client
	client_json_properties := to_json(client_json_feature::json->'properties');
	RAISE NOTICE 'client_json_properties %',  client_json_properties ;
	
	-- get the json from the serrver, may be null
	IF server_json_feature IS NOT NULL THEN
		server_json_properties := to_json(server_json_feature::json->'properties');
	  	RAISE NOTICE 'server_json_properties  % ',  server_json_properties ;
	
		-- overwrite client JSON properties with server property values
	  	SELECT array_agg("key"),array_agg("value")  INTO keys_to_set,values_to_set
		FROM json_each(server_json_properties) WHERE "value"::text != 'null';
		client_json_properties := topo_update.json_object_set_keys(client_json_properties, keys_to_set, values_to_set);
		RAISE NOTICE 'json_properties after update  %',  client_json_properties ;
	END IF;

	json_input_structure.json_properties := client_json_properties;
	
	-- This maps from the simple format used on the client 
	-- Because the client do not support Postgres user defined types like we have used in  topo_rein.sosi_felles_egenskaper;
	-- First append the info from the client properties, only properties that maps to valid names in topo_rein.simple_sosi_felles_egenskaper will be used.
	simple_sosi_felles_egenskaper := json_populate_record(NULL::topo_rein.simple_sosi_felles_egenskaper,client_json_properties );

	RAISE NOTICE 'felles_egenskaper_sosi point/line before  %',  simple_sosi_felles_egenskaper;

	-- Here we map from simple properties to topo_rein.sosi_felles_egenskaper for line an point objects
	json_input_structure.sosi_felles_egenskaper := topo_rein.get_rein_felles_egenskaper(simple_sosi_felles_egenskaper,use_default_dates);
	
	RAISE NOTICE 'felles_egenskaper_sosi point/line after  %',  json_input_structure.sosi_felles_egenskaper;
	
	-- Here we get info for the surface objects
   	json_input_structure.sosi_felles_egenskaper_flate := topo_rein.get_rein_felles_egenskaper_flate(simple_sosi_felles_egenskaper,use_default_dates);
	

	RETURN json_input_structure;

END;
$$ LANGUAGE plpgsql IMMUTABLE;


-- apply the list of new surfaces to the exting list of object
-- pick values from objects close to an so on
-- return the id's of the rows affected

-- DROP FUNCTION topo_update.update_domain_surface_layer(_new_topo_objects regclass) cascade;


CREATE OR REPLACE FUNCTION topo_update.update_domain_surface_layer(surface_topo_info topo_update.input_meta_info, border_topo_info topo_update.input_meta_info, json_input_structure topo_update.json_input_structure,  _new_topo_objects regclass) 
RETURNS SETOF topo_update.topogeometry_def AS $$
DECLARE

-- this border layer id will picked up by input parameters
border_layer_id int;

-- this surface layer id will picked up by input parameters
surface_layer_id int;

-- this is the tolerance used for snap to 
snap_tolerance float8 = null;

-- hold striped gei
edge_with_out_loose_ends geometry = null;

-- holds dynamic sql to be able to use the same code for different
command_string text;

-- holds the num rows affected when needed
num_rows_affected int;

-- number of rows to delete from org table
num_rows_to_delete int;

-- The border topology
new_border_data topology.topogeometry;

-- used for logging
add_debug_tables int = 0;

-- array of quoted field identifiers
-- for attribute fields passed in by user and known (by name)
-- in the target table
update_fields text[];

-- array of quoted field identifiers
-- for attribute fields passed in by user and known (by name)
-- in the temp table
update_fields_t text[];

-- String surface layer name
surface_layer_name text;

-- the closed geom if the instring is closed
valid_closed_user_geometry geometry = null;

-- temp variable
temp_text_var TEXT;



BEGIN
	-- this is the tolerance used for snap to 
	snap_tolerance := surface_topo_info.snap_tolerance;
	
	-- find border layer id
	border_layer_id := border_topo_info.border_layer_id;
	RAISE NOTICE 'topo_update.update_domain_surface_layer border_layer_id   %',  border_layer_id ;
	
	-- find surface layer id
	surface_layer_id := surface_topo_info.border_layer_id;
	RAISE NOTICE 'topo_update.update_domain_surface_layer surface_layer_id   %',  surface_layer_id ;

	surface_layer_name := surface_topo_info.layer_schema_name || '.' || surface_topo_info.layer_table_name;

	-- check if this is closed polygon drawn by the user 
	-- if it's a closed polygon the only surface inside this polygon should be affected
	IF St_IsClosed(json_input_structure.input_geo) THEN
		valid_closed_user_geometry = ST_MakePolygon(json_input_structure.input_geo);
	END IF;

	-- get the data into a new tmp table
	DROP TABLE IF EXISTS new_surface_data; 

	
	EXECUTE format('CREATE TEMP TABLE new_surface_data AS (SELECT * FROM %s)', _new_topo_objects);
	ALTER TABLE new_surface_data ADD COLUMN id_foo SERIAL PRIMARY KEY;
	ALTER TABLE new_surface_data ADD COLUMN status_foo int default 0;

	
	DROP TABLE IF EXISTS old_surface_data; 
	-- Find out if any old topo objects overlaps with this new objects using the relation table
	-- by using the surface objects owned by the both the new objects and the exting one
	-- Exlude the the new surface object created
	-- We are using the rows in new_surface_data to cpare with, this contains all the rows which are affected
	command_string :=  format('CREATE TEMP TABLE old_surface_data AS 
	(SELECT 
	re.* 
	FROM 
	%I.relation re,
	%I.relation re_tmp,
	new_surface_data new_sd
	WHERE 
	re.layer_id =%L AND
	re.element_type = 3 AND
	re.element_id = re_tmp.element_id AND
	re_tmp.layer_id = %L AND
	re_tmp.element_type = 3 AND
	(new_sd.surface_topo).id = re_tmp.topogeo_id AND
	(new_sd.surface_topo).id != re.topogeo_id)',
    surface_topo_info.topology_name,
    surface_topo_info.topology_name,
    surface_layer_id,
    surface_layer_id);  
	EXECUTE command_string;
	
	DROP TABLE IF EXISTS old_surface_data_not_in_new; 
	-- Find any old objects that are not covered totaly by new surfaces 
	-- This objets should not be deleted, but the geometry should only decrease in size.
	-- TODO Take a disscusion about how to handle attributtes in this cases
	-- TODO add a test case for this
	command_string :=  format('CREATE TEMP TABLE old_surface_data_not_in_new AS 
	(SELECT 
	re.* 
	FROM 
	%I.relation re,
	old_surface_data re_tmp
	WHERE 
	re.layer_id = %L AND
	re.element_type = 3 AND
	re.topogeo_id = re_tmp.topogeo_id AND
	re.element_id NOT IN (SELECT element_id FROM old_surface_data))',
    surface_topo_info.topology_name,
    surface_layer_id);  
	EXECUTE command_string;

	
	
	DROP TABLE IF EXISTS old_rows_be_reused;
	-- IF old_surface_data_not_in_new is empty we know that all areas are coverbed by the new objects
	-- and we can delete/resuse this objects for the new rows
	-- Get a list of old row id's used
	
	command_string :=  format('CREATE TEMP TABLE old_rows_be_reused AS 
	-- we can have distinct here 
	(SELECT distinct(old_data_row.id) FROM 
	%I.%I old_data_row,
	old_surface_data sf 
	WHERE (old_data_row.%I).id = sf.topogeo_id)',
    surface_topo_info.layer_schema_name,
    surface_topo_info.layer_table_name,
    surface_topo_info.layer_feature_column);  
	EXECUTE command_string;

	
	-- Take a copy of old attribute values because they will be needed when you add new rows.
	-- The new surfaces should pick up old values from the old row attributtes that overlaps the new rows
	-- We also have to take copy of the geometry we need that to find overlaps when we pick up old values
	-- TODO this should have been solved by using topology relation table, but I do that later 
	DROP TABLE IF EXISTS old_rows_attributes;
	
	command_string :=  format('CREATE TEMP TABLE old_rows_attributes AS 
	(SELECT distinct old_data_row.*, old_data_row.omrade::geometry as foo_geo FROM 
	%I.%I  old_data_row,
	old_surface_data sf 
	WHERE (old_data_row.%I).id = sf.topogeo_id)',
    surface_topo_info.layer_schema_name,
    surface_topo_info.layer_table_name,
    surface_topo_info.layer_feature_column);  
	EXECUTE command_string;

		-- Only used for debug
	IF add_debug_tables = 1 THEN
		-- list topo objects to be reused
		-- get new objects created from topo_update.create_edge_surfaces
		DROP TABLE IF EXISTS topo_rein.update_domain_surface_layer_t4;
		CREATE TABLE topo_rein.update_domain_surface_layer_t4 AS 
		( SELECT * FROM old_rows_attributes) ;
	END IF;

	
	-- Only used for debug
	IF add_debug_tables = 1 THEN
		-- list topo objects to be reused
		-- get new objects created from topo_update.create_edge_surfaces
		DROP TABLE IF EXISTS topo_rein.update_domain_surface_layer_t1;
		CREATE TABLE topo_rein.update_domain_surface_layer_t1 AS 
		( SELECT r.id, r.omrade::geometry AS geo, 'reuse topo objcts' || r.omrade::text AS topo
			FROM topo_rein.arstidsbeite_sommer_flate r, old_rows_be_reused reuse WHERE reuse.id = r.id) ;
	END IF;

	
	-- We now know which rows we can reuse clear out old data rom the realation table
	command_string :=  format('UPDATE %I.%I  r
	SET %I = clearTopoGeom(%I)
	FROM old_rows_be_reused reuse
	WHERE reuse.id = r.id',
    surface_topo_info.layer_schema_name,
    surface_topo_info.layer_table_name,
    surface_topo_info.layer_feature_column,
    surface_topo_info.layer_feature_column);  
	EXECUTE command_string;
	
	GET DIAGNOSTICS num_rows_affected = ROW_COUNT;
	RAISE NOTICE 'topo_update.update_domain_surface_layer Number rows to be reused in org table %',  num_rows_affected;

	-- If no rows are updated the user don't have update rights, we are using row level security
	-- We return no data and it will done a rollback
	IF num_rows_affected = 0 AND (SELECT count(*) FROM old_rows_be_reused)::int > 0 THEN
		RETURN;	
	END IF;
	
	SELECT (num_rows_affected - (SELECT count(*) FROM new_surface_data)) INTO num_rows_to_delete;

	RAISE NOTICE 'topo_update.update_domain_surface_layer Number rows to be added in org table  %',  count(*) FROM new_surface_data;

	RAISE NOTICE 'topo_update.update_domain_surface_layer Number rows to be deleted in org table  %',  num_rows_to_delete;

	-- When overwrite we may have more rows in the org table so we may need do delete the rows that are not needed 
	-- from  topo_rein.arstidsbeite_var_flate, we the just delete the left overs 
	command_string :=  format('DELETE FROM %I.%I
	WHERE ctid IN (
	SELECT r.ctid FROM
	%I.%I r,
	old_rows_be_reused reuse
	WHERE reuse.id = r.id 
	LIMIT  greatest(%L, 0))',
    surface_topo_info.layer_schema_name,
    surface_topo_info.layer_table_name,
    surface_topo_info.layer_schema_name,
    surface_topo_info.layer_table_name,
    num_rows_to_delete
  	);  
	EXECUTE command_string;
	
	
	-- Delete rows, also rows that could be reused, since I was not able to update those.
	-- TODO fix update of old rows instead of using delete
	DROP TABLE IF EXISTS new_rows_updated_in_org_table;
	
	command_string :=  format('CREATE TEMP TABLE new_rows_updated_in_org_table AS (SELECT * FROM %I.%I  limit 0);
	WITH updated AS (
		DELETE FROM %I.%I  old
		USING old_rows_be_reused reuse
		WHERE old.id = reuse.id
		returning *
	)
	INSERT INTO new_rows_updated_in_org_table(omrade)
	SELECT omrade FROM updated',
    surface_topo_info.layer_schema_name,
    surface_topo_info.layer_table_name,
    surface_topo_info.layer_schema_name,
    surface_topo_info.layer_table_name
  	);  
	EXECUTE command_string;
	
	GET DIAGNOSTICS num_rows_affected = ROW_COUNT;
	RAISE NOTICE 'topo_update.update_domain_surface_layer Number old rows to deleted in table %',  num_rows_affected;
	
	


	-- Only used for debug
	IF add_debug_tables = 1 THEN
		-- list new objects added reused
		-- get new objects created from topo_update.create_edge_surfaces
		DROP TABLE IF EXISTS topo_rein.update_domain_surface_layer_t2;
		CREATE TABLE topo_rein.update_domain_surface_layer_t2 AS 
		( SELECT r.id, r.omrade::geometry AS geo, 'old rows deleted update' || r.omrade::text AS topo
			FROM new_rows_updated_in_org_table r) ;
	END IF;

	
	IF (SELECT count(*) FROM old_rows_attributes)::int > 0 THEN

      -- Update status, value before insert attribttus 
	
 	    command_string := format(
 	    'UPDATE new_surface_data a
 		SET 
 		status_foo = c.status
 		FROM old_rows_attributes c
 		WHERE ST_Intersects(c.foo_geo,ST_pointOnSurface(a.surface_topo::geometry))'
 	    );
 	    EXECUTE command_string;
 		
 		GET DIAGNOSTICS num_rows_affected = ROW_COUNT;
 		--UPDATE new_surface_data a SET status_foo = 1 where status_foo <> 1;
 	
 	END IF;

	-- insert missing rows and keep a copy in them a temp table
	DROP TABLE IF EXISTS new_rows_added_in_org_table;
	
	command_string :=  format('CREATE TEMP TABLE new_rows_added_in_org_table AS (SELECT * FROM %I.%I limit 0);
	WITH inserted AS (
	INSERT INTO  %I.%I(%I,reinbeitebruker_id,felles_egenskaper,status)
	SELECT new.surface_topo, new.reinbeitebruker_id, new.felles_egenskaper as felles_egenskaper, new.status_foo as status
	FROM new_surface_data new
	WHERE NOT EXISTS ( SELECT f.id FROM %I.%I f WHERE (new.surface_topo).id = (f.%I).id )
	returning *
	)
	INSERT INTO new_rows_added_in_org_table(id,omrade)
	SELECT inserted.id, omrade FROM inserted',
    surface_topo_info.layer_schema_name,
    surface_topo_info.layer_table_name,
    surface_topo_info.layer_schema_name,
    surface_topo_info.layer_table_name,
    surface_topo_info.layer_feature_column,
    surface_topo_info.layer_schema_name,
    surface_topo_info.layer_table_name,
    surface_topo_info.layer_feature_column
  	);  
	EXECUTE command_string;
	
	
	-- Only used for debug
	IF add_debug_tables = 1 THEN
		-- list new objects added reused
		-- get new objects created from topo_update.create_edge_surfaces
		DROP TABLE IF EXISTS topo_rein.update_domain_surface_layer_t3;
		CREATE TABLE topo_rein.update_domain_surface_layer_t3 AS 
		( SELECT r.id, r.omrade::geometry AS geo, 'new topo objcts' || r.omrade::text AS topo
			FROM new_rows_added_in_org_table r) ;
	END IF;

	   	-- update the newly inserted rows with attribute values based from old_rows_table
    -- find the rows toubching
  DROP TABLE IF EXISTS touching_surface;
  

  -- If this is a not a closed polygon you have use touches
  IF  valid_closed_user_geometry IS NULL  THEN
	  CREATE TEMP TABLE touching_surface AS 
	  (SELECT a.id, topo_update.touches(surface_layer_name,a.id,surface_topo_info) as id_from 
	  FROM new_rows_added_in_org_table a);
  ELSE
  -- IF this a cloesed polygon only use objcet thats inside th e surface drawn by the user
	  CREATE TEMP TABLE touching_surface AS 
	  (
	  SELECT a.id, topo_update.touches(surface_layer_name,a.id,surface_topo_info) as id_from 
	  FROM new_rows_added_in_org_table a
	  WHERE ST_Covers(valid_closed_user_geometry,ST_PointOnSurface(a.omrade::geometry))
	  );
	  
  END IF;


	  -- Extract name of fields with not-null values:
  -- Extract name of fields with not-null values and append the table prefix n.:
  -- Only update json value that exits 
  IF (SELECT count(*) FROM old_rows_attributes)::int > 0 THEN
  
 	 	RAISE NOTICE 'topo_update.update_domain_surface_layer num rows in old attrbuttes: %', (SELECT count(*) FROM old_rows_attributes)::int;

 	 	-- Update felles_egenskaper attribttus 
	    command_string := format(
	    'UPDATE %I.%I a
		SET 
		felles_egenskaper.forstedatafangstdato = (c.felles_egenskaper).forstedatafangstdato, 
		felles_egenskaper.verifiseringsdato = (c.felles_egenskaper).verifiseringsdato, 
		felles_egenskaper.opphav = (c.felles_egenskaper).opphav 
		FROM new_rows_added_in_org_table b, 
		old_rows_attributes c
		WHERE 
	    a.id = b.id AND                           
	    ST_Intersects(c.foo_geo,ST_pointOnSurface(a.%I::geometry))',
	    surface_topo_info.layer_schema_name,
	    surface_topo_info.layer_table_name,
	    surface_topo_info.layer_feature_column
	    );
		RAISE NOTICE 'topo_update.update_domain_surface_layer command_string %', command_string;
		EXECUTE command_string;
		
		GET DIAGNOSTICS num_rows_affected = ROW_COUNT;
		RAISE NOTICE 'topo_update.update_domain_surface_layer no old attribute values found  %',  num_rows_affected;

        -- Update other attribttus 
  		SELECT
	  	array_agg(quote_ident(update_column)) AS update_fields,
	  	array_agg('c.'||quote_ident(update_column)) as update_fields_t
		  INTO
		  	update_fields,
		  	update_fields_t
		  FROM (
		   SELECT distinct(key) AS update_column
		   FROM old_rows_attributes t, json_each_text(to_json((t)))  
		   WHERE key != 'id' AND key != 'foo_geo'  AND key != 'omrade' AND key != 'felles_egenskaper'  
		  ) AS keys;
		
		  RAISE NOTICE 'topo_update.update_domain_surface_layer Extract name of not-null fields-c: %', update_fields_t;
		  RAISE NOTICE 'topo_update.update_domain_surface_layer Extract name of not-null fields-c: %', update_fields;
		
	    command_string := format(
	    'UPDATE %I.%I a
		SET 
		(%s) = (%s) 
		FROM new_rows_added_in_org_table b, 
		old_rows_attributes c
		WHERE 
	    a.id = b.id AND                           
	    ST_Intersects(c.foo_geo,ST_pointOnSurface(a.%I::geometry))',
	    surface_topo_info.layer_schema_name,
	    surface_topo_info.layer_table_name,
	    array_to_string(update_fields, ','),
	    array_to_string(update_fields_t, ','),
	    surface_topo_info.layer_feature_column
	    );
	    EXECUTE command_string;
		
		GET DIAGNOSTICS num_rows_affected = ROW_COUNT;
		RAISE NOTICE 'topo_update.update_domain_surface_layer no old attribute values found  %',  num_rows_affected;

	
	END IF;

      -- if there are any toching interfaces  
	IF (SELECT count(*) FROM touching_surface)::int > 0 THEN

	IF valid_closed_user_geometry IS NOT NULL THEN
	   SELECT
		  	array_agg(quote_ident(update_column)) AS update_fields
		  INTO
		  	update_fields
		  FROM (
		   SELECT distinct(key) AS update_column
		   FROM new_rows_added_in_org_table t, json_each_text(to_json((t)))  
		   WHERE key != 'id' AND key != 'foo_geo' AND key != 'omrade' 
		   AND key != 'felles_egenskaper' AND key != 'status' 
		   AND key != 'saksbehandler' AND key != 'slette_status_kode' AND key != 'alle_reinbeitebr_id' AND key != 'simple_geo'
		  ) AS keys;
		  RAISE NOTICE 'topo_update.update_domain_surface_layer Extract name of not-null fields-a: %', update_fields;
	ELSE
	   SELECT
		  	array_agg(quote_ident(update_column)) AS update_fields
		  INTO
		  	update_fields
		  FROM (
		   SELECT distinct(key) AS update_column
		   FROM new_rows_added_in_org_table t, json_each_text(to_json((t)))  
		   WHERE key != 'reinbeitebruker_id' AND key != 'id' AND  key != 'foo_geo' AND key != 'omrade' 
		   AND key != 'felles_egenskaper' AND key != 'status' 
		   AND key != 'saksbehandler' AND key != 'slette_status_kode' AND key != 'alle_reinbeitebr_id' AND key != 'simple_geo'
		  ) AS keys;
		  RAISE NOTICE 'topo_update.update_domain_surface_layer Extract name of not-null fields-a: %', update_fields;	
	END IF;
		
	   	-- update the newly inserted rows with attribute values based from old_rows_table
	    -- find the rows toubching
--	  	DROP TABLE IF EXISTS touching_surface;
--		CREATE TEMP TABLE touching_surface AS 
--		(SELECT topo_update.touches(surface_layer_name,a.id,surface_topo_info) as id 
--		FROM new_rows_added_in_org_table a);
	
	
		-- we set values with null row that can pick up a value from a neighbor.
		-- NB! this onlye work if new rows dont' have any defalut value
		-- TODO use a test based on new rows added and not a test on null values
		FOR temp_text_var IN SELECT unnest( update_fields ) LOOP
	        raise notice 'update colum: %', temp_text_var;
		    command_string := format('UPDATE %I.%I a
			SET 
				%s = d.%s 
			FROM 
			%I.%I d,
			touching_surface b
			WHERE 
			d.id = b.id_from AND
			a.id = b.id AND
			d.%s is not null AND
			a.%s is null',
		    surface_topo_info.layer_schema_name,
		    surface_topo_info.layer_table_name,
		    temp_text_var,
		    temp_text_var,
		    surface_topo_info.layer_schema_name,
		    surface_topo_info.layer_table_name,
		    temp_text_var,
		    temp_text_var);
--			RAISE NOTICE '? command_string %', command_string;
			EXECUTE command_string;
		
--			GET DIAGNOSTICS num_rows_affected = ROW_COUNT;
--			RAISE NOTICE 'topo_update.update_domain_surface_layer Number num_rows_affected  %',  num_rows_affected;
   		END loop;

  
	END IF;


	



	RETURN QUERY SELECT a.surface_topo::topogeometry as t FROM new_surface_data a;

	
END;
$$ LANGUAGE plpgsql;


--	RAISE NOTICE 'topo_update.update_domain_surface_layer touching_surface  %', to_json(array_agg(row_to_json(t.*))) FROM touching_surface t;
--	RAISE NOTICE 'topo_update.update_domain_surface_layer new_surface_data  %', to_json(array_agg(row_to_json(t.*))) FROM new_surface_data t;
--	RAISE NOTICE 'topo_update.update_domain_surface_layer new_rows_added_in_org_table  %', to_json(array_agg(row_to_json(t.*))) FROM new_rows_added_in_org_table t;
--	RAISE NOTICE 'topo_update.update_domain_surface_layer old_rows_attributes  %', to_json(array_agg(row_to_json(t.*))) FROM old_rows_attributes t;
--  RAISE NOTICE 'topo_update.update_domain_surface_layer old_surface_data %', to_json(array_agg(row_to_json(t.*))) FROM old_surface_data t;
--	RAISE NOTICE 'topo_update.update_domain_surface_layer valid_closed_user_geometry  %', ST_AsText(valid_closed_user_geometry);

--{"15" : "0106000020A210000001000000010300000001000000080000004D5073032713244136C9202FD79C5D41FAAF4A00A31D2441B4D7C9EA529A5D413F8E9C0F18212441959EA025449D5D4168008CDCFF3B2441AA2C686E169E5D41417493AD4E392441F9099639F6975D416D070D7C840024410DD0B708F2975D417D4058E360072441F789B7297B9C5D414D5073032713244136C9202FD79C5D41"},
--{"16" : "0106000020A210000001000000010300000001000000050000003F8E9C0F18212441959EA025449D5D414D5073032713244136C9202FD79C5D416315863C41FE23414ADE8359DBA15D410D5E3E58F8262441B4E7F75D44A25D413F8E9C0F18212441959EA025449D5D41"},
--{"17" : "0106000020A210000001000000010300000001000000040000003F8E9C0F18212441959EA025449D5D41FAAF4A00A31D2441B4D7C9EA529A5D414D5073032713244136C9202FD79C5D413F8E9C0F18212441959EA025449D5D41"}]
-- This is a simple helper function that createa a common dataholder object based on input objects
-- TODO splitt in different objects dependig we don't send unused parameters around
-- snap_tolerance float8 is optinal if not given default is 0

CREATE OR REPLACE FUNCTION topo_update.make_input_meta_info(layer_schema text, layer_table text, layer_column text,
  snap_tolerance float8 = 0)
RETURNS topo_update.input_meta_info AS $$
DECLARE


topo_info topo_update.input_meta_info ;

BEGIN
	
	
	-- Read parameters
	topo_info.layer_schema_name := layer_schema;
	topo_info.layer_table_name := layer_table;
	topo_info.layer_feature_column := layer_column;
	topo_info.snap_tolerance := snap_tolerance;

-- Find out topology name and element_type from layer identifier
  BEGIN
    SELECT t.name, l.feature_type, t.srid
    FROM topology.topology t, topology.layer l
    WHERE l.level = 0 -- need be primitive
      AND l.schema_name = topo_info.layer_schema_name
      AND l.table_name = topo_info.layer_table_name
      AND l.feature_column = topo_info.layer_feature_column
      AND t.id = l.topology_id
    INTO STRICT topo_info.topology_name,
                topo_info.element_type,
                topo_info.srid;
  EXCEPTION
    WHEN NO_DATA_FOUND THEN
      RAISE EXCEPTION 'Cannot find info for primitive layer %.%.%',
        topo_info.layer_schema_name,
        topo_info.layer_table_name,
        topo_info.layer_feature_column;
  END;

	-- find border layer id
	topo_info.border_layer_id := topo_update.get_topo_layer_id(topo_info);

    return topo_info;
END;
$$ LANGUAGE plpgsql STABLE;

-- This is a common method to parse all input data
-- It returns a struture that is adjusted reindrift that depends on sosi felles eganskaper


DROP FUNCTION IF EXISTS topo_update.handle_input_json_props(json, json, int,boolean) ;
DROP FUNCTION IF EXISTS topo_update.handle_input_json_props(json, json, int) ;

CREATE OR REPLACE FUNCTION  topo_update.handle_input_json_props(client_json_feature json,  server_json_feature json, srid_out int) 
RETURNS topo_update.json_input_structure AS $$DECLARE

DECLARE 
use_default_dates boolean = true;
BEGIN
return  topo_update.handle_input_json_props(client_json_feature,  server_json_feature, srid_out, use_default_dates); 
END;
$$ LANGUAGE plpgsql IMMUTABLE;



-- This is a common method to parse all input data
-- It returns a struture that is adjusted reindrift that depends on sosi felles eganskaper

CREATE OR REPLACE FUNCTION  topo_update.handle_input_json_props(client_json_feature json,  server_json_feature json, srid_out int, use_default_dates boolean) 
RETURNS topo_update.json_input_structure AS $$DECLARE

DECLARE 
-- holds the value for felles egenskaper from input
simple_sosi_felles_egenskaper topo_rein.simple_sosi_felles_egenskaper;

-- JSON that is sent from the cleint
client_json_properties json;

-- JSON produced on the server side
server_json_properties json;

-- Keys in the server JSON properties
server_json_keys text;
keys_to_set   TEXT[];
values_to_set json[];

-- holde the computed value for json input reday to use
json_input_structure topo_update.json_input_structure;  

BEGIN

	RAISE NOTICE 'client_json_feature %, server_json_feature % use_default_dates %',  client_json_feature, server_json_feature , use_default_dates;
	
	-- geth the geometry may be null
	json_input_structure.input_geo := topo_rein.get_geom_from_json(client_json_feature::json,srid_out);

	-- get json from the client
	client_json_properties := to_json(client_json_feature::json->'properties');
	RAISE NOTICE 'client_json_properties %',  client_json_properties ;
	
	-- get the json from the serrver, may be null
	IF server_json_feature IS NOT NULL THEN
		server_json_properties := to_json(server_json_feature::json->'properties');
	  	RAISE NOTICE 'server_json_properties  % ',  server_json_properties ;
	
		-- overwrite client JSON properties with server property values
	  	SELECT array_agg("key"),array_agg("value")  INTO keys_to_set,values_to_set
		FROM json_each(server_json_properties) WHERE "value"::text != 'null';
		client_json_properties := topo_update.json_object_set_keys(client_json_properties, keys_to_set, values_to_set);
		RAISE NOTICE 'json_properties after update  %',  client_json_properties ;
	END IF;

	json_input_structure.json_properties := client_json_properties;
	
	-- This maps from the simple format used on the client 
	-- Because the client do not support Postgres user defined types like we have used in  topo_rein.sosi_felles_egenskaper;
	-- First append the info from the client properties, only properties that maps to valid names in topo_rein.simple_sosi_felles_egenskaper will be used.
	simple_sosi_felles_egenskaper := json_populate_record(NULL::topo_rein.simple_sosi_felles_egenskaper,client_json_properties );

	RAISE NOTICE 'felles_egenskaper_sosi point/line before  %',  simple_sosi_felles_egenskaper;

	-- Here we map from simple properties to topo_rein.sosi_felles_egenskaper for line an point objects
	json_input_structure.sosi_felles_egenskaper := topo_rein.get_rein_felles_egenskaper(simple_sosi_felles_egenskaper,use_default_dates);
	
	RAISE NOTICE 'felles_egenskaper_sosi point/line after  %',  json_input_structure.sosi_felles_egenskaper;
	
	-- Here we get info for the surface objects
   	json_input_structure.sosi_felles_egenskaper_flate := topo_rein.get_rein_felles_egenskaper_flate(simple_sosi_felles_egenskaper,use_default_dates);
	

	RETURN json_input_structure;

END;
$$ LANGUAGE plpgsql IMMUTABLE;


-- Return the geom from Json and transform it to correct zone

CREATE OR REPLACE FUNCTION topo_rein.get_geom_from_json(feat json, srid_out int) 
RETURNS geometry AS $$DECLARE

DECLARE 
geom geometry;
srid int;
BEGIN

	geom := ST_GeomFromGeoJSON(feat->>'geometry');
	srid = St_Srid(geom);
	
	IF (srid_out != srid) THEN
		geom := ST_transform(geom,srid_out);
	END IF;
	
	geom := ST_SetSrid(geom,srid_out);

	RAISE NOTICE 'srid %, geom  %',   srid_out, ST_AsEWKT(geom);

	RETURN geom;

END;
$$ LANGUAGE plpgsql IMMUTABLE;


-- used to get felles egenskaper for with all values
-- including måle metode

CREATE OR REPLACE FUNCTION topo_rein.get_rein_felles_egenskaper(felles topo_rein.simple_sosi_felles_egenskaper) 
RETURNS topo_rein.sosi_felles_egenskaper AS $$DECLARE
DECLARE 
use_default_dates boolean = true;
BEGIN
return topo_rein.get_rein_felles_egenskaper(felles, use_default_dates ) ;
END;
$$ LANGUAGE plpgsql IMMUTABLE ;


CREATE OR REPLACE FUNCTION topo_rein.get_rein_felles_egenskaper(felles topo_rein.simple_sosi_felles_egenskaper, use_default_dates boolean ) 
RETURNS topo_rein.sosi_felles_egenskaper AS $$DECLARE

DECLARE 

res topo_rein.sosi_felles_egenskaper ;
res_kvalitet topo_rein.sosi_kvalitet;


BEGIN

res := topo_rein.get_rein_felles_egenskaper_flate(felles,use_default_dates);

-- add målemetode
res_kvalitet.maalemetode := (felles)."fellesegenskaper.kvalitet.maalemetode";
res_kvalitet.noyaktighet := (felles)."fellesegenskaper.kvalitet.noyaktighet";
res_kvalitet.synbarhet := (felles)."fellesegenskaper.kvalitet.synbarhet";
res.kvalitet = res_kvalitet;

return res;

END;
$$ LANGUAGE plpgsql IMMUTABLE ;


-- used to get felles egenskaper for where we don't use målemetode
CREATE OR REPLACE FUNCTION topo_rein.get_rein_felles_egenskaper_flate(felles topo_rein.simple_sosi_felles_egenskaper ) 
RETURNS topo_rein.sosi_felles_egenskaper AS $$DECLARE
use_default_dates boolean = true;
DECLARE 
BEGIN
	return topo_rein.get_rein_felles_egenskaper_flate(felles,use_default_dates);
END;
$$ LANGUAGE plpgsql IMMUTABLE ;


-- used to get felles egenskaper for where we don't use målemetode
CREATE OR REPLACE FUNCTION topo_rein.get_rein_felles_egenskaper_flate(felles topo_rein.simple_sosi_felles_egenskaper,use_default_dates boolean) 
RETURNS topo_rein.sosi_felles_egenskaper AS $$DECLARE

DECLARE 

res topo_rein.sosi_felles_egenskaper;
res_kvalitet topo_rein.sosi_kvalitet;
res_sosi_registreringsversjon topo_rein.sosi_registreringsversjon;


BEGIN

	
res.opphav :=  (felles)."fellesegenskaper.opphav";
res.oppdateringsdato :=  current_date;
	

IF use_default_dates = true THEN	
	RAISE NOTICE '------------------------------------Use default date values ';

	res.forstedatafangstdato := current_date;
	
	-- if we have a value for felles_egenskaper.forstedatafangstdato 
	IF (felles)."fellesegenskaper.forstedatafangstdato" is NOT null THEN
		res.forstedatafangstdato :=  (felles)."fellesegenskaper.forstedatafangstdato";
	END IF;
	
	-- if we have a value for felles_egenskaper.verifiseringsdato is null use forstedatafangstdato
	IF (felles)."fellesegenskaper.verifiseringsdato" is null THEN
		res.verifiseringsdato := res.forstedatafangstdato;
	ELSE
		res.verifiseringsdato := (felles)."fellesegenskaper.verifiseringsdato";
	END IF;

ELSE
	RAISE NOTICE '------------------------------------Do not default date values ';
	IF (felles)."fellesegenskaper.forstedatafangstdato" is NOT null THEN
		res.forstedatafangstdato :=  (felles)."fellesegenskaper.forstedatafangstdato";
    END IF;
    
	IF (felles)."fellesegenskaper.verifiseringsdato" is NOT null THEN
		res.verifiseringsdato := (felles)."fellesegenskaper.verifiseringsdato";
	ELSIF (felles)."fellesegenskaper.forstedatafangstdato" is NOT null THEN
		-- if verifiseringsdato is null use forstedatafangstdato if not null
		res.verifiseringsdato := (felles)."fellesegenskaper.forstedatafangstdato";
    END IF;
END IF;


return res;

END;
$$ LANGUAGE plpgsql IMMUTABLE ;



-- used to get felles egenskaper when it is a update
-- we then only update verifiseringsdato, opphav
-- res is the old record
-- felles is the new value from server

CREATE OR REPLACE FUNCTION topo_rein.get_rein_felles_egenskaper_update(
curent_value topo_rein.sosi_felles_egenskaper, 
new_value_from_client topo_rein.sosi_felles_egenskaper) 
RETURNS topo_rein.sosi_felles_egenskaper AS $$DECLARE

DECLARE 
	current_res_kvalitet topo_rein.sosi_kvalitet;
	new_res_kvalitet topo_rein.sosi_kvalitet;

BEGIN

current_res_kvalitet := (curent_value)."kvalitet";
new_res_kvalitet := (new_value_from_client)."kvalitet";
	

-- if vo value fr the client don't use it.
IF (new_value_from_client)."forstedatafangstdato" is not null THEN
	curent_value.forstedatafangstdato :=  (new_value_from_client)."forstedatafangstdato";
END IF;

-- if vo value fr the client don't use it.
IF (new_value_from_client)."verifiseringsdato" is not null THEN
	curent_value.verifiseringsdato :=  (new_value_from_client)."verifiseringsdato";
END IF;

curent_value.oppdateringsdato :=  current_date;


-- if vo value fr the client don't use it.
IF (new_value_from_client)."opphav" is not null THEN
	curent_value.opphav :=  (new_value_from_client)."opphav";
END IF;

-- if vo value fr the client don't use it.
IF (new_res_kvalitet)."maalemetode" is not null THEN
	current_res_kvalitet.maalemetode :=  (new_res_kvalitet)."maalemetode";
END IF;

-- if vo value fr the client don't use it.
IF (new_res_kvalitet)."noyaktighet" is not null THEN
	current_res_kvalitet.noyaktighet :=  (new_res_kvalitet)."noyaktighet";
END IF;

-- if vo value fr the client don't use it.
IF (new_res_kvalitet)."synbarhet" is not null THEN
	current_res_kvalitet.synbarhet :=  (new_res_kvalitet)."synbarhet";
END IF;

curent_value.kvalitet = current_res_kvalitet;

return curent_value;

END;
$$ LANGUAGE plpgsql IMMUTABLE ;







