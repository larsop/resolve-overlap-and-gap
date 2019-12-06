-- TODO find another way to pick up this from https://github.com/larsop/find-overlap-and-gap

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



DROP FUNCTION IF EXISTS find_overlap_gap_make_run_cmd(
table_to_analyze_ varchar, -- The table to analyze 
geo_collumn_name_ varchar, 	-- the name of geometry column on the table to analyze	
srid_ int, -- the srid for the given geo column on the table analyze
table_name_result_prefix_ varchar, -- This is the prefix used for the result tables
max_rows_in_each_cell_ int -- this is the max number rows that intersects with box before it's split into 4 new boxes, default is 5000
);

CREATE OR REPLACE FUNCTION find_overlap_gap_make_run_cmd(
table_to_analyze_ varchar, -- The table to analyze 
geo_collumn_name_ varchar, 	-- the name of geometry column on the table to analyze	
srid_ int, -- the srid for the given geo column on the table analyze
table_name_result_prefix_ varchar, -- This is the prefix used for the result tables
max_rows_in_each_cell_ int DEFAULT 5000 -- this is the max number rows that intersects with box before it's split into 4 new boxes, default is 5000
) RETURNS SETOF text 
AS $$DECLARE
	command_string text;
	num_rows int;

	part text;	
	id_list_tmp int[];
	this_list_id int;
	
	func_call text;
	
	-- the number of cells created in the grid
	num_cells int;

	overlapgap_overlap varchar = table_name_result_prefix_ || '_overlap'; -- The schema.table name for the overlap/intersects found in each cell 
	overlapgap_gap varchar = table_name_result_prefix_ || '_gap'; -- The schema.table name for the gaps/holes found in each cell 
	overlapgap_grid varchar  = table_name_result_prefix_ || '_grid'; -- The schema.table name of the grid that will be created and used to break data up in to managle pieces
	overlapgap_boundery varchar = table_name_result_prefix_ || '_boundery'; -- The schema.table name the outer boundery of the data found in each cell 

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
	END loop;

	-- return call for each cell
	RETURN QUERY select * FROM return_call_list;

END;
$$
LANGUAGE plpgsql;

GRANT EXECUTE on FUNCTION find_overlap_gap_make_run_cmd(table_to_analyze_ varchar, -- The table to analyze 
geo_collumn_name_ varchar, 	-- the name of geometry column on the table to analyze	
srid_ int, -- the srid for the given geo column on the table analyze
table_name_result_prefix_ varchar, -- This is the prefix used for the result tables
max_rows_in_each_cell_ int
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
LANGUAGE plpgsql;

GRANT EXECUTE on FUNCTION find_overlap_gap_single_cell(
	table_to_analyze_ varchar, -- The table to analyze 
	geo_collumn_name_ varchar, 	-- the name of geometry column on the table to analyze	
	srid_ int, -- the srid for the given geo column on the table analyze
	table_name_result_prefix_ varchar, -- This is the prefix used for the result tables
	this_list_id int, num_cells int
) TO public;
 


