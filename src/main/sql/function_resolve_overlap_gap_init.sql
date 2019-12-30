
-- this is internal helper function
-- this is a function that creates unlogged tables and the the grid neeed when later checking this table for overlap and gaps. 
 
DROP FUNCTION IF EXISTS resolve_overlap_gap_init(
table_to_resolve_ varchar, -- The schema.table name with polygons to analyze for gaps and intersects
geo_collumn_name_ varchar, 	-- the name of geometry column on the table to analyze	
srid_ int, -- the srid for the given geo column on the table analyze
max_rows_in_each_cell int, -- this is the max number rows that intersects with box before it's split into 4 new boxes 
overlapgap_grid_ varchar, -- The schema.table name of the grid that will be created and used to break data up in to managle pieces
topology_schema_name_ varchar -- The topology schema name where we store store sufaces and lines from the simple feature dataset
);

CREATE OR REPLACE FUNCTION resolve_overlap_gap_init(
table_to_resolve_ varchar, -- The schema.table name with polygons to analyze for gaps and intersects
geo_collumn_name_ varchar, 	-- the name of geometry column on the table to analyze	
srid_ int, -- the srid for the given geo column on the table analyze
max_rows_in_each_cell int, -- this is the max number rows that intersects with box before it's split into 4 new boxes 
overlapgap_grid_ varchar, -- The schema.table name of the grid that will be created and used to break data up in to managle pieces
topology_schema_name_ varchar -- The topology schema name where we store store sufaces and lines from the simple feature dataset
)
    RETURNS INTEGER
AS $$DECLARE

	-- used to run commands
	command_string text;
	
	-- the number of cells created in the grid
	num_cells int;
	
	-- drop result tables
	drop_result_tables_ boolean = true;
	
	
BEGIN

	-- ############################# Handle Topology 
	-- drop schema if exists
	IF (drop_result_tables_ = true AND (SELECT count(*) from topology.topology WHERE name = quote_literal(topology_schema_name_)) = 1 ) THEN
		EXECUTE FORMAT('SELECT topology.droptopology(%s)',quote_literal(topology_schema_name_));
	END IF;
	
	-- drop this schema in case it exists
	EXECUTE FORMAT('DROP SCHEMA IF EXISTS %s CASCADE',topology_schema_name_);

	-- create topology 
	EXECUTE FORMAT('SELECT topology.createtopology(%s)',quote_literal(topology_schema_name_));
	
	
	
	-- ############################# Handle content based grid init
	-- drop content based grid table if exits 
	IF (drop_result_tables_ = true) THEN
		EXECUTE FORMAT('DROP TABLE IF EXISTS %s',overlapgap_grid_);
	END IF;

	-- create a content based grid table for input data
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
	quote_literal(table_to_resolve_ || ' ' || geo_collumn_name_)::text,
	max_rows_in_each_cell
	);
	-- display
	RAISE NOTICE 'command_string %.', command_string;
	-- execute the sql command
	EXECUTE command_string;

	-- count number of cells in grid
	command_string := FORMAT('SELECT count(*) from %s',overlapgap_grid_);
	-- display
	RAISE NOTICE 'command_string % .', command_string;
	-- execute the sql command
	EXECUTE command_string  INTO num_cells;


	return num_cells;

END;
$$
LANGUAGE plpgsql;

GRANT EXECUTE on FUNCTION  resolve_overlap_gap_init(
table_to_resolve_ varchar, -- The schema.table name with polygons to analyze for gaps and intersects
geo_collumn_name_ varchar, 	-- the name of geometry column on the table to analyze	
srid_ int, -- the srid for the given geo column on the table analyze
max_rows_in_each_cell int, -- this is the max number rows that intersects with box before it's split into 4 new boxes 
overlapgap_grid_ varchar, -- The schema.table name of the grid that will be created and used to break data up in to managle pieces
topology_schema_name_ varchar -- The topology schema name where we store store sufaces and lines from the simple feature dataset
) TO public;

