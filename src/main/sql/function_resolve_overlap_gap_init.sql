
-- this is internal helper function
-- this is a function that creates unlogged tables and the the grid neeed when later checking this table for overlap and gaps. 
 
DROP FUNCTION IF EXISTS resolve_overlap_gap_init(
table_to_analyze_ varchar, -- The schema.table name with polygons to analyze for gaps and intersects
geo_collumn_name_ varchar, 	-- the name of geometry column on the table to analyze	
srid_ int, -- the srid for the given geo column on the table analyze
max_rows_in_each_cell int, -- this is the max number rows that intersects with box before it's split into 4 new boxes 
overlapgap_overlap_ varchar, -- The schema.table name for the overlap/intersects found in each cell 
overlapgap_gap_ varchar, -- The schema.table name for the gaps/holes found in each cell 
overlapgap_grid_ varchar, -- The schema.table name of the grid that will be created and used to break data up in to managle pieces
overlapgap_boundery_ varchar -- The schema.table name the outer boundery of the data found in each cell 
);

CREATE OR REPLACE FUNCTION resolve_overlap_gap_init(
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

	return num_cells;

END;
$$
LANGUAGE plpgsql;

GRANT EXECUTE on FUNCTION  resolve_overlap_gap_init(
table_to_analyze_ varchar, -- The schema.table name with polygons to analyze for gaps and intersects
geo_collumn_name_ varchar, 	-- the name of geometry column on the table to analyze	
srid_ int, -- the srid for the given geo column on the table analyze
max_rows_in_each_cell int, -- this is the max number rows that intersects with box before it's split into 4 new boxes 
overlapgap_overlap_ varchar, -- The schema.table name for the overlap/intersects found in each cell 
overlapgap_gap_ varchar, -- The schema.table name for the gaps/holes found in each cell 
overlapgap_grid_ varchar, -- The schema.table name of the grid that will be created and used to break data up in to managle pieces
overlapgap_boundery_ varchar -- The schema.table name the outer boundery of the data found in each cell 
) TO public;

