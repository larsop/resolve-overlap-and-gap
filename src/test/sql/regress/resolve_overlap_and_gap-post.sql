drop schema test_data cascade;

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

DROP FUNCTION IF EXISTS find_overlap_gap_make_run_cmd(
table_to_analyze_ varchar, -- The table to analyze 
geo_collumn_name_ varchar, 	-- the name of geometry column on the table to analyze	
srid_ int, -- the srid for the given geo column on the table analyze
table_name_result_prefix_ varchar, -- This is the prefix used for the result tables
max_rows_in_each_cell_ int -- this is the max number rows that intersects with box before it's split into 4 new boxes, default is 5000
);

DROP FUNCTION IF EXISTS find_overlap_gap_single_cell(
	table_to_analyze_ varchar, -- The table to analyze 
	geo_collumn_name_ varchar, 	-- the name of geometry column on the table to analyze	
	srid_ int, -- the srid for the given geo column on the table analyze
	table_name_result_prefix_ varchar, -- This is the prefix used for the result tables
	this_list_id int, num_cells int);


DROP FUNCTION cbg_get_table_extent (schema_table_name_column_name_array VARCHAR[]);

DROP FUNCTION cbg_content_based_balanced_grid (	
													table_name_column_name_array VARCHAR[], 
													grid_geom_in geometry,
													min_distance integer,
													max_rows integer);	
DROP FUNCTION cbg_content_based_balanced_grid(
													table_name_column_name_array VARCHAR[],
													max_rows integer); 
													
DROP TYPE IF EXISTS find_overlap_gap_single_cell_pameter cascade;
