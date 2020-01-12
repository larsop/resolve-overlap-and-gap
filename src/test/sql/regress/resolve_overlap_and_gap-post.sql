drop schema test_data cascade;

DROP FUNCTION IF EXISTS resolve_overlap_gap_init(
table_to_resolve_ varchar, -- The schema.table name with polygons to analyze for gaps and intersects
geo_collumn_name_ varchar, 	-- the name of geometry column on the table to analyze	
srid_ int, -- the srid for the given geo column on the table analyze
max_rows_in_each_cell int, -- this is the max number rows that intersects with box before it's split into 4 new boxes 
overlapgap_grid_ varchar, -- The schema.table name of the grid that will be created and used to break data up in to managle pieces
topology_schema_name_ varchar, -- The topology schema name where we store store sufaces and lines from the simple feature dataset
snap_tolerance_ double precision
);

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

DROP FUNCTION IF EXISTS resolve_overlap_gap_job_list(
table_to_resolve_ varchar, -- The table to resolve 
geo_collumn_name_ varchar, 	-- the name of geometry column on the table to analyze	
srid_ int, -- the srid for the given geo column on the table analyze
overlapgap_grid_ varchar, -- the name of the content based grid table
topology_name_ varchar,  -- The topology schema name where we store store sufaces and lines from the simple feature dataset. -- NB. Any exting data will related to topology_name will be deleted 
job_list_name_ varchar, -- the name of job_list table, this table is ued to track of done jobs
input_table_pk_column_name_ varchar, -- the nam eof the promary collum
simplify_tolerance_ double precision, -- the tolerance to be used when creating topolayer
snap_tolerance_ double precision, -- the tolrence to be used when add data
do_chaikins_ boolean, -- simlyfy lines by using chaikins and simlify
inside_cell_data_ boolean -- add lines inside cell, or boderlines
);

DROP FUNCTION IF EXISTS resolve_overlap_gap_single_cell(
 	table_to_analyze_ varchar, -- The table to analyze 
 	geo_collumn_name_ varchar, 	-- the name of geometry column on the table to analyze	
 	srid_ int, -- the srid for the given geo column on the table analyze
 	table_name_result_prefix_ varchar, -- This is the prefix used for the result tables
	this_list_id int, num_cells int
);


-- Drop helper function from other packages 
-- ###############################################

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

DROP PROCEDURE IF EXISTS find_overlap_gap_run(
table_to_analyze_ varchar, -- The table to analyze 
geo_collumn_name_ varchar, 	-- the name of geometry column on the table to analyze	
srid_ int, -- the srid for the given geo column on the table analyze
table_name_result_prefix_ varchar, -- This is the prefix used for the result tables
max_parallel_jobs_ int,
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

DROP FUNCTION IF EXISTS execute_parallel(stmts text[], num_parallel_thread int);

drop extension dblink cascade;

DROP FUNCTION IF EXISTS "vsr_get_data_type"(_t regclass, _c text);

-- Drop function used by view_ar5_forest_split_distinct_func.sql
drop schema topo_update cascade;

drop schema topo_rein cascade;
