drop schema test_data cascade;

DROP PROCEDURE IF EXISTS resolve_overlap_gap_run(
_table_to_resolve varchar, -- The table to resolv, imcluding schema name
_table_pk_column_name varchar, -- The primary of the input table
_table_geo_collumn_name varchar, -- the name of geometry column on the table to analyze
_table_srid int, -- the srid for the given geo column on the table analyze
_utm boolean,
_topology_name varchar, -- The topology schema name where we store store sufaces and lines from the simple feature dataset and th efinal result
-- NB. Any exting data will related to topology_name will be deleted
_tolerance  double precision, -- this is tolerance used as base when creating the the top layer
_max_parallel_jobs int, -- this is the max number of paralell jobs to run. There must be at least the same number of free connections
_max_rows_in_each_cell int -- this is the max number rows that intersects with box before it's split into 4 new boxes, default is 5000
);

DROP FUNCTION IF EXISTS resolve_overlap_gap_init(
_table_name_result_prefix varchar,
_table_to_resolve varchar, -- The schema.table name with polygons to analyze for gaps and intersects
_geo_collumn_name varchar, -- the name of geometry column on the table to analyze
_srid int, -- the srid for the given geo column on the table analyze
_max_rows_in_each_cell int, -- this is the max number rows that intersects with box before it's split into 4 new boxes
_overlapgap_grid varchar, -- The schema.table name of the grid that will be created and used to break data up in to managle pieces
_topology_schema_name varchar, -- The topology schema name where we store store result sufaces and lines from the simple feature dataset,
_snap_tolerance double precision
);


DROP FUNCTION IF EXISTS resolve_overlap_gap_job_list(
table_to_resolve_ varchar, -- The table to resolve
geo_collumn_name_ varchar, -- the name of geometry column on the table to analyze
_srid int, -- the srid for the given geo column on the table analyze
_utm boolean, 
overlapgap_grid_ varchar, -- the name of the content based grid table
_topology_schema_name varchar, -- The topology schema name where we store store result sufaces and lines from the simple feature dataset,  
topology_name_ varchar, -- The topology schema name where we store store sufaces and lines from the simple feature dataset. -- NB. Any exting data will related to topology_name will be deleted
job_list_name_ varchar, -- the name of job_list table, this table is ued to track of done jobs
input_table_pk_column_name_ varchar, -- the nam eof the promary collum
simplify_tolerance_ double precision, -- the tolerance to be used when creating topolayer
snap_tolerance_ double precision, -- the tolrence to be used when add data
do_chaikins_ boolean, -- simlyfy lines by using chaikins and simlify
_min_area_to_keep float, -- surfaces with area less than this will merge with a neightbor
_cell_job_type int
);

DROP PROCEDURE IF EXISTS resolve_overlap_gap_job_list (
table_to_resolve_ varchar, -- The table to resolve
geo_collumn_name_ varchar, -- the name of geometry column on the table to analyze
_srid int, -- the srid for the given geo column on the table analyze
_utm boolean, 
overlapgap_grid_ varchar, -- the name of the content based grid table
_topology_schema_name varchar, -- The topology schema name where we store store result sufaces and lines from the simple feature dataset,  
topology_name_ varchar, -- The topology schema name where we store store sufaces and lines from the simple feature dataset. -- NB. Any exting data will related to topology_name will be deleted
job_list_name_ varchar, -- the name of job_list table, this table is ued to track of done jobs
input_table_pk_column_name_ varchar, -- the nam eof the promary collum
simplify_tolerance_ double precision, -- the tolerance to be used when creating topolayer
snap_tolerance_ double precision, -- the tolrence to be used when add data
do_chaikins_ boolean, -- simlyfy lines by using chaikins and simlify
_min_area_to_keep float, -- surfaces with area less than this will merge with a neightbor
cell_job_type_ int -- add lines inside cell, or boderlines
);

DROP FUNCTION IF EXISTS resolve_overlap_gap_block_cell(
input_table_name varchar, 
input_table_geo_column_name varchar, 
input_table_pk_column_name varchar, 
_job_list_name varchar, 
bb geometry
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
