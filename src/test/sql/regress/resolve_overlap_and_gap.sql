CREATE EXTENSION dblink; -- needed by  execute_parallel

-- Test 1 -------------
-- This is test that does a obverlap and gap test on overlap_gap_input_t1.sql

-- Test that input data are ok
SELECT '1 spheroid-true', count(*), ROUND(sum(st_area(geom,true))::numeric,0) from test_data.overlap_gap_input_t1;
SELECT '1 transform 3035', count(*), ROUND(sum(st_area(ST_Transform(geom,3035)))::numeric,0) from test_data.overlap_gap_input_t1;

-- Call this the overlap and gap function to find overlap and gap
-- The geometry column is named 'geom' in the test dataset and uses srid 4258, 	
-- We place the results in test_data.overlap_gap_input_t1_res*
-- || '_overlap'; -- The schema.table name for the overlap/intersects found in each cell 
-- || '_gap'; -- The schema.table name for the gaps/holes found in each cell 
-- || '_grid'; -- The schema.table name of the grid that will be created and used to break data up in to managle pieces
-- || '_boundery'; -- The schema.table name the outer boundery of the data found in each cell 
-- It will run with 10 paralell threads and max 50 polygons in each content based cell  
CALL find_overlap_gap_run('test_data.overlap_gap_input_t1','geom',4258,'test_data.overlap_gap_input_t1_res',10,50);

-- Check the result so we know that we have overlap and gap
SELECT 'check overlap table', count(*) num_overlap, ROUND(sum(st_area(ST_Transform(geom,3035)))::numeric,0) from (SELECT  (ST_dump(geom)).geom as geom, cell_id 
from test_data.overlap_gap_input_t1_res_overlap) as r where ST_Area(geom) >0;                  

SELECT 'check gap table',  count(*) num_gap, ROUND(sum(st_area(ST_Transform(geom,3035)))::numeric,0) 
from (SELECT  (ST_dump(geom)).geom as geom, cell_id from test_data.overlap_gap_input_t1_res_gap) as r;                  

SELECT 'check grid table',  count(*) num_grid, ROUND(sum(st_area(ST_Transform(geom,3035)))::numeric,0) from (SELECT  (ST_dump(geom)).geom as geom, id 
from test_data.overlap_gap_input_t1_res_grid) as r;                  

SELECT 'check boundery table',  count(*) num_boudery, ROUND(sum(st_area(ST_Transform(geom,3035)))::numeric,0) from (SELECT  (ST_dump(geom)).geom as geom, id 
from test_data.overlap_gap_input_t1_res_boundery) as r;                  

-- Call function to resolve overlap and gap in the function in test_data.overlap_gap_input_t1 which we just testet for overlap
CALL resolve_overlap_gap_run('test_data.overlap_gap_input_t1','geom',4258,'test_data.overlap_gap_input_t1_res',10,50);


