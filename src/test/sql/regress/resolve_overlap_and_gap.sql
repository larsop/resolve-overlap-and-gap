CREATE EXTENSION dblink; -- needed by  execute_parallel

-- Test 1 -------------
-- This is test that does a obverlap and gap test on overlap_gap_input_t1.sql

-- Test that input data are ok
SELECT '1 spheroid-true', count(*), ROUND(sum(st_area(geom,true))::numeric,0) from test_data.overlap_gap_input_t1;
SELECT '1 transform 3035', count(*), ROUND(sum(st_area(ST_Transform(geom,3035)))::numeric,0) from test_data.overlap_gap_input_t1;

-- Pipe output sql to a file to execute later - \o /tmp/run_cmd.sql does not work in Travis
CALL find_overlap_gap_run('test_data.overlap_gap_input_t1','geom',4258,'test_data.overlap_gap_input_t1_res',10,50);

-- Check the result
SELECT 'check overlap table', count(*) num_overlap, ROUND(sum(st_area(ST_Transform(geom,3035)))::numeric,0) from (SELECT  (ST_dump(geom)).geom as geom, cell_id 
from test_data.overlap_gap_input_t1_res_overlap) as r where ST_Area(geom) >0;                  

SELECT 'check gap table',  count(*) num_gap, ROUND(sum(st_area(ST_Transform(geom,3035)))::numeric,0) 
from (SELECT  (ST_dump(geom)).geom as geom, cell_id from test_data.overlap_gap_input_t1_res_gap) as r;                  

SELECT 'check grid table',  count(*) num_grid, ROUND(sum(st_area(ST_Transform(geom,3035)))::numeric,0) from (SELECT  (ST_dump(geom)).geom as geom, id 
from test_data.overlap_gap_input_t1_res_grid) as r;                  

SELECT 'check boundery table',  count(*) num_boudery, ROUND(sum(st_area(ST_Transform(geom,3035)))::numeric,0) from (SELECT  (ST_dump(geom)).geom as geom, id 
from test_data.overlap_gap_input_t1_res_boundery) as r;                  

