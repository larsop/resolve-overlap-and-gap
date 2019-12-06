-- Test 1 -------------
-- This is test that does a obverlap and gap test on overlap_gap_input_t1.sql

-- Test that input data are ok
SELECT '1 spheroid-true', count(*), ROUND(sum(st_area(geom,true))::numeric,0) from test_data.overlap_gap_input_t1;
SELECT '1 transform 3035', count(*), ROUND(sum(st_area(ST_Transform(geom,3035)))::numeric,0) from test_data.overlap_gap_input_t1;

-- Pipe output sql to a file to execute later - \o /tmp/run_cmd.sql does not work in Travis
SELECT find_overlap_gap_make_run_cmd('test_data.overlap_gap_input_t1','geom',4258,'test_data.overlap_gap_input_t1_res',50);

-- Run with GNU parallel (Does not work on travis. Have to install paralell)
-- \! parallel -j 4  psql nibio_reg  -c :::: /tmp/run_cmd.sql > /tmp/run_cmd_result.log

-- So we use single calls instead
SELECT find_overlap_gap_single_cell('test_data.overlap_gap_input_t1','geom',4258,'test_data.overlap_gap_input_t1_res',1,28);
SELECT find_overlap_gap_single_cell('test_data.overlap_gap_input_t1','geom',4258,'test_data.overlap_gap_input_t1_res',2,28);
SELECT find_overlap_gap_single_cell('test_data.overlap_gap_input_t1','geom',4258,'test_data.overlap_gap_input_t1_res',3,28);
SELECT find_overlap_gap_single_cell('test_data.overlap_gap_input_t1','geom',4258,'test_data.overlap_gap_input_t1_res',4,28);
SELECT find_overlap_gap_single_cell('test_data.overlap_gap_input_t1','geom',4258,'test_data.overlap_gap_input_t1_res',5,28);
SELECT find_overlap_gap_single_cell('test_data.overlap_gap_input_t1','geom',4258,'test_data.overlap_gap_input_t1_res',6,28);
SELECT find_overlap_gap_single_cell('test_data.overlap_gap_input_t1','geom',4258,'test_data.overlap_gap_input_t1_res',7,28);
SELECT find_overlap_gap_single_cell('test_data.overlap_gap_input_t1','geom',4258,'test_data.overlap_gap_input_t1_res',8,28);
SELECT find_overlap_gap_single_cell('test_data.overlap_gap_input_t1','geom',4258,'test_data.overlap_gap_input_t1_res',9,28);
SELECT find_overlap_gap_single_cell('test_data.overlap_gap_input_t1','geom',4258,'test_data.overlap_gap_input_t1_res',10,28);
SELECT find_overlap_gap_single_cell('test_data.overlap_gap_input_t1','geom',4258,'test_data.overlap_gap_input_t1_res',11,28);
SELECT find_overlap_gap_single_cell('test_data.overlap_gap_input_t1','geom',4258,'test_data.overlap_gap_input_t1_res',12,28);
SELECT find_overlap_gap_single_cell('test_data.overlap_gap_input_t1','geom',4258,'test_data.overlap_gap_input_t1_res',13,28);
SELECT find_overlap_gap_single_cell('test_data.overlap_gap_input_t1','geom',4258,'test_data.overlap_gap_input_t1_res',14,28);
SELECT find_overlap_gap_single_cell('test_data.overlap_gap_input_t1','geom',4258,'test_data.overlap_gap_input_t1_res',15,28);
SELECT find_overlap_gap_single_cell('test_data.overlap_gap_input_t1','geom',4258,'test_data.overlap_gap_input_t1_res',16,28);
SELECT find_overlap_gap_single_cell('test_data.overlap_gap_input_t1','geom',4258,'test_data.overlap_gap_input_t1_res',17,28);
SELECT find_overlap_gap_single_cell('test_data.overlap_gap_input_t1','geom',4258,'test_data.overlap_gap_input_t1_res',18,28);
SELECT find_overlap_gap_single_cell('test_data.overlap_gap_input_t1','geom',4258,'test_data.overlap_gap_input_t1_res',19,28);
SELECT find_overlap_gap_single_cell('test_data.overlap_gap_input_t1','geom',4258,'test_data.overlap_gap_input_t1_res',20,28);
SELECT find_overlap_gap_single_cell('test_data.overlap_gap_input_t1','geom',4258,'test_data.overlap_gap_input_t1_res',21,28);
SELECT find_overlap_gap_single_cell('test_data.overlap_gap_input_t1','geom',4258,'test_data.overlap_gap_input_t1_res',22,28);
SELECT find_overlap_gap_single_cell('test_data.overlap_gap_input_t1','geom',4258,'test_data.overlap_gap_input_t1_res',23,28);
SELECT find_overlap_gap_single_cell('test_data.overlap_gap_input_t1','geom',4258,'test_data.overlap_gap_input_t1_res',24,28);
SELECT find_overlap_gap_single_cell('test_data.overlap_gap_input_t1','geom',4258,'test_data.overlap_gap_input_t1_res',25,28);
SELECT find_overlap_gap_single_cell('test_data.overlap_gap_input_t1','geom',4258,'test_data.overlap_gap_input_t1_res',26,28);
SELECT find_overlap_gap_single_cell('test_data.overlap_gap_input_t1','geom',4258,'test_data.overlap_gap_input_t1_res',27,28);
SELECT find_overlap_gap_single_cell('test_data.overlap_gap_input_t1','geom',4258,'test_data.overlap_gap_input_t1_res',28,28);

-- Check the result
SELECT 'check overlap table', count(*) num_overlap, ROUND(sum(st_area(ST_Transform(geom,3035)))::numeric,0) from (SELECT  (ST_dump(geom)).geom as geom, cell_id 
from test_data.overlap_gap_input_t1_res_overlap) as r where ST_Area(geom) >0;                  

SELECT 'check gap table',  count(*) num_gap, ROUND(sum(st_area(ST_Transform(geom,3035)))::numeric,0) 
from (SELECT  (ST_dump(geom)).geom as geom, cell_id from test_data.overlap_gap_input_t1_res_gap) as r;                  

SELECT 'check grid table',  count(*) num_grid, ROUND(sum(st_area(ST_Transform(geom,3035)))::numeric,0) from (SELECT  (ST_dump(geom)).geom as geom, id 
from test_data.overlap_gap_input_t1_res_grid) as r;                  

SELECT 'check boundery table',  count(*) num_boudery, ROUND(sum(st_area(ST_Transform(geom,3035)))::numeric,0) from (SELECT  (ST_dump(geom)).geom as geom, id 
from test_data.overlap_gap_input_t1_res_boundery) as r;                  

