CREATE EXTENSION dblink; -- needed by  execute_parallel

-- Create data test case degrees
CREATE table test_data.overlap_gap_input_t2 AS (SELECT * from test_data.overlap_gap_input_t1 WHERE c1 in (633,1233,1231,834));

-- Create data test case meter and new column names
CREATE table test_data.overlap_gap_input_t3 AS (SELECT distinct c1 as c1t3, c2 as c2t3, c3, ST_transform(geom,25833)::Geometry(Polygon,25833) as geo from test_data.overlap_gap_input_t1 WHERE c1 in (633,1233,1231,834));

-- Call function to resolve overlap and gap in the function in test_data.overlap_gap_input_t1 which we just testet for overlap
CALL resolve_overlap_gap_run(
('test_data.overlap_gap_input_t2','c1','geom',4258,false), -- TYPE resolve_overlap_data_input
('test_topo_t2',0.00001), -- TYPE resolve_overlap_data_topology
resolve_overlap_data_clean_type_func(  -- TYPE resolve_overlap_data_clean
49,  -- if this a polygon  is below this limit it will merge into a neighbour polygon. The area is sqare meter.
0, -- is this is more than zero simply will called with
null, -- _max_average_vertex_length, in meter both for utm and deegrees, this used to avoid running ST_simplifyPreserveTopology for long lines lines with few points
0, -- IF 0 NO CHAKINS WILL BE DONE A big value here make no sense because the number of points will increaes exponential )
10000, --edge that are longer than this value will not be touched by _chaikins_min_degrees and _chaikins_max_degrees  
120, -- The angle has to be less this given value, This is used to avoid to touch all angles. 
240, -- OR the angle has to be greather than this given value, This is used to avoid to touch all angles 
40, -- The angle has to be less this given value, This is used to avoid to touch all angles. 
320 -- OR The angle has to be greather than this given value, This is used to avoid to touch all angles 
)
,5,4);

SELECT 'degrees_check_failed_lines', count(geo) from test_topo_t2.overlap_gap_input_t2_no_cut_line_failed;

SELECT 'degrees_check_border_lines', count(geo) from test_topo_t2.overlap_gap_input_t2_border_line_segments;

--SELECT 'degrees_check_added_lines', count(geom) from test_topo_t2.edge;

SELECT 'degrees_check_added_faces', count(mbr) from test_topo_t2.face;

SELECT 'degrees_check_added_simple_feature_geom', count(*) from test_topo_t2.overlap_gap_input_t2_result where geom is not null;

SELECT 'degrees_check_added_simple_feature_c1', count(*) from test_topo_t2.overlap_gap_input_t2_result where c1 is not null;

SELECT 'degrees_check_added_simple_feature_c2', count(*) from test_topo_t2.overlap_gap_input_t2_result where c2 is not null;

SELECT 'degrees_check_added_simple_feature_c3', count(*) from test_topo_t2.overlap_gap_input_t2_result where c3 is not null;

-- Records from this function would mean an invalid topology was created
SELECT 'validation', * FROM topology.ValidateTopology('test_topo_t2');

SELECT 'degrees', topology.droptopology('test_topo_t2');


-- Call function to resolve overlap and gap in the function in test_data.overlap_gap_input_t1 which we just testet for overlap
CALL resolve_overlap_gap_run(
('test_data.overlap_gap_input_t3','c1t3','geo',25833,true), -- TYPE resolve_overlap_data_input
('test_topo_t3',1.0), -- TYPE resolve_overlap_data_topology
resolve_overlap_data_clean_type_func(  -- TYPE resolve_overlap_data_clean
49,  -- if this a polygon  is below this limit it will merge into a neighbour polygon. The area is sqare meter.
30, -- is this is more than zero simply will called with
10000, -- _max_average_vertex_length, in meter both for utm and deegrees, this used to avoid running ST_simplifyPreserveTopology for long lines lines with few points
1, -- IF 0 NO CHAKINS WILL BE DONE A big value here make no sense because the number of points will increaes exponential )
10000, --edge that are longer than this value will not be touched by _chaikins_min_degrees and _chaikins_max_degrees  
120, -- The angle has to be less this given value, This is used to avoid to touch all angles. 
240, -- OR the angle has to be greather than this given value, This is used to avoid to touch all angles 
40, -- The angle has to be less this given value, This is used to avoid to touch all angles. 
320 -- OR The angle has to be greather than this given value, This is used to avoid to touch all angles 
),
5,4);

SELECT 'utm_check_failed_lines', count(geo) from test_topo_t3.overlap_gap_input_t3_no_cut_line_failed;

SELECT 'utm_check_border_lines', count(geo) from test_topo_t3.overlap_gap_input_t3_border_line_segments;

--SELECT 'utm_check_added_lines', count(geom) from test_topo_t3.edge;

SELECT 'utm_check_added_faces', count(mbr) from test_topo_t3.face;

SELECT 'utm_check_added_simple_feature_polygons', count(*) from test_topo_t3.overlap_gap_input_t3_result;

SELECT 'utm', topology.droptopology('test_topo_t3');


