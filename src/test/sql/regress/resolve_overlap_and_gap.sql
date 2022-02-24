-- To avoid error since binary geo seems to vary from local systen and Travis 
SET client_min_messages TO ERROR;


CREATE EXTENSION dblink; -- needed by  execute_parallel

-- Create data test case degrees
CREATE table test_data.overlap_gap_input_t2 AS (SELECT * from test_data.overlap_gap_input_t1 WHERE c1 in (633,1233,1231,834));

-- Create data test case meter and new column names
CREATE table test_data.overlap_gap_input_t3 AS (SELECT distinct c1 as c1t3, c2 as c2t3, c3, ST_transform(geom,25833)::Geometry(Polygon,25833) as geo from test_data.overlap_gap_input_t1 WHERE c1 in (633,1233,1231,834));

-- Call function to resolve overlap and gap in the function in test_data.overlap_gap_input_t1 which we just testet for overlap
CALL resolve_overlap_gap_run(
(null,null,null, -- The simple line feature info with attributtes 
'test_data.overlap_gap_input_t2','c1','geom' -- The simple polygons feature info with attributtes
,4258,false, -- info about srid and utm or not
null,null,null,null
), -- TYPE resolve_overlap_data_input
('test_topo_t2',0.00001,false,null,null), -- TYPE resolve_overlap_data_topology
  resolve_overlap_data_clean_type_func(  -- TYPE resolve_overlap_data_clean
  49,  -- if this a polygon  is below this limit it will merge into a neighbour polygon. The area is sqare meter.
  resolve_based_on_attribute_type_func(), -- resolve_based_on_attribute_type for attributes that have equal values
  0, -- is this is more than zero simply will called with
  null, -- _max_average_vertex_length, in meter both for utm and deegrees, this used to avoid running ST_simplifyPreserveTopology for long lines lines with few points
  0, -- IF 0 NO CHAKINS WILL BE DONE A big value here make no sense because the number of points will increaes exponential )
  10000, --edge that are longer than this value will not be touched by _chaikins_min_degrees and _chaikins_max_degrees  
  120, -- The angle has to be less this given value, This is used to avoid to touch all angles. 
  240, -- OR the angle has to be greather than this given value, This is used to avoid to touch all angles 
  40, -- The angle has to be less this given value, This is used to avoid to touch all angles. 
  320 -- OR The angle has to be greather than this given value, This is used to avoid to touch all angles 
)
,5,4,
  resolve_overlap_data_debug_options_func(
  false --if set to false, it will do topology.ValidateTopology and stop to if the this call returns any rows
  ) 
);

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
(null,null,null, -- The simple line feature info with attributtes 
'test_data.overlap_gap_input_t3','c1t3','geo' -- The simple polygons feature info with attributtes
,25833,true, -- info about srid and utm or not
null,null,null,null
), -- TYPE resolve_overlap_data_input
('test_topo_t3',1.0,false,null,null), -- TYPE resolve_overlap_data_topology
resolve_overlap_data_clean_type_func(  -- TYPE resolve_overlap_data_clean
  49,  -- if this a polygon  is below this limit it will merge into a neighbour polygon. The area is sqare meter.
  resolve_based_on_attribute_type_func(), -- resolve_based_on_attribute_type for attributes that have equal values
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

-- Records from this function would mean an invalid topology was created
SELECT 'validation', * FROM topology.ValidateTopology('test_topo_t3');

SELECT 'utm', topology.droptopology('test_topo_t3');


-- Call function to resolve overlap but stop a job_type 3 and loop number 1
CALL resolve_overlap_gap_run(
(null,null,null, -- The simple line feature info with attributtes 
'test_data.overlap_gap_input_t3','c1t3','geo' -- The simple polygons feature info with attributtes
,25833,true, -- info about srid and utm or not
null,null,null,null
), -- TYPE resolve_overlap_data_input
('test_topo_t3',1.0,false,null,null), -- TYPE resolve_overlap_data_topology
  resolve_overlap_data_clean_type_func(  -- TYPE resolve_overlap_data_clean
  49,  -- if this a polygon  is below this limit it will merge into a neighbour polygon. The area is sqare meter.
  resolve_based_on_attribute_type_func(), -- resolve_based_on_attribute_type for attributes that have equal values
  0, -- is this is more than zero simply will called with
  null, -- _max_average_vertex_length, in meter both for utm and deegrees, this used to avoid running ST_simplifyPreserveTopology for long lines lines with few points
  0, -- IF 0 NO CHAKINS WILL BE DONE A big value here make no sense because the number of points will increaes exponential )
  10000, --edge that are longer than this value will not be touched by _chaikins_min_degrees and _chaikins_max_degrees  
  120, -- The angle has to be less this given value, This is used to avoid to touch all angles. 
  240, -- OR the angle has to be greather than this given value, This is used to avoid to touch all angles 
  40, -- The angle has to be less this given value, This is used to avoid to touch all angles. 
  320 -- OR The angle has to be greather than this given value, This is used to avoid to touch all angles 
)
,5,4,
  resolve_overlap_data_debug_options_func(
  false, --if set to false, it will do topology.ValidateTopology and stop to if the this call returns any rows
  false, -- if set to true, it will do topology.ValidateTopology at each loop return if it's error 
  false, --  if set to false, it will in many cases generate topo errors beacuse of running in many parralell threads
  1, -- if set to more than 1 it will skip init procces and start at given job_type
  1, -- many of jobs are ran in loops beacuse because if get an exception or cell is not allowed handle because cell close to is also started to work , this cell will gandled in the next loop.
  1, -- if set to more than 0 the job will stop  when this job type is reday to run and display a set sql to run
  2 -- if set to more than 0 the job will stop  when this job type is reday to run and display a set sql to run
  ) 
);

SELECT 'utm_check_failed_lines', count(geo) from test_topo_t3.overlap_gap_input_t3_no_cut_line_failed;

SELECT 'utm_check_border_lines', count(geo) from test_topo_t3.overlap_gap_input_t3_border_line_segments;

--SELECT 'utm_check_added_lines', count(geom) from test_topo_t3.edge;

SELECT 'utm_check_added_faces', count(mbr) from test_topo_t3.face;

SELECT 'utm_check_added_simple_feature_polygons', count(*) from test_topo_t3.overlap_gap_input_t3_result;


-- Call function to resolve overlap but stop a job_type 3 and loop number 2
CALL resolve_overlap_gap_run(
(null,null,null, -- The simple line feature info with attributtes 
'test_data.overlap_gap_input_t3','c1t3','geo' -- The simple polygons feature info with attributtes
,25833,true, -- info about srid and utm or not
null,null,null,null
), -- TYPE resolve_overlap_data_input
('test_topo_t3',1.0,false,null,null), -- TYPE resolve_overlap_data_topology
  resolve_overlap_data_clean_type_func(  -- TYPE resolve_overlap_data_clean
  49,  -- if this a polygon  is below this limit it will merge into a neighbour polygon. The area is sqare meter.
  resolve_based_on_attribute_type_func(), -- resolve_based_on_attribute_type for attributes that have equal values
  0, -- is this is more than zero simply will called with
  null, -- _max_average_vertex_length, in meter both for utm and deegrees, this used to avoid running ST_simplifyPreserveTopology for long lines lines with few points
  0, -- IF 0 NO CHAKINS WILL BE DONE A big value here make no sense because the number of points will increaes exponential )
  10000, --edge that are longer than this value will not be touched by _chaikins_min_degrees and _chaikins_max_degrees  
  120, -- The angle has to be less this given value, This is used to avoid to touch all angles. 
  240, -- OR the angle has to be greather than this given value, This is used to avoid to touch all angles 
  40, -- The angle has to be less this given value, This is used to avoid to touch all angles. 
  320 -- OR The angle has to be greather than this given value, This is used to avoid to touch all angles 
)
,5,4,
  resolve_overlap_data_debug_options_func(
  false, --if set to false, it will do topology.ValidateTopology and stop to if the this call returns any rows
  false, -- if set to true, it will do topology.ValidateTopology at each loop return if it's error 
  false, --  if set to false, it will in many cases generate topo errors beacuse of running in many parralell threads
  2, -- if set to more than 1 it will skip init procces and start at given job_type
  1, -- many of jobs are ran in loops beacuse because if get an exception or cell is not allowed handle because cell close to is also started to work , this cell will gandled in the next loop.
  0, -- if set to more than 0 the job will stop  when this job type is reday to run and display a set sql to run
  0 -- if set to more than 0 the job will stop  when this job type is reday to run and display a set sql to run
  ) 
);

SELECT 'utm_check_valid', c1t3, _input_geo_is_valid from test_topo_t3.overlap_gap_input_t3_result order by c1t3;

SELECT 'utm_check_failed_lines', count(geo) from test_topo_t3.overlap_gap_input_t3_no_cut_line_failed;

SELECT 'utm_check_border_lines', count(geo) from test_topo_t3.overlap_gap_input_t3_border_line_segments;

--SELECT 'utm_check_added_lines', count(geom) from test_topo_t3.edge;

SELECT 'utm_check_added_faces', count(mbr) from test_topo_t3.face;

SELECT 'utm_check_added_simple_feature_polygons', count(*) from test_topo_t3.overlap_gap_input_t3_result;


-- Records from this function would mean an invalid topology was created
SELECT 'validation', * FROM topology.ValidateTopology('test_topo_t3');

SELECT 'utm', topology.droptopology('test_topo_t3');

-- Create test data for ar
CREATE table test_ar5_web.flate_t1 AS SELECT f.* from test_ar5_web.flate f 
WHERE ST_Intersects('0103000020A21000000100000005000000BAF34A7E4DD6174082EF7C7F078A4D40BAF34A7E4DD617406217819A2D8A4D40B443FDC569D717406217819A2D8A4D40B443FDC569D7174082EF7C7F078A4D40BAF34A7E4DD6174082EF7C7F078A4D40',
f.geo);

create unique index on test_ar5_web.flate_t1(qms_id_flate);

-- Create data test case meter and new column names
CREATE table test_ar5_web.grense_t1 AS SELECT distinct g.* from test_ar5_web.flate f,test_ar5_web.grense g 
WHERE ST_Intersects('0103000020A21000000100000005000000BAF34A7E4DD6174082EF7C7F078A4D40BAF34A7E4DD617406217819A2D8A4D40B443FDC569D717406217819A2D8A4D40B443FDC569D7174082EF7C7F078A4D40BAF34A7E4DD6174082EF7C7F078A4D40',
f.geo) AND ST_Intersects(g.geo,f.geo);

create unique index on test_ar5_web.grense_t1(qms_id_grense);

CALL resolve_overlap_gap_run(
('test_ar5_web.grense_t1','qms_id_grense','geo', -- The simple line feature info with attributtes 
'test_ar5_web.flate_t1','qms_id_flate','geo' -- The simple polygons feature info with attributtes
,4258,false, -- info about srid and utm or not
null,null,null,null
), -- TYPE resolve_overlap_data_input
('topo_ar5_sysdata_webclient_t1',0.00001,true,null,null), -- TYPE resolve_overlap_data_topology
resolve_overlap_data_clean_type_func(), -- No parameters line simplifcations will be done
5,4);


SELECT 'ar5_check_added_simple_feature_polygons', count(*) from topo_ar5_sysdata_webclient_t1.flate_t1_result;
SELECT 'ar5_artype_simple_feature_polygons', qms_id_flate, artype,areal, round(ST_area(geo,true)) from topo_ar5_sysdata_webclient_t1.flate_t1_result order by qms_id_flate, round(ST_area(geo,true));
SELECT 'ar5_num_faces', count(mbr) from topo_ar5_sysdata_webclient_t1.face;
SELECT 'ar5_num_edge_data', count(*) from topo_ar5_sysdata_webclient_t1.edge_data;
SELECT 'ar5_num_relation', count(*) from topo_ar5_sysdata_webclient_t1.relation;
SELECT 'ar5_check_added_topo_line_ref', count(*) from topo_ar5_sysdata_webclient_t1.edge_attributes;
SELECT 'ar5_artype_edge_attributes',  qms_id_grense, opphav, round(ST_length(geo::Geometry,true)) from topo_ar5_sysdata_webclient_t1.edge_attributes order by qms_id_grense;
SELECT 'ar5_check_added_topo_face_ref', count(*) from topo_ar5_sysdata_webclient_t1.face_attributes;
SELECT 'ar5_artype_face_attributes',  qms_id_flate, opphav, round(ST_Area(geo::Geometry,true)) from topo_ar5_sysdata_webclient_t1.face_attributes order by qms_id_flate;
\d topo_ar5_sysdata_webclient_t1.face_attributes


-- Call function to resolve overlap and gap in the function in test_data.overlap_gap_
CALL resolve_overlap_gap_run(
(null,null,null, -- The simple line feature info with attributtes 
'test_ar5_web.flate_t1','qms_id_flate','geo' -- The simple polygons feature info with attributtes
,4258,false, -- info about srid and utm or not
null,null,null,null
), -- TYPE resolve_overlap_data_input
('test_topo_ar5_test2',0.00001,false,null,null), -- TYPE resolve_overlap_data_topology
  resolve_overlap_data_clean_type_func(  -- TYPE resolve_overlap_data_clean
  49,  -- if this a polygon  is below this limit it will merge into a neighbour polygon. The area is sqare meter.
  resolve_based_on_attribute_type_func('artype arskogbon',30,10), -- resolve_based_on_attribute_type for attributes that have equal values
  0, -- is this is more than zero simply will called with
  null, -- _max_average_vertex_length, in meter both for utm and deegrees, this used to avoid running ST_simplifyPreserveTopology for long lines lines with few points
  0, -- IF 0 NO CHAKINS WILL BE DONE A big value here make no sense because the number of points will increaes exponential )
  10000, --edge that are longer than this value will not be touched by _chaikins_min_degrees and _chaikins_max_degrees  
  120, -- The angle has to be less this given value, This is used to avoid to touch all angles. 
  240, -- OR the angle has to be greather than this given value, This is used to avoid to touch all angles 
  40, -- The angle has to be less this given value, This is used to avoid to touch all angles. 
  320 -- OR The angle has to be greather than this given value, This is used to avoid to touch all angles 
)
,5,4,
  resolve_overlap_data_debug_options_func(
  false --if set to false, it will do topology.ValidateTopology and stop to if the this call returns any rows
  ) 
);

SELECT 'degrees_check_failed_lines', count(geo) from test_topo_ar5_test2.flate_t1_no_cut_line_failed;

SELECT 'degrees_check_border_lines', count(geo) from test_topo_ar5_test2.flate_t1_border_line_segments;

--SELECT 'degrees_check_added_lines', count(geom) from test_topo_ar5_test2.edge;

SELECT 'degrees_check_added_faces', count(mbr) from test_topo_ar5_test2.face;

SELECT 'degrees_check_added_simple_feature_geo', count(*) from test_topo_ar5_test2.flate_t1_result where geo is not null;

SELECT 'degrees_check_added_simple_feature_artype', count(*) from test_topo_ar5_test2.flate_t1_result where artype is not null;

SELECT 'degrees_check_added_simple_feature_artype_group by', r.* from (select count(*), round(sum(ST_Area(geo,true))) as area, artype from test_topo_ar5_test2.flate_t1_result where artype is not null group by artype) as r order by artype;

SELECT 'degrees_check_added_simple_feature_arskogbon', count(*) from test_topo_ar5_test2.flate_t1_result where arskogbon is not null;

--SELECT 'degrees_check_added_simple_feature_artype face_attributes', count(*) from test_topo_ar5_test2.face_attributes where artype is not null;

-- Records from this function would mean an invalid topology was created
SELECT 'validation', * FROM topology.ValidateTopology('test_topo_ar5_test2');

SELECT 'drop test_topo_ar5_test2', topology.droptopology('test_topo_ar5_test2');
