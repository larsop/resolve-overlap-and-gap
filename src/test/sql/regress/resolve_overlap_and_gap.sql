CREATE EXTENSION dblink; -- needed by  execute_parallel



CREATE table test_data.overlap_gap_input_t2 AS (SELECT * from test_data.overlap_gap_input_t1 WHERE c1 in (633,1233,1231,834));

-- Call function to resolve overlap and gap in the function in test_data.overlap_gap_input_t1 which we just testet for overlap
CALL resolve_overlap_gap_run('test_data.overlap_gap_input_t2','geom',4258,'test_data.overlap_gap_input_t2_res','test_topo_t2',5,4);

SELECT 'check_border_lines', count(geo) from test_topo_t2.border_line_segments;

SELECT 'check_added_lines', count(geom) from test_topo_t2.edge;

SELECT topology.droptopology('test_topo_t2');
