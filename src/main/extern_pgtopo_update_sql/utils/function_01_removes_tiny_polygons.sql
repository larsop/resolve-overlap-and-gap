-- check if this geo ovetlaps with data in org layer
DROP FUNCTION IF EXISTS topo_update.removes_tiny_polygons (_face_id int);

DROP FUNCTION IF EXISTS topo_update.removes_tiny_polygons (_atopology varchar, _face_id int);

DROP FUNCTION IF EXISTS topo_update.removes_tiny_polygons (_atopology varchar, _face_id int, topo_area float);

DROP FUNCTION IF EXISTS topo_update.removes_tiny_polygons (_atopology varchar, _face_id int, topo_area float, _min_area float);

CREATE OR REPLACE FUNCTION topo_update.removes_tiny_polygons (_atopology varchar, _face_id int, topo_area float, _min_area float)
  RETURNS int
  AS $$
DECLARE
  -- holds dynamic sql to be able to use the same code for different
  command_string text;
  remove_edge int;
  remove_count int = 0;
BEGIN
  IF (topo_area < _min_area) THEN
    BEGIN
      command_string := Format('select edge_id FROM (                                                     
   SELECT edge_id, ST_length(geom)  as edge_length from %1$s.edge_data 
   where ((%2$L = left_face) or (%2$L = right_face)) 
   order by edge_length desc
   ) as r limit 1', _atopology, _face_id);
      EXECUTE command_string INTO remove_edge;
      IF (remove_edge > 0) THEN
        -- using perform ST_RemEdgeModFace(_atopology, remove_edge);  seem make invalid faces somtimes
        PERFORM ST_RemEdgeNewFace (_atopology, remove_edge);
        remove_count := 1;
        RAISE NOTICE 'For tiny face_id % has egde_id % been removed', _face_id, remove_edge;
      END IF;
      EXCEPTION
      WHEN OTHERS THEN
        RAISE NOTICE 'ERROR failed to remove tiny face % ', _face_id;
      END;
  END IF;
  RETURN remove_count;
END;

$$
LANGUAGE plpgsql;

--}
--select topo_update.removes_tiny_polygons('topo_ar5_forest_sysdata_35',14,100);
