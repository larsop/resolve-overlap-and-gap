-- check if this geo ovetlaps with data in org layer
DROP FUNCTION IF EXISTS topo_update.get_face_area (_face_id int);

DROP FUNCTION IF EXISTS topo_update.get_face_area (_atopology varchar, _face_id int);

CREATE OR REPLACE FUNCTION topo_update.get_face_area (_atopology varchar, _face_id int)
  RETURNS float
  AS $$
DECLARE
  -- holds dynamic sql to be able to use the same code for different
  command_string text;
  face_area float = 0;
BEGIN
  BEGIN
    face_area := ST_Area (st_getFaceGeometry (_atopology, _face_id), FALSE);
    EXCEPTION
    WHEN OTHERS THEN
      RAISE NOTICE 'WARNING failed to find area for face % ', _face_id;
    face_area := 0;
    END;
  RETURN face_area;
END;

$$
LANGUAGE plpgsql
STABLE;

--}
--select topo_update.get_face_area('topo_ar5_forest_sysdata',456343);
