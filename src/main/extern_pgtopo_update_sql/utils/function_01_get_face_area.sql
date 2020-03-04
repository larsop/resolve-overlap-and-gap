/**
 * Get face area in meter, exception return 0
 */
CREATE OR REPLACE FUNCTION topo_update.get_face_area(_atopology varchar, _face_id int, utm boolean)
  RETURNS float
  AS $$
DECLARE
  -- holds dynamic sql to be able to use the same code for different
  command_string text;
  face_area float = 0;
BEGIN
  BEGIN
	IF (utm = false) THEN
      face_area := ST_Area (st_getFaceGeometry (_atopology, _face_id), TRUE);
    ELSE
      face_area := ST_Area (st_getFaceGeometry (_atopology, _face_id)); 
    END IF;
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

