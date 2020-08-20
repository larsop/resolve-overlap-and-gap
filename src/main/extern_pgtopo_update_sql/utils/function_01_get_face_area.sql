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
  v_state text;
  v_msg text;
  v_detail text;
  v_hint text;
  v_context text;
  face_id_found boolean = false;

BEGIN
  command_string := Format('select false FROM %1$s.face where face_id = %2$s limit 1', _atopology, _face_id);
  EXECUTE command_string INTO face_id_found;

  IF face_id_found = true THEN 
  BEGIN
	  IF (utm = false) THEN
        face_area := ST_Area (st_getFaceGeometry (_atopology, _face_id), TRUE);
      ELSE
        face_area := ST_Area (st_getFaceGeometry (_atopology, _face_id)); 
      END IF;
  	  EXCEPTION WHEN OTHERS THEN
	    GET STACKED DIAGNOSTICS v_state = RETURNED_SQLSTATE, v_msg = MESSAGE_TEXT, v_detail = PG_EXCEPTION_DETAIL, v_hint = PG_EXCEPTION_HINT,
                    v_context = PG_EXCEPTION_CONTEXT;
        RAISE NOTICE 'Failed to find area for face_id % in topo % state  : %  message: % detail : % hint   : % context: %', 
        _face_id, _atopology, v_state, v_msg, v_detail, v_hint, v_context;
      face_area := null;
      END;
  END IF;
    
  RETURN face_area;
END;

$$
LANGUAGE plpgsql
STABLE;

