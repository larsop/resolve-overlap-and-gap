
CREATE OR REPLACE FUNCTION 
topo_update.try_ST_ChangeEdgeGeom(_atopology varchar, _edge int, _geom Geometry)
RETURNS int AS $$
DECLARE
  result int = 0;
  result_var varchar;
  v_state text;
  v_msg text;
  v_detail text;
  v_hint text;
  v_context text;
  command_string text;
  
  BEGIN                                                                                                                       

    BEGIN                                                                                                                       

	command_string := Format('select topology.ST_ChangeEdgeGeom(%1$L,%2$s,%3$L)', _atopology, _edge, _geom);    
	execute command_string into result_var;

	EXCEPTION WHEN OTHERS THEN
	  	
	    GET STACKED DIAGNOSTICS v_state = RETURNED_SQLSTATE, v_msg = MESSAGE_TEXT, v_detail = PG_EXCEPTION_DETAIL, v_hint = PG_EXCEPTION_HINT,
                    v_context = PG_EXCEPTION_CONTEXT;
        RAISE NOTICE 'FAILED select topology.ST_ChangeEdgeGeom(%, %, %) state  : %  message: % detail : % hint   : % context: %', 
        quote_literal(_atopology), _edge, _geom, 
        v_state, v_msg, v_detail, v_hint, v_context;
        result := -1;
	END;

    return result;
END;
$$  LANGUAGE plpgsql STABLE;
--}

--select topo_update.try_ST_ChangeEdgeGeom('topo_ar5_forest_sysdata', 4240268,1812214);

