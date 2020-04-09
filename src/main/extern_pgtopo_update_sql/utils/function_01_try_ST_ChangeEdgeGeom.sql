
CREATE OR REPLACE FUNCTION 
topo_update.try_ST_ChangeEdgeGeom(_org_geom Geometry, _atopology varchar, _max_average_vertex_length real, _utm boolean, _edge int, _geom Geometry)
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

	IF ST_Equals(ST_AsBinary(_org_geom), ST_AsBinary(_geom)) THEN
	  RETURN 0;
	END IF;
	
	
    -- we may not want change lines that have very long lines.
	IF (_max_average_vertex_length is not null and _max_average_vertex_length > 0) THEN
	  IF _utm and ST_Length(_org_geom)/ST_NumPoints(_org_geom) > _max_average_vertex_length THEN
	    RETURN 0;
	  ELSIF _utm = false and ST_Length(_org_geom,true)/ST_NumPoints(_org_geom) > _max_average_vertex_length THEN 
	    RETURN 0;
	  END IF;
	END IF;
	
    BEGIN                                                                                                                       

	command_string := Format('select topology.ST_ChangeEdgeGeom(%1$L,%2$s,%3$L)', _atopology, _edge, _geom);    
	execute command_string into result_var;
	result := 1;

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

--SELECT topo_update.try_ST_ChangeEdgeGeom(e.geom,'test_topo_sr16',e.edge_id, ST_simplifyPreserveTopology(e.geom,5.0)) from test_topo_sr16.edge_data e where edge_id = 2219308

--SELECT topo_update.try_ST_ChangeEdgeGeom(e.geom, 'test_topo_sr16',e.edge_id, topo_update.chaikinsAcuteAngle(e.geom,25,true,120,240,2)) FROM test_topo_sr16.edge_data e where edge_id = 2219308

--\timing
--SELECT topo_update.try_ST_ChangeEdgeGeom(e.geom,'test_topo_sr16',e.edge_id, ST_simplifyPreserveTopology(e.geom,5.0)) from test_topo_sr16.edge_data e where edge_id = 29855;

--SELECT topo_update.try_ST_ChangeEdgeGeom(e.geom, 'test_topo_sr16',e.edge_id, topo_update.chaikinsAcuteAngle(e.geom,25,true,120,240,2)) FROM test_topo_sr16.edge_data e where edge_id = 29855;

 
--select topo_update.chaikinsAcuteAngle(e.geom,25,true,120,240,2) FROM test_topo_sr16.edge_data e where edge_id = 2219308;
--SELECT ST_simplifyPreserveTopology(e.geom,5.0) from test_topo_sr16.edge_data e where edge_id = 2219308

