
CREATE OR REPLACE FUNCTION topo_update.try_ST_ModEdgeHeal(_atopology varchar, _edge_to_live int, _edge_to_eat int)
  RETURNS int
  AS $$
DECLARE
  -- holds dynamic sql to be able to use the same code for different
  command_string text;
  result int = 0;
  v_state text;
  v_msg text;
  v_detail text;
  v_hint text;
  v_context text;

BEGIN
  BEGIN
    result = topology.ST_ModEdgeHeal (_atopology, _edge_to_live, _edge_to_eat);
    
	EXCEPTION WHEN OTHERS THEN
  	
     GET STACKED DIAGNOSTICS v_state = RETURNED_SQLSTATE, v_msg = MESSAGE_TEXT, v_detail = PG_EXCEPTION_DETAIL, v_hint = PG_EXCEPTION_HINT,
                v_context = PG_EXCEPTION_CONTEXT;
  --   RAISE NOTICE 'FAILED select ST_ModEdgeHeal(%, %, %) state  : %  message: % detail : % hint   : % context: %', 
  --   Quote_literal(_atopology), _edge_to_live, _edge_to_eat,v_state, v_msg, v_detail, v_hint, v_context;
     result := -1;
  END;
  RETURN result;
END;

$$
LANGUAGE plpgsql
STABLE;

