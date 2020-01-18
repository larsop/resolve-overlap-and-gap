-- This a function that will be called from the client when user is drawing a line
-- This line will be applied the data in the line layer
-- The result is a set of id's of the new line objects created
-- TODO set attributtes for the line

DROP FUNCTION IF EXISTS topo_update.create_nocutline_edge_domain_try_one (json_feature text, border_topo_info topo_update.input_meta_info, server_json_feature text);

-- {
CREATE OR REPLACE FUNCTION topo_update.create_nocutline_edge_domain_try_one (border_topo_info topo_update.input_meta_info, json_input_structure topo_update.json_input_structure, server_json_feature text DEFAULT NULL)
  RETURNS TABLE (
    id integer
  )
  AS $$
DECLARE
  v_error_stack text;
  -- this is the tolerance used for snap to
  -- TODO use as parameter put for testing we just have here for now
  -- border_topo_info topo_update.input_meta_info ;
  -- holds dynamic sql to be able to use the same code for different
  command_string text;
  -- the number times the input line intersects
  num_edge_intersects int;
  -- holds the value for felles egenskaper from input
  felles_egenskaper_linje topo_rein.sosi_felles_egenskaper;
  -- array of quoted field identifiers
  -- for attribute fields passed in by user and known (by name)
  -- in the target table
  not_null_fields text[];
  -- holde the computed value for json input reday to use
  --json_input_structure topo_update.json_input_structure;
BEGIN
  -- Convert geometry to TopoGeometry, write it in the temp table
  command_string := Format('select topology.TopoGeo_AddLineString(%L, %L, %L)', border_topo_info.topology_name, json_input_structure.input_geo, border_topo_info.snap_tolerance);
  RAISE NOTICE 'command_string %', command_string;
  EXECUTE command_string;
  RETURN;
END;
$$
LANGUAGE plpgsql;

--}
