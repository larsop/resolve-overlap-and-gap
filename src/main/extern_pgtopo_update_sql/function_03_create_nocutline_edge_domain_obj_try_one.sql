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
  command_string text;
  -- the number times the input line intersects
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
