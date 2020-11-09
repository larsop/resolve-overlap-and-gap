-- this function that adds indexes and set table to logged soooo


CREATE OR REPLACE FUNCTION resolve_overlap_gap_post (
_input_data resolve_overlap_data_input_type, 
_topology_info resolve_overlap_data_topology_type,
_table_name_result_prefix varchar
)
  RETURNS VOID
  AS $$
DECLARE
 
BEGIN



IF (_topology_info).create_topology_attrbute_tables = true OR (_topology_info).create_topology_attrbute_tables = true THEN
  EXECUTE Format('ALTER TABLE %s.relation SET logged', (_topology_info).topology_name);
  EXECUTE Format('ALTER TABLE %s.face SET logged', (_topology_info).topology_name);
  EXECUTE Format('ALTER TABLE %s.node SET logged', (_topology_info).topology_name);
  EXECUTE Format('ALTER TABLE %s.edge_data SET logged', (_topology_info).topology_name);
  
  EXECUTE Format('ALTER TABLE %s SET logged',(_topology_info).topology_name||'.edge_attributes');
  EXECUTE Format('ALTER TABLE %s SET logged',(_topology_info).topology_name||'.face_attributes');
  
END IF;

IF (_topology_info).create_topology_attrbute_tables = true and (_input_data).line_table_name is not null THEN
  EXECUTE Format('CREATE INDEX ON %s(((%s).id)) ',
  (_topology_info).topology_name||'.edge_attributes',
  (_input_data).line_table_geo_collumn);
END IF;

IF (_topology_info).create_topology_attrbute_tables = true and (_input_data).polygon_table_name is not null THEN
  EXECUTE Format('CREATE INDEX ON %s(((%s).id)) ',
  (_topology_info).topology_name||'.face_attributes',
  (_input_data).polygon_table_geo_collumn);

  EXECUTE Format('CREATE INDEX ON %s(%s) ',
  (_topology_info).topology_name||'.relation',
  'topogeo_id');
ELSE

  EXECUTE Format('ALTER TABLE %s SET logged',_table_name_result_prefix||'_result');
  EXECUTE Format('GRANT select ON TABLE %s TO PUBLIC',_table_name_result_prefix||'_result');
  -- TODO should have been done after data are created
  EXECUTE Format('CREATE INDEX ON %s USING GIST (%s)', _table_name_result_prefix||'_result',(_input_data).polygon_table_geo_collumn);

END IF;


END;
$$
LANGUAGE plpgsql;
