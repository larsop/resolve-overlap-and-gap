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
END IF;

END;
$$
LANGUAGE plpgsql;
