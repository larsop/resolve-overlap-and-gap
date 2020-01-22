CREATE OR REPLACE FUNCTION topo_update.get_left_over_borders (
_overlapgap_grid varchar,
_input_table_geo_column_name character varying,
_bb geometry, 
_table_name_result_prefix varchar 
)
  RETURNS TABLE (
    geo Geometry(LineString))
  LANGUAGE 'plpgsql'
  AS $function$
DECLARE
BEGIN
  RETURN QUERY EXECUTE 'SELECT 
 	distinct lg3.geo 
 	FROM ' || _table_name_result_prefix||'_border_line_segments 	lg3
 	where ST_IsValid(lg3.geo) and ST_Intersects(lg3.geo,$1)
 	and (
 	ST_StartPoint(lg3.geo) && $1 or 
 	(ST_EndPoint(lg3.geo) && $1 and NOT EXISTS (SELECT 1 FROM ' || _overlapgap_grid || ' gt where ST_StartPoint(lg3.geo) && gt.'||_input_table_geo_column_name||'))
 	)'
  USING _bb;
END
$function$;

