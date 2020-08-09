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
 command_string text;
BEGIN
   	
  command_string := Format(' SELECT r.geo FROM
    ( 
    SELECT l.geo, min(g.id) as min_id 
    FROM %1$s l,
    %2$s g
    where ST_Intersects(%4$L,l.geo) and ST_Intersects(l.geo,g.cell_geo)
    group by l.geo
    ) AS r,
    %2$s g 
    WHERE g.cell_geo && %4$L and ST_Intersects(g.cell_geo,  ST_PointOnSurface(%4$L)) and r.min_id = g.id',
  _table_name_result_prefix||'_border_line_segments',
  _table_name_result_prefix||'_job_list', 
  _input_table_geo_column_name, 
  _bb);
  
  RETURN QUERY EXECUTE command_string;
  
END
$function$;
