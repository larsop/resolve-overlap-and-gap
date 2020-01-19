/**
 * Clear blocked area
 */
CREATE OR REPLACE FUNCTION topo_update.clear_blocked_area (bb geometry, _job_list_name varchar)
  RETURNS void
  LANGUAGE 'plpgsql'
  AS $function$
DECLARE
  command_string text;
BEGIN
  --	execute command_string;
  command_string := Format('insert into %s(id) select gt.id from %s as gt
       where gt.cell_geo && ST_PointOnSurface(%3$L)', _job_list_name || '_donejobs', _job_list_name, bb);
  EXECUTE command_string;
END
$function$;

