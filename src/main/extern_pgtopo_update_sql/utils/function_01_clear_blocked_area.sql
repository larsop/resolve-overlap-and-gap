DROP FUNCTION IF EXISTS topo_update.clear_blocked_area (bb geometry, _job_list_name varchar);

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

--\timing
--select topo_update.clear_blocked_area('0103000020E864000001000000050000000000004035BD2341000000A0A6EB58410000004035BD23410000001419EC5841000000A093C223410000001419EC5841000000A093C22341000000A0A6EB58410000004035BD2341000000A0A6EB5841','topo_update.job_list_block');
--psql -p 5433 -h vroom2.ad.skogoglandskap.no  -U postgres sl -c "select topo_update.do_remove_small_areas('topo_ar5_forest_sysdata.face','mbr','face_id','topo_update.job_list_block','0103000020E86400000100000005000000FEE4FF5FE8C3224198FBFF7722BF5941FEE4FF5FE8C32241ADFBFFE1F6C1594176E4FF7F5ECC2241ADFBFFE1F6C1594176E4FF7F5ECC224198FBFF7722BF5941FEE4FF5FE8C3224198FBFF7722BF5941');"
