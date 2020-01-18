DROP FUNCTION IF EXISTS topo_update.set_blocked_area (input_table_name varchar, input_table_geo_column_name varchar, input_table_pk_column_name varchar, bb geometry, _job_list_name varchar);

CREATE OR REPLACE FUNCTION topo_update.set_blocked_area (input_table_name varchar, input_table_geo_column_name varchar, input_table_pk_column_name varchar, _job_list_name varchar, bb geometry)
  RETURNS geometry
  LANGUAGE 'plpgsql'
  AS $function$
DECLARE
  command_string text;
  get_boundery_function text = 'ST_Boundary';
  data_env geometry;
  num_rows int;
  -- we extra distace outside this box
  -- 0.2
  extra_block_dist float = 1;
  is_done integer = 0;
BEGIN
  -- check if job is done already
  command_string := Format('select count(*) from %s as gt, %s as done
   where gt.cell_geo && ST_PointOnSurface(%3$L) and gt.id = done.id', _job_list_name, _job_list_name || '_donejobs', bb);
  EXECUTE command_string INTO is_done;
  IF is_done = 1 THEN
    RAISE NOTICE 'Job is_done for  : %', ST_astext (bb);
    RETURN NULL;
  END IF;
  -- get area to block for update
  -- geometry(LineString
  RAISE NOTICE 'Get area to lock for : %', ST_Centroid (bb);
  DROP TABLE IF EXISTS tmp_data_border_lines;
  IF Strpos((vsr_get_data_type (input_table_name, input_table_geo_column_name)), 'LineString') > 0 THEN
    command_string := Format('create temp table tmp_data_border_lines as 
 	 	( SELECT DISTINCT 
             (
               ST_Dump(
                 %5$s((
                     ST_Dump(g.%3$s)
                   ).geom
                 )
               )
             ).geom as geo, 
             g.%4$s as id 
           FROM 
             %2$s g
          WHERE g.%3$s && %1$L 
         )', bb, input_table_name, input_table_geo_column_name, input_table_pk_column_name, get_boundery_function);
  ELSE
    command_string := Format('create temp table tmp_data_border_lines as 
 	 	( SELECT DISTINCT 
             (
               ST_Dump(
                 %5$s((
                     ST_Dump(g.%3$s)
                   ).geom
                 )
               )
             ).geom as geo, 
             g.%4$s as id 
           FROM 
             %2$s g
          WHERE g.%3$s && %1$L
         )', bb, input_table_name, input_table_geo_column_name, input_table_pk_column_name, get_boundery_function);
  END IF;
  --	RAISE NOTICE 'execute command_string; %', command_string;
  EXECUTE command_string;
  GET DIAGNOSTICS num_rows = ROW_COUNT;
  IF num_rows > 0 THEN
    command_string := Format('select ST_Buffer(ST_Envelope(ST_Collect(nbl.geo,ng.cell_geo)),%3$L) from tmp_data_border_lines nbl, %1$s ng  
       where ng.cell_geo && ST_PointOnSurface(%2$L)', _job_list_name, bb, extra_block_dist);
    EXECUTE command_string INTO data_env;
    RAISE NOTICE 'Found area to lock with size  : %', ST_area (data_env);
    --	 command_string := format('ANALYZE ',_job_list_name);
    --	execute command_string;
  ELSE
    RAISE NOTICE 'No area to lock found for  : %', ST_astext (bb);
  END IF;
  RETURN data_env;
END
$function$;

--\timing
--select topo_update.set_blocked_area('tmp_sf_ar5_forest_input.existing_forest_surface','wkb_geometry','ogc_fid','topo_update.job_list_block',
--'0103000020E864000001000000050000000000004035BD2341000000A0A6EB58410000004035BD23410000001419EC5841000000A093C223410000001419EC5841000000A093C22341000000A0A6EB58410000004035BD2341000000A0A6EB5841');
--update topo_update.job_list_block SET block_bb = null;
--select topo_update.set_blocked_area('tmp_sf_ar5_forest_input.selected_forest_area','wkb_geometry','ogc_fid','topo_update.job_list_block',
--'0103000020E8640000010000000500000040F1FF7F101C2241BCFAFF272632594140F1FF7F101C2241D8F9FF2FA437594114EFFFFF23232241D8F9FF2FA437594114EFFFFF23232241BCFAFF272632594140F1FF7F101C2241BCFAFF2726325941');
--select topo_update.set_blocked_area('tmp_sf_ar5_forest_input.selected_forest_area','wkb_geometry','ogc_fid','topo_update.job_list_block',
--'0103000020E8640000010000000500000000000000815C2341000000D0D20C594100000000815C234100000020E810594100000040FC6D234100000020E810594100000040FC6D2341000000D0D20C594100000000815C2341000000D0D20C5941');
