
CREATE OR REPLACE FUNCTION resolve_overlap_gap_block_cell(
input_table_name varchar, 
input_table_geo_column_name varchar, 
input_table_pk_column_name varchar, 
_job_list_name varchar, 
bb geometry
)
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

