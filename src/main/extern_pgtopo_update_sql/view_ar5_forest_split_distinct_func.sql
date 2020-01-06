--https://gis.stackexchange.com/questions/94049/how-to-get-the-data-type-of-each-column-from-a-postgis-table

CREATE OR REPLACE FUNCTION "vsr_get_data_type"(_t regclass, _c text)
  RETURNS text AS
$body$
DECLARE
    _schema text;
    _table text;
    data_type text;
BEGIN
-- Prepare names to use in index and trigger names
IF _t::text LIKE '%.%' THEN
    _schema := regexp_replace (split_part(_t::text, '.', 1),'"','','g');
    _table := regexp_replace (split_part(_t::text, '.', 2),'"','','g');
    ELSE
        _schema := 'public';
        _table := regexp_replace(_t::text,'"','','g');
    END IF;

    data_type := 
    (
        SELECT format_type(a.atttypid, a.atttypmod)
        FROM pg_attribute a 
        JOIN pg_class b ON (a.attrelid = b.oid)
        JOIN pg_namespace c ON (c.oid = b.relnamespace)
        WHERE
            b.relname = _table AND
            c.nspname = _schema AND
            a.attname = _c
     );

    RETURN data_type;
END
$body$ LANGUAGE plpgsql;

DROP FUNCTION IF EXISTS topo_update.view_split_distinct_func(
  input_table_name varchar,
input_table_geo_column_name varchar,
  bb geometry,
  inside_cell_data boolean
);

DROP FUNCTION IF EXISTS topo_update.view_split_distinct_func(
  input_table_name varchar,
input_table_geo_column_name varchar,
  bb geometry,
  inside_cell_data boolean,
_job_list_name varchar
);

CREATE OR REPLACE FUNCTION topo_update.view_split_distinct_func(
input_table_name varchar,
input_table_geo_column_name varchar,
input_table_pk_column_name varchar,
bb geometry,
inside_cell_data boolean,
_job_list_name varchar
) RETURNS TABLE (
  json text, 
  geo geometry(LineString, 25832), 
  objectid integer
) LANGUAGE 'plpgsql' AS $function$ 
DECLARE
 grid_lines  geometry(LineString,25832);
  command_string text;
  
  get_boundery_function text = 'ST_Multi';
  

BEGIN 
	
	if strpos((vsr_get_data_type(input_table_name,input_table_geo_column_name)),'Polygon') > 0 then 
	  get_boundery_function := 'ST_Boundary';
	end if;
	
	drop table if exists tmp_data_border_lines; 

	IF NOT inside_cell_data THEN
	
	
	grid_lines := ST_ExteriorRing(bb) as geo;

	command_string := format('LOCK TABLE %s IN SHARE ROW EXCLUSIVE MODE',_job_list_name);
	execute command_string;

	command_string := format('create temp table tmp_data_border_lines as 
	 	( SELECT lg.geo as geo
     FROM 
      (
        WITH sample(geom, id) AS (
          SELECT DISTINCT
            (
              ST_Dump(
                %6$s(
                  (
                    ST_Dump(g.%4$s)
                  ).geom
                )
              )
            ).geom as geom, 
            g.%5$s as id 
          FROM 
            %3$s g
         WHERE g.%4$s && %1$L and ST_intersects(g.%4$s,%1$L)
        ), 
        line_counts (cts, id) AS (
          SELECT 
            ST_NPoints(geom) -1, 
            id 
          FROM 
            sample
        ), 
        series (num, id) AS (
          SELECT 
            generate_series(1, cts), 
            id 
          FROM 
            line_counts
        ) 
        SELECT 
          case when ST_intersects(ST_PointN(sample.geom, num),%2$L)
	      AND ST_LineCrossingDirection(ST_MakeLine(
	            ST_PointN(geom, num), 
	            ST_PointN(geom, num + 1)
	          ) , %1$L) = 1
          THEN
	          ST_MakeLine(
	            ST_PointN(geom, num), 
	            ST_PointN(geom, num + 1)
	          ) 
 		  WHEN ST_intersects(ST_PointN(sample.geom, num+1),%2$L)
	      AND ST_LineCrossingDirection(ST_MakeLine(
	            ST_PointN(geom, num), 
	            ST_PointN(geom, num + 1)
	          ) , %1$L) = 1
          THEN
	          ST_MakeLine(
	            ST_PointN(geom, num), 
	            ST_PointN(geom, num + 1)
	          ) 
	      else
	          null
	      END as geo
	      
	      FROM 
          series, sample 
        WHERE series.id = sample.id 
      ) AS lg 
      LEFT JOIN  %7$s gt ON gt.cell_geo && lg.geo
      and ST_equals(gt.cell_geo,%2$L) 
     WHERE lg.geo is not null
--     AND  (gt.border_lines is null OR ST_intersects(gt.border_lines,lg.geo) = false)
)'
	,grid_lines
	,bb
	,input_table_name
	,input_table_geo_column_name
	,input_table_pk_column_name
	,get_boundery_function
	,_job_list_name
	
	);

	execute command_string;
	
      command_string := format('update %s as gt
      set border_lines = (SELECT ST_Multi(ST_Union(nbl.geo)) from tmp_data_border_lines as nbl)
      where gt.cell_geo && %2$L and ST_equals(gt.cell_geo,%2$L)',_job_list_name,bb);

      	execute command_string;

ELSE	

	command_string := format('create temp table tmp_data_border_lines as 
	 	( SELECT lg.geo as geo
     FROM 
      (
        WITH sample(geom, id) AS (
          SELECT DISTINCT
            (
              ST_Dump(
                %5$s(
                  (
                    ST_Dump(g.%3$s)
                  ).geom
                )
              )
            ).geom as geom, 
            g.%4$s as id 
          FROM 
            %2$s g
         WHERE g.%3$s && %1$L
        ), 
        line_counts (cts, id) AS (
          SELECT 
            ST_NPoints(geom) -1, 
            id 
          FROM 
            sample
        ), 
        series (num, id) AS (
          SELECT 
            generate_series(1, cts), 
            id 
          FROM 
            line_counts
        ) 
        SELECT 
          case when 
          ST_intersects(ST_PointN(sample.geom, num),%1$L) and ST_intersects(ST_PointN(sample.geom, num+1),%1$L) 
          THEN
	          ST_MakeLine(
	            ST_PointN(geom, num), 
	            ST_PointN(geom, num + 1)
	          ) 
	          else
	          null
	      END as geo
	      
	      FROM 
          series, sample 
        WHERE series.id = sample.id 
      ) AS lg 
)'
,bb
,input_table_name
,input_table_geo_column_name
,input_table_pk_column_name
,get_boundery_function);

	execute command_string;
      
      END IF;

      	RETURN QUERY 
SELECT 
  * 
FROM 
  (
    SELECT 
      '{"type": "Feature",' || '"geometry":' || ST_AsGeoJSON(lg3.geo, 10, 2):: json || ',' || '"properties":' || row_to_json(
        (
          SELECT 
            l 
          FROM 
            (
              SELECT 
                "oppdateringsdato"
            ) As l
        )
      ) || '}' as json, 
      lg3.geo, 
      lg3.objectid 
      FROM (
    SELECT distinct lg2.geo, now() as "oppdateringsdato", 1 as "objectid" FROM
    	 	( SELECT (ST_Dump(ST_LineMerge(ST_Union(lg.geo)))).geom as geo
from tmp_data_border_lines lg)  lg2
      ) As lg3
      ) As f;

END $function$;


--\timing
--select count(*) from topo_update.view_split_distinct_func('tmp_sf_ar5_forest_input.existing_forest_surface','wkb_geometry','ogc_fid','0103000020E86400000100000005000000248BC215F70624410A6E11E197F45841248BC215F70624417ADA07521E015941292E3333435124417ADA07521E015941292E3333435124410A6E11E197F45841248BC215F70624410A6E11E197F45841',true,'topo_update.job_list_c');
--select count(*) from topo_update.view_split_distinct_func('tmp_sf_ar5_forest_input.existing_forest_surface','wkb_geometry','ogc_fid','0103000020E86400000100000005000000248BC215F70624410A6E11E197F45841248BC215F70624417ADA07521E015941292E3333435124417ADA07521E015941292E3333435124410A6E11E197F45841248BC215F70624410A6E11E197F45841',false,'topo_update.job_list_c');
