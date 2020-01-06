-- CREATE : OK 07/09/2017
-- TEST :

-- This is a function used to import data from simple feature to topplogy for surface laers


DROP FUNCTION IF EXISTS topo_update.simplefeature_2_topo_surface_border_retry(
layer_schema text, 
  border_layer_table text, border_layer_column text,  input_table_name text,
  snap_tolerance float8, bb geometry );

  DROP FUNCTION IF EXISTS topo_update.simplefeature_2_topo_surface_border_retry(
layer_schema text, 
  border_layer_table text, border_layer_column text,  input_table_name text,
  inside_cell_data boolean,
  snap_tolerance float8, bb geometry );

  DROP FUNCTION IF EXISTS topo_update.simplefeature_2_topo_surface_border_retry(
layer_schema text, 
  border_layer_table text, border_layer_column text,  input_table_name text,
  snap_tolerance float8,
  bb geometry) ;

DROP FUNCTION IF EXISTS  topo_update.simplefeature_2_topo_surface_border_retry(
  input_table_name varchar,
input_table_geo_column_name varchar,
 layer_schema varchar, 
  border_layer_table text, 
  border_layer_column text, 
  snap_tolerance float8,
  bb geometry,
  inside_cell_data boolean
);

DROP FUNCTION IF EXISTS  topo_update.simplefeature_2_topo_surface_border_retry(
  input_table_name varchar,
input_table_geo_column_name varchar,
input_table_pk_column_name varchar,
 layer_schema varchar, 
  border_layer_table text, 
  border_layer_column text, 
  snap_tolerance float8,
  bb geometry,
  inside_cell_data boolean
);

DROP FUNCTION IF EXISTS  topo_update.simplefeature_2_topo_surface_border_retry(
  input_table_name varchar,
input_table_geo_column_name varchar,
input_table_pk_column_name varchar,
layer_schema text, 
  border_layer_table text, border_layer_column text,
  snap_tolerance float8,
  bb geometry,
  inside_cell_data boolean
); 

DROP FUNCTION IF EXISTS  topo_update.simplefeature_2_topo_surface_border_retry(
  input_table_name varchar,
input_table_geo_column_name varchar,
input_table_pk_column_name varchar,
layer_schema text, 
  border_layer_table text, border_layer_column text,
  snap_tolerance float8,
  _job_list_name varchar,
  bb geometry,
  inside_cell_data boolean
); 


CREATE FUNCTION topo_update.simplefeature_2_topo_surface_border_retry(
  input_table_name varchar,
input_table_geo_column_name varchar,
input_table_pk_column_name varchar,
layer_schema text, 
  border_layer_table text, border_layer_column text,
  snap_tolerance float8,
  _job_list_name varchar,
  bb geometry,
  inside_cell_data boolean
) 
  RETURNS integer AS $$
  
  DECLARE

  surface_topo_info topo_update.input_meta_info ;
	border_topo_info topo_update.input_meta_info ;
	
	-- holde the computed value for json input reday to use
	json_input_structure topo_update.json_input_structure;  

	server_json_feature text;
	
  -- holds dynamic sql to be able to use the same code for different
	command_string text;
    added_rows int;

        start_time timestamp with time zone;
    done_time timestamp with time zone;
    used_time real;

    BEGIN

	start_time  := clock_timestamp();

	RAISE NOTICE 'timeofday:% ,start job nocutline', timeofday();


	IF bb is NULL and input_table_name is not null  THEN
		command_string := format('select ST_Envelope(ST_Collect(geo)) from %s',input_table_name);
		EXECUTE command_string into bb;
	END IF;
    
	
	-- get meta data the border line for the surface
	border_topo_info := topo_update.make_input_meta_info(layer_schema, border_layer_table , border_layer_column );
	
	border_topo_info.snap_tolerance :=  snap_tolerance;

	-- TODO totally rewrite this code
	--json_input_structure := topo_update.handle_input_json_props(json_input_structure::json,server_json_feature::json,border_topo_info.srid);

	-- RAISE NOTICE 'DONE with  topo_update.make_input_meta_info  %' , border_topo_info;
	
	command_string := format(
	'SELECT topo_update.create_nocutline_edge_domain_obj_retry(json::text, %L) 
	FROM topo_update.view_split_distinct_func(%L,%L,%L,%L,%L,%L) g',
	border_topo_info, input_table_name,input_table_geo_column_name,input_table_pk_column_name,bb,inside_cell_data,_job_list_name);
	
	--RAISE NOTICE 'command_string %' , command_string;
	----
--SELECT topo_update.create_nocutline_edge_domain_obj_retry(json::text, '(topo_ar5_forest_sysdata,topo_ar5_forest,ar5_forest_grense,grense,2,1,1,25832)'::topo_update.input_meta_info)
--FROM topo_update.view_split_distinct_func('tmp_sf_ar5_forest_input.existing_forest_surface','0103000020E86400000100000005000000DA1E85F79D77224154EDA158F2085941DA1E85F79D7722411ABBC026830A59414433337BE78022411ABBC026830A59414433337BE780224154EDA158F2085941DA1E85F79D77224154EDA158F2085941','t')

	EXECUTE command_string;
			
	RAISE NOTICE 'timeofday:% ,done job nocutline ready to start next', timeofday();

	done_time  := clock_timestamp();
	used_time :=  (EXTRACT(EPOCH FROM (done_time - start_time)));
	RAISE NOTICE 'work done proc :% border_layer_id %, using % sec', done_time, border_topo_info.border_layer_id, used_time;

-- This is a list of lines that fails
-- this is used for debug

	IF used_time > 10 THEN
		RAISE NOTICE 'very long a set of lines % time with geo for bb % ', used_time, bb;
		insert into topo_update.long_time_log2(execute_time,info,sql,geo) 
		values(used_time,'simplefeature_2_topo_surface_border_retry',command_string, bb);
	END IF;

	command_string := format(
	'SELECT count(*) from %I.%I',layer_schema,  border_layer_table);

    EXECUTE command_string INTO added_rows;
    
    return added_rows;
    
    END;

$$ LANGUAGE plpgsql;



--psql -h localhost -U postgres sl -c "SELECT topo_update.simplefeature_2_topo_surface_border_retry('topo_ar5_forest','ar5_forest_grense','grense','topo_update.topo_ar5_forest_org_from_sf_2_json_grense_v','false',1,'0103000020E86400000100000005000000248BC215F70624410A6E11E197F45841248BC215F70624417ADA07521E015941292E3333435124417ADA07521E015941292E3333435124410A6E11E197F45841248BC215F70624410A6E11E197F45841')"