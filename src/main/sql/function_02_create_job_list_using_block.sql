-- CREATE : OK 07/09/2017
-- TEST :

-- This is a function used to import data from simple feature to topplogy for surface laers

DROP FUNCTION IF EXISTS topo_update.create_job_list_using_block(
table_name_column_name_array varchar,
input_table_pk_column_name varchar,
max_cell_rows integer,
layer_schema text, 
border_layer_table text, 
border_layer_column text,  
_simplify_tolerance float8,
snap_tolerance float8,
_do_chaikins boolean,
create_new_grid boolean, 
_job_list_name varchar,
inside_cell_data boolean
); 

CREATE FUNCTION topo_update.create_job_list_using_block(
table_name_column_name_array varchar,
input_table_pk_column_name varchar,
max_cell_rows integer,
layer_schema text, 
border_layer_table text, 
border_layer_column text,  
_simplify_tolerance float8,
snap_tolerance float8,
_do_chaikins boolean,
create_new_grid boolean, 
_job_list_name varchar,
inside_cell_data boolean
) 
  RETURNS integer AS $$
  
DECLARE
command_string text;
command_string_var text;
num_rows int;
grid_geom geometry = ST_GeomFromText('POINT(0 0)');
min_distance integer = 100;

line_values VARCHAR[];
input_table_name VARCHAR;
input_table_geo_column_name VARCHAR;
sql_to_block_cmd VARCHAR;
topo_ar5_forest VARCHAR = 'topo_ar5_forest_sysdata';
BEGIN

	
SELECT string_to_array(table_name_column_name_array, ' ') INTO line_values; 
input_table_name := line_values[1];
input_table_geo_column_name := line_values[2];

-- create grid
IF create_new_grid=true THEN

DROP table if exists grid_tmp;

command_string := FORMAT('
CREATE TABLE grid_tmp AS (
SELECT q_grid.cell::geometry(geometry,25832)  as geo 
FROM (
SELECT(ST_Dump(
cbg_content_based_balanced_grid(ARRAY[%s],%L,%s,%s))
).geom AS cell)
AS q_grid
)', 
quote_literal(table_name_column_name_array),
grid_geom,
min_distance,
max_cell_rows
);
RAISE NOTICE 'command_string %', command_string;
execute command_string;

CREATE INDEX ON grid_tmp USING GIST (geo);

alter table grid_tmp add id serial;

END IF;

-- create jobList table
command_string := FORMAT('DROP table if exists %s',_job_list_name);
RAISE NOTICE 'command_string %', command_string;
execute command_string;

command_string := FORMAT('CREATE table %s(id serial, start_time timestamp with time zone, sql_to_block varchar, sql_to_run varchar, cell_geo geometry(geometry,25832),block_bb Geometry(geometry,25832))',_job_list_name);
RAISE NOTICE 'command_string %', command_string;
execute command_string;

-- create jobList table
command_string := FORMAT('DROP table if exists %s',_job_list_name||'_donejobs');
RAISE NOTICE 'command_string %', command_string;
execute command_string;

command_string := FORMAT('CREATE table %s(id int, done_time timestamp with time zone default clock_timestamp())'
,_job_list_name||'_donejobs');
RAISE NOTICE 'command_string %', command_string;
execute command_string;



sql_to_block_cmd := FORMAT('select topo_update.set_blocked_area(%s,%s,%s,%s,',
quote_literal(input_table_name),
quote_literal(input_table_geo_column_name),
quote_literal(input_table_pk_column_name),
quote_literal(_job_list_name)
);

	command_string_var := FORMAT('SELECT topo_update.simplefeature_c2_topo_surface_border_retry(%s,%s,%s,%s,%s,%s,%s,%s,', 
	quote_literal(input_table_name),
	quote_literal(input_table_geo_column_name),
	quote_literal(input_table_pk_column_name),
	quote_literal(topo_ar5_forest),
	_simplify_tolerance,
	snap_tolerance, 
	quote_literal(_do_chaikins),
	quote_literal(_job_list_name)
	);
	
	RAISE NOTICE 'command_string_var %', command_string_var;
	
	-- add inside cell polygons
	command_string := FORMAT('
	INSERT INTO %s(sql_to_run,cell_geo,sql_to_block) 
	SELECT %s||quote_literal(r.geo::varchar)||%s as sql_to_run, r.geo as cell_geo, %s||quote_literal(r.geo::varchar)||%s as sql_to_block
	FROM grid_tmp r',
	_job_list_name,
	quote_literal(command_string_var),
	quote_literal(','||inside_cell_data||');'),
	quote_literal(sql_to_block_cmd),
	quote_literal(');')
	);
	RAISE NOTICE 'command_string %', command_string;
	execute command_string;


command_string := FORMAT('CREATE INDEX ON %s USING GIST (cell_geo);',_job_list_name);
RAISE NOTICE 'command_string %', command_string;
execute command_string;

command_string := FORMAT('CREATE INDEX ON %s USING GIST (block_bb);',_job_list_name);
RAISE NOTICE 'command_string %', command_string;
execute command_string;

command_string := FORMAT('CREATE INDEX ON %s (id);',_job_list_name);
RAISE NOTICE 'command_string %', command_string;
execute command_string;

command_string := FORMAT('CREATE INDEX ON %s (id);',_job_list_name||'_donejobs');
RAISE NOTICE 'command_string %', command_string;
execute command_string;

command_string := FORMAT('select count(*) from %s',_job_list_name);
RAISE NOTICE 'command_string %', command_string;
execute command_string into num_rows;

return num_rows;
  
END;

$$ LANGUAGE plpgsql;


--SELECT topo_update.create_job_list_using_block(
--'tmp_sf_ar5_forest_input.existing_forest_surface wkb_geometry','ogc_fid',100,
--'topo_ar5_forest','ar5_forest_grense','grense',1,false,true,'topo_update.job_list_block',true);
