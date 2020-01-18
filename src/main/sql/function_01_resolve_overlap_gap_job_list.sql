-- This is the main funtion used resolve overlap and gap
DROP FUNCTION IF EXISTS resolve_overlap_gap_job_list (table_to_resolve_ varchar, -- The table to resolve
  geo_collumn_name_ varchar, -- the name of geometry column on the table to analyze
  srid_ int, -- the srid for the given geo column on the table analyze
  overlapgap_grid_ varchar, -- the name of the content based grid table
  topology_name_ varchar, -- The topology schema name where we store store sufaces and lines from the simple feature dataset. -- NB. Any exting data will related to topology_name will be deleted
  job_list_name_ varchar, -- the name of job_list table, this table is ued to track of done jobs
  input_table_pk_column_name_ varchar, -- the nam eof the promary collum
  simplify_tolerance_ double precision, -- the tolerance to be used when creating topolayer
  snap_tolerance_ double precision, -- the tolrence to be used when add data
  do_chaikins_ boolean, -- simlyfy lines by using chaikins and simlify
  inside_cell_data_ boolean -- add lines inside cell, or boderlines
);

CREATE OR REPLACE FUNCTION resolve_overlap_gap_job_list (table_to_resolve_ varchar, -- The table to resolve
geo_collumn_name_ varchar, -- the name of geometry column on the table to analyze
srid_ int, -- the srid for the given geo column on the table analyze
overlapgap_grid_ varchar, -- the name of the content based grid table
topology_name_ varchar, -- The topology schema name where we store store sufaces and lines from the simple feature dataset. -- NB. Any exting data will related to topology_name will be deleted
job_list_name_ varchar, -- the name of job_list table, this table is ued to track of done jobs
input_table_pk_column_name_ varchar, -- the nam eof the promary collum
simplify_tolerance_ double precision, -- the tolerance to be used when creating topolayer
snap_tolerance_ double precision, -- the tolrence to be used when add data
do_chaikins_ boolean, -- simlyfy lines by using chaikins and simlify
inside_cell_data_ boolean -- add lines inside cell, or boderlines
)
  RETURNS void
  AS $$
DECLARE
  command_string text;
  -- the number of cells
  num_cells int;
  -- just to create sql
  command_string_var varchar;
  -- the sql used for blocking cells
  sql_to_block_cmd varchar;
BEGIN
  -- ############################# START # create jobList tables
  command_string := Format('DROP table if exists %s', job_list_name_);
  RAISE NOTICE 'command_string %', command_string;
  EXECUTE command_string;
  -- TODO handle SRID
  command_string := Format('CREATE unlogged table %s(id serial, start_time timestamp with time zone, sql_to_block varchar, sql_to_run varchar, cell_geo geometry(geometry,4258),block_bb Geometry(geometry,4258))', job_list_name_);
  RAISE NOTICE 'command_string %', command_string;
  EXECUTE command_string;
  -- create a table for don jobs
  command_string := Format('DROP table if exists %s', job_list_name_ || '_donejobs');
  RAISE NOTICE 'command_string %', command_string;
  EXECUTE command_string;
  command_string := Format('CREATE unlogged table %s(id int, done_time timestamp with time zone default clock_timestamp())', job_list_name_ || '_donejobs');
  RAISE NOTICE 'command_string %', command_string;
  EXECUTE command_string;
  sql_to_block_cmd := Format('select topo_update.set_blocked_area(%s,%s,%s,%s,', Quote_literal(table_to_resolve_), Quote_literal(geo_collumn_name_), Quote_literal(input_table_pk_column_name_), Quote_literal(job_list_name_));
  command_string_var := Format('SELECT topo_update.simplefeature_c2_topo_surface_border_retry(%s,%s,%s,%s,%s,%s,%s,%s,', Quote_literal(table_to_resolve_), Quote_literal(geo_collumn_name_), Quote_literal(input_table_pk_column_name_), Quote_literal(topology_name_), simplify_tolerance_, snap_tolerance_, Quote_literal(do_chaikins_), Quote_literal(job_list_name_));
  RAISE NOTICE 'command_string_var %', command_string_var;
  -- add inside cell polygons
  -- TODO solve how to find r.geom
  command_string := Format('
 	INSERT INTO %s(sql_to_run,cell_geo,sql_to_block) 
 	SELECT %s||quote_literal(r.geom::Varchar)||%s as sql_to_run, r.geom as cell_geo, %s||quote_literal(r.geom::Varchar)||%s as sql_to_block
 	from %s r', job_list_name_, Quote_literal(command_string_var), Quote_literal(',' || inside_cell_data_ || ');'), Quote_literal(sql_to_block_cmd), Quote_literal(');'), overlapgap_grid_);
  RAISE NOTICE 'command_string %', command_string;
  EXECUTE command_string;
  command_string := Format('CREATE INDEX ON %s USING GIST (cell_geo);', job_list_name_);
  RAISE NOTICE 'command_string %', command_string;
  EXECUTE command_string;
  command_string := Format('CREATE INDEX ON %s USING GIST (block_bb);', job_list_name_);
  RAISE NOTICE 'command_string %', command_string;
  EXECUTE command_string;
  command_string := Format('CREATE INDEX ON %s (id);', job_list_name_);
  RAISE NOTICE 'command_string %', command_string;
  EXECUTE command_string;
  command_string := Format('CREATE INDEX ON %s (id);', job_list_name_ || '_donejobs');
  RAISE NOTICE 'command_string %', command_string;
  EXECUTE command_string;
END;
$$
LANGUAGE plpgsql;

;

GRANT EXECUTE ON FUNCTION resolve_overlap_gap_job_list (table_to_resolve_ varchar, -- The table to resolve
  geo_collumn_name_ varchar, -- the name of geometry column on the table to analyze
  srid_ int, -- the srid for the given geo column on the table analyze
  overlapgap_grid_ varchar, -- the name of the content based grid table
  topology_name_ varchar, -- The topology schema name where we store store sufaces and lines from the simple feature dataset. -- NB. Any exting data will related to topology_name will be deleted
  job_list_name_ varchar, -- the name of job_list table, this table is ued to track of done jobs
  input_table_pk_column_name_ varchar, -- the nam eof the promary collum
  simplify_tolerance_ double precision, -- the tolerance to be used when creating topolayer
  snap_tolerance_ double precision, -- the tolrence to be used when add data
  do_chaikins_ boolean, -- simlyfy lines by using chaikins and simlify
  inside_cell_data_ boolean -- add lines inside cell, or boderlines
) TO public;

