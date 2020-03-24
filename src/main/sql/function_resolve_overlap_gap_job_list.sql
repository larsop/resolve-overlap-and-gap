-- This is the main funtion used resolve overlap and gap
CREATE OR REPLACE FUNCTION resolve_overlap_gap_job_list (
table_to_resolve_ varchar, -- The table to resolve
geo_collumn_name_ varchar, -- the name of geometry column on the table to analyze
_srid int, -- the srid for the given geo column on the table analyze
_utm boolean, 
overlapgap_grid_ varchar, -- the name of the content based grid table
_table_name_result_prefix varchar,
topology_name_ varchar, -- The topology schema name where we store store sufaces and lines from the simple feature dataset. -- NB. Any exting data will related to topology_name will be deleted
_topology_snap_tolerance float, -- the tolrence to be used when add data
job_list_name_ varchar, -- the name of job_list table, this table is ued to track of done jobs
input_table_pk_column_name_ varchar, -- the nam eof the promary collum
_clean_info resolve_overlap_data_clean_type, -- different parameters used if need to clean up your data
--(_clean_info).simplify_tolerance float, -- is this is more than zero simply will called with
--(_clean_info).do_chaikins boolean, -- here we will use chaikins togehter with simply to smooth lines
--(_clean_info).min_area_to_keep float, -- if this a polygon  is below this limit it will merge into a neighbour polygon. The area is sqare meter. 
_cell_job_type int -- add lines 1 inside cell, 2 boderlines, 3 exract simple
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
  -- the sql resilve simple feature data
  sql_to_run_grid varchar;
BEGIN
  -- ############################# START # create jobList tables
  command_string := Format('DROP table if exists %s', job_list_name_);
  RAISE NOTICE 'command_string %', command_string;
  EXECUTE command_string;
  command_string := Format('CREATE unlogged table %s(id serial, start_time timestamp with time zone, inside_cell boolean, grid_thread_cell int, num_polygons int, row_number int, sql_to_block varchar, sql_to_run varchar, cell_geo geometry(geometry,%s),block_bb Geometry(geometry,%s))',
  job_list_name_,_srid,_srid);
  RAISE NOTICE 'command_string %', command_string;
  EXECUTE command_string;
  -- create a table for don jobs
  command_string := Format('DROP table if exists %s', job_list_name_ || '_donejobs');
  RAISE NOTICE 'command_string %', command_string;
  EXECUTE command_string;
  command_string := Format('CREATE unlogged table %s(id int, done_time timestamp with time zone default clock_timestamp())', job_list_name_ || '_donejobs');
  RAISE NOTICE 'command_string %', command_string;
  EXECUTE command_string;


  sql_to_run_grid := Format('CALL resolve_overlap_gap_single_cell(
  %s,%s,%s,%s,
  %s,%s,%s,%s,
  %L,
  %s,%s,', 
  Quote_literal(table_to_resolve_), 
  Quote_literal(geo_collumn_name_), 
  Quote_literal(input_table_pk_column_name_), 
  Quote_literal(_table_name_result_prefix), 
  Quote_literal(topology_name_),
  _topology_snap_tolerance, 
  _srid, 
  Quote_literal(_utm), 
  _clean_info,
  Quote_literal(job_list_name_), 
  Quote_literal(overlapgap_grid_));
  RAISE NOTICE 'sql_to_run_grid %', sql_to_run_grid;

  sql_to_block_cmd := Format('select resolve_overlap_gap_block_cell(%s,%s,%s,%s,', 
  Quote_literal(table_to_resolve_), Quote_literal(geo_collumn_name_), Quote_literal(input_table_pk_column_name_), Quote_literal(job_list_name_));
  
  
  -- make       
 -- select ST_asText( ST_Expand ( (ST_Dump( get_single_lineparts)).geom, 0000.1)) from  topo_update.get_single_lineparts(ST_ExteriorRing('0103000020E964000001000000050000000000000060E30641000000005ACC5A410000000060E30641000000008FD85A4100000000006A0841000000008FD85A4100000000006A0841000000005ACC5A410000000060E30641000000005ACC5A41'));      

--  select ST_asText( ST_Expand ( (ST_Dump( get_single_lineparts)).geom, 0000.1)) 
--  from  topo_update.get_single_lineparts(ST_ExteriorRing(geo_collumn_name_));      


	 
-- select * from (
-- select 
--    r.inside_cell,
--    r.grid_thread_cell,
--    r.num_polygons,
--    r.row_number,
-- 
-- l.geom as geo , 
-- ROW_NUMBER() OVER(PARTITION BY l.geom order by r.num_polygons desc) as cell_number from
-- (
-- select distinct ST_Expand((ST_Dump(geo)).geom,0.0001) as geom from (
-- select topo_update.get_single_lineparts((ST_Dump(ST_Union(geo))).geom) as geo 
-- from (
--   select ST_ExteriorRing(geo) as geo from  test_topo_ar50_t3.ar50_utvikling_flate_grid
-- ) as r
-- ) as r
-- ) as l,
-- test_topo_ar50_t3.ar50_utvikling_flate_grid as r
-- where r.geo && l.geom
-- ) as r where cell_number= 1 order by num_polygons desc
-- ;
 
  -- add inside cell polygons
  -- TODO solve how to find r.geom
  IF _cell_job_type = 3 or _cell_job_type = 4 THEN 
    command_string := Format('
 	INSERT INTO %s(inside_cell,grid_thread_cell,num_polygons,row_number,sql_to_run,cell_geo,sql_to_block) 
 	SELECT
    r.inside_cell, 
    r.grid_thread_cell,
    r.num_polygons,
    r.row_number,
    %s||quote_literal(r.'||geo_collumn_name_||'::Varchar)||%s as sql_to_run, 
    r.'||geo_collumn_name_||' as cell_geo, 
    %s||quote_literal(r.'||geo_collumn_name_||'::Varchar)||%s as sql_to_block
 	from (
    select 
    r.inside_cell,
    r.grid_thread_cell,
    r.num_polygons,
    r.row_number,
    l.geom as %s , 
    ROW_NUMBER() OVER(PARTITION BY l.geom order by r.num_polygons desc) as cell_number from
    (
      select ST_Expand((ST_Dump(geom)).geom,%s) as geom from (
        select topo_update.get_single_lineparts((ST_Dump(ST_Union(geom))).geom) as geom 
        from (
          select ST_ExteriorRing(%s) as geom from %s
        ) as r
      ) as r
    ) as l,
    %s as r 
    where r.%s && l.geom
    ) as r WHERE r.cell_number = 1', 
 	job_list_name_, 
 	Quote_literal(sql_to_run_grid), 
 	Quote_literal(',' || _cell_job_type || ','), 
 	Quote_literal(sql_to_block_cmd), 
 	Quote_literal(');'),
 	
    geo_collumn_name_,
    _topology_snap_tolerance*12,
    geo_collumn_name_,
    overlapgap_grid_,
 	overlapgap_grid_,
 	geo_collumn_name_);
  ELSE
    command_string := Format('
 	INSERT INTO %s(inside_cell,grid_thread_cell,num_polygons,row_number,sql_to_run,cell_geo,sql_to_block) 
 	SELECT
    r.inside_cell, 
    r.grid_thread_cell,
    r.num_polygons,
    r.row_number,
    %s||quote_literal(r.'||geo_collumn_name_||'::Varchar)||%s as sql_to_run, 
    r.'||geo_collumn_name_||' as cell_geo, 
    %s||quote_literal(r.'||geo_collumn_name_||'::Varchar)||%s as sql_to_block
 	from %s r', 
 	job_list_name_, 
 	Quote_literal(sql_to_run_grid), 
 	Quote_literal(',' || _cell_job_type || ','), 
 	Quote_literal(sql_to_block_cmd), 
 	Quote_literal(');'), 
 	overlapgap_grid_);
  END IF;
  
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
  command_string := Format('CREATE INDEX ON %s (id);', job_list_name_);
  RAISE NOTICE 'command_string %', command_string;
  command_string := Format('CREATE INDEX ON %s (num_polygons);', job_list_name_);
  RAISE NOTICE 'command_string %', command_string;
  command_string := Format('CREATE INDEX ON %s (inside_cell);', job_list_name_);
  RAISE NOTICE 'command_string %', command_string;
  EXECUTE command_string;
  command_string := Format('CREATE INDEX ON %s (id);', job_list_name_ || '_donejobs');
  RAISE NOTICE 'command_string %', command_string;
  EXECUTE command_string;
END;
$$
LANGUAGE plpgsql;

