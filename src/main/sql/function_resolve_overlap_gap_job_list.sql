-- This is the main funtion used resolve overlap and gap
CREATE OR REPLACE FUNCTION resolve_overlap_gap_job_list (
_table_to_resolve varchar, -- The table to resolve
_geo_collumn_name varchar, -- the name of geometry column on the table to analyze
_srid int, -- the srid for the given geo column on the table analyze
_utm boolean, 
_overlapgap_grid varchar, -- the name of the content based grid table
_table_name_result_prefix varchar,
_topology_name varchar, -- The topology schema name where we store store sufaces and lines from the simple feature dataset. -- NB. Any exting data will related to topology_name will be deleted
_topology_snap_tolerance float, -- the tolrence to be used when add data
_job_list_name varchar, -- the name of job_list table, this table is ued to track of done jobs
_input_table_pk_column_name varchar, -- the nam eof the promary collum
_clean_info resolve_overlap_data_clean_type, -- different parameters used if need to clean up your data
--(_clean_info).simplify_tolerance float, -- is this is more than zero simply will called with
--(_clean_info).do_chaikins boolean, -- here we will use chaikins togehter with simply to smooth lines
--(_clean_info).min_area_to_keep float, -- if this a polygon  is below this limit it will merge into a neighbour polygon. The area is sqare meter. 
_max_parallel_jobs int, -- this is the max number of paralell jobs to run. There must be at least the same number of free connections
_cell_job_type int,-- add lines 1 inside cell, 2 boderlines, 3 exract simple,
_loop_number int
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
  
  -- This is used to sure that no lines can snap to each other between two cells
  -- The size wil the this value multiplied by _topology_snap_tolerance;
  -- TODO make this as parameter
  cell_boundary_tolerance_with_multi real = 12;
  
  job_list_row_count int;
  
BEGIN
  -- ############################# START # create jobList tables
  command_string := Format('DROP table if exists %s', _job_list_name);
  RAISE NOTICE 'command_string %', command_string;
  EXECUTE command_string;
  command_string := Format('CREATE unlogged table %s(id serial, start_time timestamp with time zone, inside_cell boolean, grid_thread_cell int, num_polygons int, row_number int, sql_to_block varchar, sql_to_run varchar, cell_geo geometry(geometry,%s),block_bb Geometry(geometry,%s), blocked_by_id int, worker_id int)',
  _job_list_name,_srid,_srid);
  RAISE NOTICE 'command_string %', command_string;
  EXECUTE command_string;
  -- create a table for don jobs
  command_string := Format('DROP table if exists %s', _job_list_name || '_donejobs');
  RAISE NOTICE 'command_string %', command_string;
  EXECUTE command_string;
  command_string := Format('CREATE unlogged table %s(id int, done_time timestamp with time zone default clock_timestamp(), analyze_time timestamp with time zone )', 
  _job_list_name || '_donejobs');
  RAISE NOTICE 'command_string %', command_string;
  EXECUTE command_string;


  sql_to_run_grid := Format('CALL resolve_overlap_gap_single_cell(
  %s,%s,%s,%s,
  %s,%s,%s,%s,
  %L,
  %s,%s,', 
  Quote_literal(_table_to_resolve), 
  Quote_literal(_geo_collumn_name), 
  Quote_literal(_input_table_pk_column_name), 
  Quote_literal(_table_name_result_prefix), 
  Quote_literal(_topology_name),
  _topology_snap_tolerance, 
  _srid, 
  Quote_literal(_utm), 
  _clean_info,
  Quote_literal(_job_list_name), 
  Quote_literal(_overlapgap_grid));
  RAISE NOTICE 'sql_to_run_grid %', sql_to_run_grid;

  sql_to_block_cmd := Format('select resolve_overlap_gap_block_cell(%s,%s,%s,%s,', 
  Quote_literal(_table_to_resolve), Quote_literal(_geo_collumn_name), Quote_literal(_input_table_pk_column_name), Quote_literal(_job_list_name));
  
  
  -- make       
 -- select ST_asText( ST_Expand ( (ST_Dump( get_single_lineparts)).geom, 0000.1)) from  topo_update.get_single_lineparts(ST_ExteriorRing('0103000020E964000001000000050000000000000060E30641000000005ACC5A410000000060E30641000000008FD85A4100000000006A0841000000008FD85A4100000000006A0841000000005ACC5A410000000060E30641000000005ACC5A41'));      

--  select ST_asText( ST_Expand ( (ST_Dump( get_single_lineparts)).geom, 0000.1)) 
--  from  topo_update.get_single_lineparts(ST_ExteriorRing(_geo_collumn_name));      


	 
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
  IF _cell_job_type = 4 or _cell_job_type = 5 THEN 
    command_string := Format('
 	INSERT INTO %s(inside_cell,grid_thread_cell,num_polygons,row_number,sql_to_run,cell_geo,sql_to_block) 
 	SELECT
    true as inside_cell, 
    r.grid_thread_cell,
    r.num_polygons,
    r.row_number,
    %s||quote_literal(r.'||_geo_collumn_name||'::Varchar)||%s as sql_to_run, 
    r.'||_geo_collumn_name||' as cell_geo, 
    %s||quote_literal(r.'||_geo_collumn_name||'::Varchar)||%s as sql_to_block
 	from (
    select 
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
 	_job_list_name, 
 	Quote_literal(sql_to_run_grid), 
 	Quote_literal(',' || _cell_job_type || ','), 
 	Quote_literal(sql_to_block_cmd), 
 	Quote_literal(');'),
 	
    _geo_collumn_name,
    _topology_snap_tolerance * cell_boundary_tolerance_with_multi,
    _geo_collumn_name,
    _overlapgap_grid,
 	_overlapgap_grid,
 	_geo_collumn_name);
    EXECUTE command_string;
  
  ELSIF _cell_job_type = 2 THEN 
    command_string := Format('
 	INSERT INTO %s(inside_cell,grid_thread_cell,num_polygons,row_number,sql_to_run,cell_geo,sql_to_block) 
 	SELECT
    false inside_cell, 
    0 grid_thread_cell,
    0 num_polygons,
    0 row_number,
    %s||quote_literal(r.'||_geo_collumn_name||'::Varchar)||%s as sql_to_run, 
    r.'||_geo_collumn_name||' as cell_geo, 
    %s||quote_literal(r.'||_geo_collumn_name||'::Varchar)||%s as sql_to_block
 	from %s r', 
 	_job_list_name, 
 	Quote_literal(sql_to_run_grid), 
 	Quote_literal(',' || _cell_job_type || ','), 
 	Quote_literal(sql_to_block_cmd), 
 	Quote_literal(');'), 
 	_overlapgap_grid||'_metagrid_'||to_char(1, 'fm0000'));
 	
 	RAISE NOTICE 'command_string %', command_string;
    EXECUTE command_string;

  ELSE
    command_string := Format('
 	INSERT INTO %s(inside_cell,grid_thread_cell,num_polygons,row_number,sql_to_run,cell_geo,sql_to_block) 
 	SELECT
    r.inside_cell, 
    r.grid_thread_cell,
    r.num_polygons,
    r.row_number,
    %s||quote_literal(r.'||_geo_collumn_name||'::Varchar)||%s as sql_to_run, 
    r.'||_geo_collumn_name||' as cell_geo, 
    %s||quote_literal(r.'||_geo_collumn_name||'::Varchar)||%s as sql_to_block
 	from %s r', 
 	_job_list_name, 
 	Quote_literal(sql_to_run_grid), 
 	Quote_literal(',' || _cell_job_type || ','), 
 	Quote_literal(sql_to_block_cmd), 
 	Quote_literal(');'), 
 	_overlapgap_grid);
 	
 	RAISE NOTICE 'command_string %', command_string;
    EXECUTE command_string;

  END IF;

 
  GET DIAGNOSTICS job_list_row_count = ROW_COUNT;
  
  RAISE NOTICE 'Created joblist  %s with %s rows for cell_job_type %s ', _job_list_name, job_list_row_count, _cell_job_type ;
 
    
  EXECUTE Format('CREATE INDEX ON %s USING GIST (cell_geo);', _job_list_name);
  EXECUTE Format('CREATE INDEX ON %s USING GIST (block_bb);', _job_list_name);
  EXECUTE Format('CREATE INDEX ON %s (id);', _job_list_name);
  EXECUTE Format('CREATE INDEX ON %s (num_polygons);', _job_list_name);
  EXECUTE Format('CREATE INDEX ON %s (inside_cell);', _job_list_name);
  EXECUTE Format('CREATE INDEX ON %s (id);', _job_list_name || '_donejobs');

  EXECUTE Format('UPDATE %1$s g SET worker_id = MOD((id-1),%2$s) + 1', _job_list_name, _max_parallel_jobs);

  IF _cell_job_type = 4 or _cell_job_type = 5 THEN 
    command_string := Format('UPDATE %1$s u
    SET num_polygons = r1.num_polygons
    FROM 
    (
    SELECT count(*) num_polygons, a1.id 
    FROM 
    %1$s a1,
    %2$s a2
    WHERE a1.cell_geo && a2.%3$s
    GROUP BY a1.id
    ) r1
    WHERE r1.id = u.id', 
    _job_list_name, _table_name_result_prefix||'_border_line_segments', 'geo');
    RAISE NOTICE 'command_string %', command_string;
    EXECUTE command_string;
    
      
    EXECUTE Format('UPDATE %1$s g SET inside_cell = false 
    from 
    %4$s as t
    where ST_Intersects(g.cell_geo,t.%3$s)', 
    _job_list_name,
    _overlapgap_grid||'_metagrid_'||to_char(1, 'fm0000'),
    _geo_collumn_name, 
    _overlapgap_grid||'_metagrid_'||to_char(1, 'fm0000')||'_lines');
  
  END IF;

END;
$$
LANGUAGE plpgsql;

