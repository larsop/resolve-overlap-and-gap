-- this is internal helper function
-- this is a function that creates unlogged tables and the the grid neeed when later checking this table for overlap and gaps.

CREATE OR REPLACE FUNCTION resolve_overlap_gap_init (
_table_name_result_prefix varchar,
_table_to_resolve varchar, -- The schema.table name with polygons to analyze for gaps and intersects
_geo_collumn_name varchar, -- the name of geometry column on the table to analyze
_srid int, -- the srid for the given geo column on the table analyze
_max_rows_in_each_cell int, -- this is the max number rows that intersects with box before it's split into 4 new boxes
_overlapgap_grid varchar, -- The schema.table name of the grid that will be created and used to break data up in to managle pieces
_topology_schema_name varchar, -- The topology schema name where we store store result sufaces and lines from the simple feature dataset,
_topology_snap_tolerance double precision
)
  RETURNS INTEGER
  AS $$
DECLARE
  -- used to run commands
  command_string text;
  -- the number of cells created in the grid
  num_cells int;
  -- drop result tables
  drop_result_tables_ boolean = TRUE;
  -- table to keep track of results
  -- the name of the content based based on the grid so each thread work on diffrent cell
  overlapgap_grid_threads varchar;
  overlapgap_grid_threads_cell_size int;
  overlapgap_grid_threads_num_cells int;
  
  layer_centroid geometry;
  
BEGIN
  -- ############################# START # Create Topology master working schema
  -- drop schema if exists
  IF (drop_result_tables_ = TRUE AND (
    SELECT Count(*)
    FROM topology.topology
    WHERE name = _topology_schema_name) = 1) THEN
    EXECUTE Format('SELECT topology.droptopology(%s)', Quote_literal(_topology_schema_name));
  END IF;
  -- drop this schema in case it exists
  EXECUTE Format('DROP SCHEMA IF EXISTS %s CASCADE', _topology_schema_name);
  -- create topology
  EXECUTE Format('SELECT topology.createtopology(%s,%s,%s)', Quote_literal(_topology_schema_name), _srid, _topology_snap_tolerance);
  -- Set unlogged to increase performance
 
  EXECUTE Format('GRANT USAGE ON SCHEMA %s TO PUBLIC', _topology_schema_name);
   
  EXECUTE Format('ALTER TABLE %s.edge_data SET unlogged', _topology_schema_name);
  EXECUTE Format('ALTER TABLE %s.node SET unlogged', _topology_schema_name);
  EXECUTE Format('ALTER TABLE %s.face SET unlogged', _topology_schema_name);
  EXECUTE Format('ALTER TABLE %s.relation SET unlogged', _topology_schema_name);
  -- Create indexes
  EXECUTE Format('CREATE INDEX ON %s.node(containing_face)', _topology_schema_name);
  EXECUTE Format('CREATE INDEX ON %s.relation(layer_id)', _topology_schema_name);
  EXECUTE Format('CREATE INDEX ON %s.relation(abs(element_id))', _topology_schema_name);
  EXECUTE Format('CREATE INDEX ON %s.edge_data USING GIST (geom)', _topology_schema_name);
  EXECUTE Format('CREATE INDEX ON %s.relation(element_id)', _topology_schema_name);
  EXECUTE Format('CREATE INDEX ON %s.relation(topogeo_id)', _topology_schema_name);
  -- ----------------------------- DONE - Create Topology master working schema
  -- TODO find out what to do with help tables, they are now created in src/main/extern_pgtopo_update_sql/help_tables_for_logging.sql
  -- TODO what to do with /Users/lop/dev/git/topologi/skog/src/main/sql/table_border_line_segments.sql
  -- ############################# START # Handle content based grid init
  -- drop content based grid table if exits
  IF (drop_result_tables_ = TRUE) THEN
    EXECUTE Format('DROP TABLE IF EXISTS %s', _overlapgap_grid);
  END IF;
  -- create a content based grid table for input data
  EXECUTE Format('CREATE TABLE %s( id serial, %s geometry(Geometry,%s))', _overlapgap_grid, _geo_collumn_name, _srid);
  command_string := Format('INSERT INTO %s(%s) 
 	SELECT q_grid.cell::Geometry(geometry,%s)  as %s 
 	from (
 	select(st_dump(
 	cbg_content_based_balanced_grid(array[ %s],%s))
 	).geom as cell) as q_grid', _overlapgap_grid, _geo_collumn_name, _srid, _geo_collumn_name, Quote_literal(_table_to_resolve || ' ' || _geo_collumn_name)::Text, _max_rows_in_each_cell);
  -- execute the sql command
  EXECUTE command_string;
  -- count number of cells in grid
  command_string := Format('SELECT count(*) from %s', _overlapgap_grid);
  -- execute the sql command
  EXECUTE command_string INTO num_cells;
  -- Create Index
  EXECUTE Format('CREATE INDEX ON %s USING GIST (%s)', _overlapgap_grid, _geo_collumn_name);
  
  -- Create a second grid for each thread
  -- TODO move to init_job ??
  -- create a content based grid table for input data
  overlapgap_grid_threads := _overlapgap_grid||'_threads'; 
  overlapgap_grid_threads_cell_size := num_cells/5;
  IF overlapgap_grid_threads_cell_size = 0 THEN
    overlapgap_grid_threads_cell_size = 1;
  END IF;
  
  EXECUTE Format('CREATE TABLE %s( id serial, %s geometry(Geometry,%s))', 
  overlapgap_grid_threads, _geo_collumn_name, _srid);
  command_string := Format('INSERT INTO %s(%s) 
 	SELECT q_grid.cell::Geometry(geometry,%s) as %s
 	from (
 	select(st_dump(
 	cbg_content_based_balanced_grid(array[ %L],%s))
 	).geom as cell) as q_grid', 
 	overlapgap_grid_threads, _geo_collumn_name, _srid, _geo_collumn_name, 
 	_overlapgap_grid || ' '|| _geo_collumn_name, overlapgap_grid_threads_cell_size);
  -- execute the sql command
  EXECUTE command_string;
  -- count number of cells in grid
  command_string := Format('SELECT count(*) from %s', overlapgap_grid_threads);
  -- execute the sql command
  EXECUTE command_string INTO overlapgap_grid_threads_num_cells;
 
  EXECUTE Format('UPDATE %s set %s = ST_Buffer(%s,-%s)', 
  overlapgap_grid_threads, _geo_collumn_name, _geo_collumn_name, _topology_snap_tolerance);
  
  -- Create Index
  EXECUTE Format('CREATE INDEX ON %s USING GIST (%s)', overlapgap_grid_threads, _geo_collumn_name);

  EXECUTE Format('ALTER TABLE %s ADD column inside_cell boolean default false', _overlapgap_grid);
  EXECUTE Format('UPDATE %s g SET inside_cell = true from %s t where ST_covers(t.%s,g.%s)', 
  _overlapgap_grid,overlapgap_grid_threads,_geo_collumn_name,_geo_collumn_name,_geo_collumn_name);

  EXECUTE Format('ALTER TABLE %s ADD column grid_thread_cell int default 0', _overlapgap_grid);
  EXECUTE Format('UPDATE %s g SET grid_thread_cell = t.id from %s t where ST_Intersects(t.%s,g.%s)', 
  _overlapgap_grid,overlapgap_grid_threads,_geo_collumn_name,_geo_collumn_name,_geo_collumn_name);

  EXECUTE Format('ALTER TABLE %s ADD column num_polygons int default 0', _overlapgap_grid);
  
  EXECUTE Format('UPDATE %s g SET num_polygons = r.num_polygons FROM 
  (select count(t.*) as num_polygons, g.id from %s t, %s g where t.%s && g.%s group by g.id) as r
  where r.id = g.id', 
  _overlapgap_grid,_table_to_resolve,_overlapgap_grid,_geo_collumn_name,_geo_collumn_name);

      -- find centroid
  EXECUTE Format('SELECT ST_Centroid(ST_Union(%s)) from %s', _geo_collumn_name, _overlapgap_grid) into layer_centroid;

  EXECUTE Format('ALTER TABLE %s ADD column row_number int default 0', _overlapgap_grid);

  EXECUTE Format('UPDATE %s g SET row_number = r.row_number FROM 
  (select id,  
  ROW_NUMBER()  OVER (PARTITION BY grid_thread_cell 
  order by ST_distance(%s,%L) desc) 
  from %s) as r
  where r.id = g.id', 
  _overlapgap_grid,_geo_collumn_name, layer_centroid,_overlapgap_grid);


  
  -- ----------------------------- DONE - Handle content based grid init
  
  -- ----------------------------- Create help tables
  -- TOOD find out how to handle log tables used for debug

EXECUTE Format('CREATE UNLOGGED TABLE %s (
  id serial PRIMARY KEY NOT NULL, 
  log_time timestamp DEFAULT Now(), 
  line_geo_lost boolean,
  error_info text, 
  d_state text,
  d_msg text,
  d_detail text,
  d_hint text,
  d_context text,
  geo Geometry(LineString, %s)
)',_table_name_result_prefix||'_no_cut_line_failed',_srid);


EXECUTE Format('CREATE UNLOGGED TABLE %s (
  id serial PRIMARY KEY NOT NULL, log_time timestamp DEFAULT Now(), execute_time real, info text,
  geo Geometry(LineString, %s)
)',_table_name_result_prefix||'_long_time_logl',_srid);

EXECUTE Format('CREATE UNLOGGED TABLE %s (
  id serial PRIMARY KEY NOT NULL, log_time timestamp DEFAULT Now(), execute_time real, info text,
  sql text, geo Geometry(Polygon, %s)
)',_table_name_result_prefix||'_long_time_log2',_srid);

EXECUTE Format('CREATE UNLOGGED TABLE %s (
  id serial PRIMARY KEY NOT NULL, log_time timestamp DEFAULT Now(), geo Geometry(LineString, %s), point_geo Geometry(Point, %s)
)',_table_name_result_prefix||'_border_line_segments',_srid,_srid);


EXECUTE Format('CREATE UNLOGGED TABLE %s (
  id serial PRIMARY KEY NOT NULL, log_time timestamp DEFAULT Now(), geo Geometry(LineString, %s)
)',_table_name_result_prefix||'_border_line_many_points',_srid,_srid);

-- Create the simple feature result table  as copy of the input table
EXECUTE Format('CREATE UNLOGGED TABLE %s AS TABLE %s with NO DATA',_table_name_result_prefix||'_result',_table_to_resolve);

-- Add an extra column to hold a list of other intersections surfaces
EXECUTE Format('ALTER TABLE %s ADD column _other_intersect_id_list int[]',_table_name_result_prefix||'_result');

-- Add an extra column to hold a list of other intersections surfaces
EXECUTE Format('GRANT select ON TABLE %s TO PUBLIC',_table_name_result_prefix||'_result');


  RETURN num_cells;
END;
$$
LANGUAGE plpgsql;
