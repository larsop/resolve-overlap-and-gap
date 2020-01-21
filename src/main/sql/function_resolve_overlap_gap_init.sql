-- this is internal helper function
-- this is a function that creates unlogged tables and the the grid neeed when later checking this table for overlap and gaps.

CREATE OR REPLACE FUNCTION resolve_overlap_gap_init (
_table_to_resolve varchar, -- The schema.table name with polygons to analyze for gaps and intersects
_geo_collumn_name varchar, -- the name of geometry column on the table to analyze
_srid int, -- the srid for the given geo column on the table analyze
_max_rows_in_each_cell int, -- this is the max number rows that intersects with box before it's split into 4 new boxes
_overlapgap_grid varchar, -- The schema.table name of the grid that will be created and used to break data up in to managle pieces
_topology_schema_name varchar, -- The topology schema name where we store store result sufaces and lines from the simple feature dataset,
_snap_tolerance double precision
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
  EXECUTE Format('SELECT topology.createtopology(%s,%s,%s)', Quote_literal(_topology_schema_name), _srid, _snap_tolerance);
  -- Set unlogged to increase performance
  EXECUTE Format('ALTER TABLE %s.edge_data SET unlogged', _topology_schema_name);
  EXECUTE Format('ALTER TABLE %s.node SET unlogged', _topology_schema_name);
  EXECUTE Format('ALTER TABLE %s.face SET unlogged', _topology_schema_name);
  EXECUTE Format('ALTER TABLE %s.relation SET unlogged', _topology_schema_name);
  -- Create indexes
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
  -- ----------------------------- DONE - Handle content based grid init
  
  -- ----------------------------- Create help tables
  -- TOOD find out how to handle log tables used for debug
--DROP TABLE IF EXISTS topo_update.no_cut_line_failed;
-- This is a list of lines that fails
-- this is used for debug

EXECUTE Format('CREATE UNLOGGED TABLE %s.no_cut_line_failed (
  id serial PRIMARY KEY NOT NULL, log_time timestamp DEFAULT Now(), error_info text, geo Geometry(LineString, %s)
)',_topology_schema_name,_srid);

--DROP TABLE IF EXISTS  topo_update.long_time_log1;
-- This is a list of lines that fails
-- this is used for debug

EXECUTE Format('CREATE UNLOGGED TABLE %s.long_time_log1 (
  id serial PRIMARY KEY NOT NULL, log_time timestamp DEFAULT Now(), execute_time real, info text,
  geo Geometry(LineString, %s)
)',_topology_schema_name,_srid);

--DROP TABLE IF EXISTS  topo_update.long_time_log2;
-- This is a list of lines that fails
-- this is used for debug

EXECUTE Format('CREATE UNLOGGED TABLE %s.long_time_log2 (
  id serial PRIMARY KEY NOT NULL, log_time timestamp DEFAULT Now(), execute_time real, info text,
  sql text, geo Geometry(Polygon, %s)
)',_topology_schema_name,_srid);

--DROP TABLE IF EXISTS  topo_update.border_line_segments;
EXECUTE Format('CREATE UNLOGGED TABLE %s.border_line_segments (
  id serial PRIMARY KEY NOT NULL, log_time timestamp DEFAULT Now(), geo Geometry(LineString, %s), point_geo Geometry(Point, %s)
)',_topology_schema_name,_srid,_srid);



  RETURN num_cells;
END;
$$
LANGUAGE plpgsql;
