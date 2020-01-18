-- this is internal helper function
-- this is a function that creates unlogged tables and the the grid neeed when later checking this table for overlap and gaps.

DROP FUNCTION IF EXISTS resolve_overlap_gap_init (table_to_resolve_ varchar, -- The schema.table name with polygons to analyze for gaps and intersects
  geo_collumn_name_ varchar, -- the name of geometry column on the table to analyze
  srid_ int, -- the srid for the given geo column on the table analyze
  max_rows_in_each_cell int, -- this is the max number rows that intersects with box before it's split into 4 new boxes
  overlapgap_grid_ varchar, -- The schema.table name of the grid that will be created and used to break data up in to managle pieces
  topology_schema_name_ varchar, -- The topology schema name where we store store sufaces and lines from the simple feature dataset
  snap_tolerance_ double precision);

CREATE OR REPLACE FUNCTION resolve_overlap_gap_init (table_to_resolve_ varchar, -- The schema.table name with polygons to analyze for gaps and intersects
geo_collumn_name_ varchar, -- the name of geometry column on the table to analyze
srid_ int, -- the srid for the given geo column on the table analyze
max_rows_in_each_cell int, -- this is the max number rows that intersects with box before it's split into 4 new boxes
overlapgap_grid_ varchar, -- The schema.table name of the grid that will be created and used to break data up in to managle pieces
topology_schema_name_ varchar, -- The topology schema name where we store store sufaces and lines from the simple feature dataset,
snap_tolerance_ double precision)
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
    WHERE name = topology_schema_name_) = 1) THEN
    EXECUTE Format('SELECT topology.droptopology(%s)', Quote_literal(topology_schema_name_));
  END IF;
  -- drop this schema in case it exists
  EXECUTE Format('DROP SCHEMA IF EXISTS %s CASCADE', topology_schema_name_);
  -- create topology
  EXECUTE Format('SELECT topology.createtopology(%s,%s,%s)', Quote_literal(topology_schema_name_), 4258, snap_tolerance_);
  -- Set unlogged to increase performance
  EXECUTE Format('ALTER TABLE %s.edge_data SET unlogged', topology_schema_name_);
  EXECUTE Format('ALTER TABLE %s.node SET unlogged', topology_schema_name_);
  EXECUTE Format('ALTER TABLE %s.face SET unlogged', topology_schema_name_);
  EXECUTE Format('ALTER TABLE %s.relation SET unlogged', topology_schema_name_);
  -- Create indexes
  EXECUTE Format('CREATE INDEX ON %s.relation(layer_id)', topology_schema_name_);
  EXECUTE Format('CREATE INDEX ON %s.relation(abs(element_id))', topology_schema_name_);
  EXECUTE Format('CREATE INDEX ON %s.edge_data USING GIST (geom)', topology_schema_name_);
  EXECUTE Format('CREATE INDEX ON %s.relation(element_id)', topology_schema_name_);
  EXECUTE Format('CREATE INDEX ON %s.relation(topogeo_id)', topology_schema_name_);
  -- ----------------------------- DONE - Create Topology master working schema
  -- TODO find out what to do with help tables, they are now created in src/main/extern_pgtopo_update_sql/help_tables_for_logging.sql
  -- TODO what to do with /Users/lop/dev/git/topologi/skog/src/main/sql/table_border_line_segments.sql
  -- ############################# START # Handle content based grid init
  -- drop content based grid table if exits
  IF (drop_result_tables_ = TRUE) THEN
    EXECUTE Format('DROP TABLE IF EXISTS %s', overlapgap_grid_);
  END IF;
  -- create a content based grid table for input data
  EXECUTE Format('CREATE TABLE %s( id serial, %s geometry(Geometry,%s))', overlapgap_grid_, geo_collumn_name_, srid_);
  command_string := Format('INSERT INTO %s(%s) 
 	SELECT q_grid.cell::Geometry(geometry,%s)  as %s 
 	from (
 	select(st_dump(
 	cbg_content_based_balanced_grid(array[ %s],%s))
 	).geom as cell) as q_grid', overlapgap_grid_, geo_collumn_name_, srid_, geo_collumn_name_, Quote_literal(table_to_resolve_ || ' ' || geo_collumn_name_)::Text, max_rows_in_each_cell);
  -- execute the sql command
  EXECUTE command_string;
  -- count number of cells in grid
  command_string := Format('SELECT count(*) from %s', overlapgap_grid_);
  -- execute the sql command
  EXECUTE command_string INTO num_cells;
  -- Create Index
  EXECUTE Format('CREATE INDEX ON %s USING GIST (geom)', overlapgap_grid_);
  -- ----------------------------- DONE - Handle content based grid init
  RETURN num_cells;
END;
$$
LANGUAGE plpgsql;

GRANT EXECUTE ON FUNCTION resolve_overlap_gap_init (table_to_resolve_ varchar, -- The schema.table name with polygons to analyze for gaps and intersects
  geo_collumn_name_ varchar, -- the name of geometry column on the table to analyze
  srid_ int, -- the srid for the given geo column on the table analyze
  max_rows_in_each_cell int, -- this is the max number rows that intersects with box before it's split into 4 new boxes
  overlapgap_grid_ varchar, -- The schema.table name of the grid that will be created and used to break data up in to managle pieces
  topology_schema_name_ varchar, -- The topology schema name where we store store sufaces and lines from the simple feature dataset
  snap_tolerance_ double precision) TO public;

