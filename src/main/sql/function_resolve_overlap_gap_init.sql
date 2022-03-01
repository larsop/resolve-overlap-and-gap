	-- this is internal helper function
-- this is a function that creates unlogged tables and the the grid neeed when later checking this table for overlap and gaps.

CREATE OR REPLACE FUNCTION resolve_overlap_gap_init (
_input_data resolve_overlap_data_input_type, 
_topology_info resolve_overlap_data_topology_type,
_table_name_result_prefix varchar,
_max_rows_in_each_cell int, -- this is the max number rows that intersects with box before it's split into 4 new boxes
_overlapgap_grid varchar -- The schema.table name of the grid that will be created and used to break data up in to managle pieces
)
  RETURNS INTEGER
  AS $$
DECLARE
  -- used to run commands
  command_string text;
  -- the number of cells created in the grid
  num_cells_master_grid int;
  -- drop result tables
  drop_result_tables_ boolean = TRUE;
  -- table to keep track of results
  -- the name of the content based based on the grid so each thread work on diffrent cell
  overlapgap_grid_metagrid_name varchar;
  try_with_grid_metagrid_size int;
  overlapgap_grid_metagrid_name_num_cells int;
  
  layer_centroid geometry;
  i int;
  num_metagrids_try_loop int;
  last_grid_table_size int = 0;
  next_grid_table_num int = 1;
  tmp_overlapgap_grid varchar;
  reduce_cell_by int;
  unique_id_type varchar;
  

BEGIN
  -- ############################# START # Create Topology master working schema
  -- drop schema if exists
  IF (drop_result_tables_ = TRUE AND (
    SELECT Count(*)
    FROM topology.topology
    WHERE name = (_topology_info).topology_name) = 1) THEN
    EXECUTE Format('SELECT topology.droptopology(%s)', Quote_literal((_topology_info).topology_name));
  END IF;
  -- drop this schema in case it exists
  EXECUTE Format('DROP SCHEMA IF EXISTS %s CASCADE', (_topology_info).topology_name);
  -- create topology
  EXECUTE Format('SELECT topology.createtopology(%s,%s,%s)', Quote_literal((_topology_info).topology_name), (_input_data).table_srid, (_topology_info).topology_snap_tolerance);
  -- Set unlogged to increase performance
 
  EXECUTE Format('GRANT USAGE ON SCHEMA %s TO PUBLIC', (_topology_info).topology_name);
   
  EXECUTE Format('ALTER TABLE %s.edge_data SET unlogged', (_topology_info).topology_name);
  EXECUTE Format('ALTER TABLE %s.node SET unlogged', (_topology_info).topology_name);
  EXECUTE Format('ALTER TABLE %s.face SET unlogged', (_topology_info).topology_name);
  EXECUTE Format('ALTER TABLE %s.relation SET unlogged', (_topology_info).topology_name);
  -- Create indexes
  -- This Inxdes does not seem to help since  containing_face is null;
  -- EXECUTE Format('CREATE INDEX ON %s.node(containing_face)', (_topology_info).topology_name);
  EXECUTE Format('CREATE INDEX ON %s.relation(layer_id)', (_topology_info).topology_name);
  EXECUTE Format('CREATE INDEX ON %s.relation(abs(element_id))', (_topology_info).topology_name);
  EXECUTE Format('CREATE INDEX ON %s.edge_data USING GIST (geom)', (_topology_info).topology_name);
  EXECUTE Format('CREATE INDEX ON %s.edge_data(abs_next_left_edge)', (_topology_info).topology_name);
  EXECUTE Format('CREATE INDEX ON %s.edge_data(abs_next_right_edge)', (_topology_info).topology_name);
  EXECUTE Format('CREATE INDEX ON %s.relation(element_id)', (_topology_info).topology_name);
  EXECUTE Format('CREATE INDEX ON %s.relation(topogeo_id)', (_topology_info).topology_name);
  -- ----------------------------- DONE - Create Topology master working schema
  -- TODO find out what to do with help tables, they are now created in src/main/extern_pgtopo_update_sql/help_tables_for_logging.sql
  -- TODO what to do with /Users/lop/dev/git/topologi/skog/src/main/sql/table_border_line_segments.sql
  -- ############################# START # Handle content based grid init
  -- drop content based grid table if exits
  IF (drop_result_tables_ = TRUE) THEN
    EXECUTE Format('DROP TABLE IF EXISTS %s', _overlapgap_grid);
  END IF;
  -- create a content based grid table for input data
  EXECUTE Format('CREATE TABLE %s( id serial, %s geometry(Geometry,%s))', _overlapgap_grid, (_input_data).polygon_table_geo_collumn, (_input_data).table_srid);
  command_string := Format('INSERT INTO %s(%s) 
 	SELECT q_grid.cell::Geometry(geometry,%s)  as %s 
 	from (
 	select(st_dump(
 	cbg_content_based_balanced_grid(array[ %s],%s))
 	).geom as cell) as q_grid', _overlapgap_grid, (_input_data).polygon_table_geo_collumn, (_input_data).table_srid, (_input_data).polygon_table_geo_collumn, Quote_literal((_input_data).polygon_table_name || ' ' || (_input_data).polygon_table_geo_collumn)::Text, _max_rows_in_each_cell);
  -- execute the sql command
  EXECUTE command_string;
  -- count number of cells in grid
  command_string := Format('SELECT count(*) from %s', _overlapgap_grid);
  -- execute the sql command
  EXECUTE command_string INTO num_cells_master_grid;
  -- Create Index
  EXECUTE Format('CREATE INDEX ON %s USING GIST (%s)', _overlapgap_grid, (_input_data).polygon_table_geo_collumn);
  
  -- Create a second grid for each thread
  -- TODO move to init_job ??
  -- create a content based grid table for input data
  -- TODO Need to find out how to handle meta grids  
  reduce_cell_by := 25;
  
  -- TODO find out what value to use here ????
  IF num_cells_master_grid < 20 THEN
      num_metagrids_try_loop = 1;
      try_with_grid_metagrid_size = num_cells_master_grid;
  ELSE
      num_metagrids_try_loop := 10;
      try_with_grid_metagrid_size = 20;
  END IF;
  
  
  tmp_overlapgap_grid := _overlapgap_grid;
  

  FOR i IN 1..num_metagrids_try_loop LOOP
    overlapgap_grid_metagrid_name := _overlapgap_grid||'_metagrid_'||to_char(next_grid_table_num, 'fm0000'); 
   
    IF i > 1 THEN 
      try_with_grid_metagrid_size := try_with_grid_metagrid_size-1;
    END IF;

    RAISE NOTICE 'try_with_grid_metagrid_size: %', try_with_grid_metagrid_size;
    
    if try_with_grid_metagrid_size < 4 THEN
      try_with_grid_metagrid_size := 4;
    END IF;
   
    EXECUTE Format('CREATE TABLE %s( id serial, %s geometry(Geometry,%s))', 
    overlapgap_grid_metagrid_name, (_input_data).polygon_table_geo_collumn, (_input_data).table_srid);
    command_string := Format('INSERT INTO %s(%s) 
     SELECT q_grid.cell::Geometry(geometry,%s) as %s
     from (
     select(st_dump(
     cbg_content_based_balanced_grid(array[ %L],%s))
     ).geom as cell) as q_grid', 
     overlapgap_grid_metagrid_name, (_input_data).polygon_table_geo_collumn, (_input_data).table_srid, (_input_data).polygon_table_geo_collumn, 
     tmp_overlapgap_grid || ' '|| (_input_data).polygon_table_geo_collumn, try_with_grid_metagrid_size);
    -- execute the sql command
    EXECUTE command_string;
    -- count number of cells in grid
    command_string := Format('SELECT count(*) from %s', overlapgap_grid_metagrid_name);
    -- execute the sql command
    EXECUTE command_string INTO overlapgap_grid_metagrid_name_num_cells;
    
    
    IF last_grid_table_size = overlapgap_grid_metagrid_name_num_cells THEN
      EXECUTE Format('DROP TABLE %s', overlapgap_grid_metagrid_name);
    ELSE
      EXECUTE Format('UPDATE %s set %s = ST_Buffer(%s,-%s)', 
      overlapgap_grid_metagrid_name, (_input_data).polygon_table_geo_collumn, (_input_data).polygon_table_geo_collumn, (_topology_info).topology_snap_tolerance);
      
      -- Create Index
      EXECUTE Format('CREATE INDEX ON %s USING GIST (%s)', overlapgap_grid_metagrid_name, (_input_data).polygon_table_geo_collumn);
    
      -- Create a table of grid_metagrid_01 lines
      EXECUTE Format('CREATE TABLE %1$s( id serial, %2$s geometry(Geometry,%3$s))', 
      overlapgap_grid_metagrid_name||'_lines', 
      (_input_data).polygon_table_geo_collumn, 
      (_input_data).table_srid);
      
      command_string := Format('INSERT INTO %1$s(%2$s) 
      SELECT distinct (ST_Dump(topo_update.get_single_lineparts(ST_Boundary(%2$s)))).geom as %2$s
      from %3$s', 
      overlapgap_grid_metagrid_name||'_lines', 
      (_input_data).polygon_table_geo_collumn, 
      overlapgap_grid_metagrid_name);
      EXECUTE command_string;
      -- Create Index
      EXECUTE Format('CREATE INDEX ON %s USING GIST (%s)', overlapgap_grid_metagrid_name||'_lines', (_input_data).polygon_table_geo_collumn);
 
      next_grid_table_num := next_grid_table_num + 1;
      last_grid_table_size := overlapgap_grid_metagrid_name_num_cells;
      tmp_overlapgap_grid := overlapgap_grid_metagrid_name; 
    END IF;

    


        --  Will not be used any more, may removed it ???
    IF i = 1 THEN
      EXECUTE Format('ALTER TABLE %s ADD column inside_cell boolean default false', _overlapgap_grid);
      EXECUTE Format('UPDATE %s g SET inside_cell = true from %s t where ST_covers(t.%s,g.%s)', 
      _overlapgap_grid,overlapgap_grid_metagrid_name,(_input_data).polygon_table_geo_collumn,(_input_data).polygon_table_geo_collumn,(_input_data).polygon_table_geo_collumn);
    
      EXECUTE Format('ALTER TABLE %s ADD column grid_thread_cell int default 0', _overlapgap_grid);
      EXECUTE Format('UPDATE %s g SET grid_thread_cell = t.id from %s t where ST_Intersects(t.%s,g.%s)', 
      _overlapgap_grid,overlapgap_grid_metagrid_name,(_input_data).polygon_table_geo_collumn,(_input_data).polygon_table_geo_collumn,(_input_data).polygon_table_geo_collumn);
    END IF;

    EXIT WHEN overlapgap_grid_metagrid_name_num_cells < 4 or try_with_grid_metagrid_size < 4;
    
    


  END LOOP;
   

  
  EXECUTE Format('ALTER TABLE %s ADD column num_polygons int default 0', _overlapgap_grid);
  EXECUTE Format('UPDATE %s g SET num_polygons = r.num_polygons FROM 
  (select count(t.*) as num_polygons, g.id from %s t, %s g where t.%s && g.%s group by g.id) as r
  where r.id = g.id', 
  _overlapgap_grid,(_input_data).polygon_table_name,_overlapgap_grid,(_input_data).polygon_table_geo_collumn,(_input_data).polygon_table_geo_collumn);

      -- find centroid
  EXECUTE Format('SELECT ST_Centroid(ST_Union(%s)) from %s', (_input_data).polygon_table_geo_collumn, _overlapgap_grid) into layer_centroid;

  EXECUTE Format('ALTER TABLE %s ADD column row_number int default 0', _overlapgap_grid);

  EXECUTE Format('UPDATE %s g SET row_number = r.row_number FROM 
  (select id,  
  ROW_NUMBER()  OVER (PARTITION BY grid_thread_cell 
  order by ST_distance(%s,%L) desc) 
  from %s) as r
  where r.id = g.id', 
  _overlapgap_grid,(_input_data).polygon_table_geo_collumn, layer_centroid,_overlapgap_grid);


  
  -- ----------------------------- DONE - Handle content based grid init
  
  -- ----------------------------- Create help tables
  -- TOOD find out how to handle log tables used for debug

EXECUTE Format('CREATE TABLE %s (
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
)',_table_name_result_prefix||'_no_cut_line_failed',(_input_data).table_srid);


EXECUTE Format('CREATE UNLOGGED TABLE %s (
  id serial PRIMARY KEY NOT NULL, log_time timestamp DEFAULT Now(), execute_time real, info text,
  geo Geometry(LineString, %s)
)',_table_name_result_prefix||'_long_time_logl',(_input_data).table_srid);

EXECUTE Format('CREATE UNLOGGED TABLE %s (
  id serial PRIMARY KEY NOT NULL, log_time timestamp DEFAULT Now(), execute_time real, info text,
  sql text, geo Geometry(Polygon, %s)
)',_table_name_result_prefix||'_long_time_log2',(_input_data).table_srid);

EXECUTE Format('CREATE UNLOGGED TABLE %s (
  id serial PRIMARY KEY NOT NULL, log_time timestamp DEFAULT Now(), added_to_master boolean default false, geo Geometry(LineString, %s), point_geo Geometry(Point, %s)
)',_table_name_result_prefix||'_border_line_segments',(_input_data).table_srid,(_input_data).table_srid);

EXECUTE Format('ALTER TABLE %s ADD column column_data_as_json jsonb',_table_name_result_prefix||'_border_line_segments',unique_id_type);



EXECUTE Format('CREATE INDEX ON %s USING GIST (%s)', _table_name_result_prefix||'_border_line_segments', 'geo');

EXECUTE Format('CREATE INDEX ON %s(%s)', _table_name_result_prefix||'_border_line_segments', 'added_to_master');


EXECUTE Format('CREATE UNLOGGED TABLE %s (
  id serial PRIMARY KEY NOT NULL, log_time timestamp DEFAULT Now(),added_to_master boolean default false, geo Geometry(LineString, %s)
)',_table_name_result_prefix||'_border_line_many_points',(_input_data).table_srid,(_input_data).table_srid);
EXECUTE Format('ALTER TABLE %s ADD column column_data_as_json jsonb',_table_name_result_prefix||'_border_line_many_points',unique_id_type);

-- Create the simple feature result table  as copy of the input table
EXECUTE Format('CREATE UNLOGGED TABLE %s AS TABLE %s with NO DATA',_table_name_result_prefix||'_result',(_input_data).polygon_table_name);

  -- Add an extra column to hold a list of other intersections surfaces
EXECUTE Format('SELECT vsr_get_data_type(%L,%L)',(_input_data).polygon_table_name,(_input_data).polygon_table_pk_column) into unique_id_type;
EXECUTE Format('ALTER TABLE %s ADD column _other_intersect_id_list %s[]',_table_name_result_prefix||'_result',unique_id_type);

-- Add an extra column to hold info add 
EXECUTE Format('ALTER TABLE %s ADD column _input_geo_is_valid boolean',_table_name_result_prefix||'_result');

IF (_topology_info).create_topology_attrbute_tables = true THEN
	IF(_input_data).line_table_name is not null THEN
	  EXECUTE Format('CREATE UNLOGGED TABLE %s(%s) ',(_topology_info).topology_name||'.edge_attributes',(_input_data).line_table_other_collumns_def);
	  EXECUTE Format('SELECT topology.AddTopoGeometryColumn(%L, %L, %L, %L, %L)',
	  (_topology_info).topology_name, (_topology_info).topology_name,'edge_attributes',(_input_data).line_table_geo_collumn,'LINESTRING');
	ELSE 
	  -- TODO REMOVE HACK when we find out how to do this
	  EXECUTE Format('CREATE UNLOGGED TABLE %s(%s) ',(_topology_info).topology_name||'.edge_attributes','id serial primary key,id_test integer');
	  EXECUTE Format('SELECT topology.AddTopoGeometryColumn(%L, %L, %L, %L, %L)',
	  (_topology_info).topology_name, (_topology_info).topology_name,'edge_attributes',(_input_data).line_table_geo_collumn,'LINESTRING');
	END IF;

  EXECUTE Format('CREATE UNLOGGED TABLE %s(%s) ',(_topology_info).topology_name||'.face_attributes',(_input_data).polygon_table_other_collumns_def);
  EXECUTE Format('SELECT topology.AddTopoGeometryColumn(%L, %L, %L, %L, %L)',
  (_topology_info).topology_name, (_topology_info).topology_name,'face_attributes',(_input_data).polygon_table_geo_collumn,'POLYGON');
	  
END IF;






  RETURN num_cells_master_grid;
END;
$$
LANGUAGE plpgsql;
