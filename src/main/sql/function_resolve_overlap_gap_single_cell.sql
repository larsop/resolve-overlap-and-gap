DROP FUNCTION IF EXISTS resolve_overlap_gap_single_cell (table_to_analyze_ varchar, -- The table to analyze
  geo_collumn_name_ varchar, -- the name of geometry column on the table to analyze
  srid_ int, -- the srid for the given geo column on the table analyze
  table_name_result_prefix_ varchar, -- This is the prefix used for the result tables
  this_list_id int, num_cells int);

CREATE OR REPLACE FUNCTION resolve_overlap_gap_single_cell (table_to_analyze_ varchar, -- The table to analyze
geo_collumn_name_ varchar, -- the name of geometry column on the table to analyze
srid_ int, -- the srid for the given geo column on the table analyze
table_name_result_prefix_ varchar, -- This is the prefix used for the result tables
this_list_id int, num_cells int)
  RETURNS text
  AS $$
DECLARE
  command_string text;
  num_rows_data int;
  num_rows_overlap int;
  num_rows_gap int;
  num_rows_overlap_area int;
  num_rows_gap_area int;
  id_list_tmp int[];
  overlapgap_overlap varchar = table_name_result_prefix_ || '_overlap';
  -- The schema.table name for the overlap/intersects found in each cell
  overlapgap_gap varchar = table_name_result_prefix_ || '_gap';
  -- The schema.table name for the gaps/holes found in each cell
  overlapgap_grid varchar = table_name_result_prefix_ || '_grid';
  -- The schema.table name of the grid that will be created and used to break data up in to managle pieces
  overlapgap_boundery varchar = table_name_result_prefix_ || '_boundery';
  -- The schema.table name the outer boundery of the data found in each cell
BEGIN
  RETURN 'num_rows_overlap:' || num_rows_overlap || ', num_rows_gap:' || num_rows_gap;
END;
$$
LANGUAGE plpgsql
PARALLEL SAFE
COST 1;

GRANT EXECUTE ON FUNCTION resolve_overlap_gap_single_cell (table_to_analyze_ varchar, -- The table to analyze
  geo_collumn_name_ varchar, -- the name of geometry column on the table to analyze
  srid_ int, -- the srid for the given geo column on the table analyze
  table_name_result_prefix_ varchar, -- This is the prefix used for the result tables
  this_list_id int, num_cells int) TO public;

