CREATE TYPE resolve_overlap_data_input_type AS (
 table_to_resolve varchar, -- The table to resolv, imcluding schema name
 table_pk_column_name varchar, -- The primary of the input table
 table_geo_collumn_name varchar, -- the name of geometry column on the table to analyze
 table_srid int, -- the srid for the given geo column on the table analyze
 utm boolean
);


CREATE TYPE resolve_overlap_data_topology_type AS (
  topology_name varchar, -- The topology schema name where we store store sufaces and lines from the simple feature dataset and th efinal result
  -- NB. Any exting data will related to topology_name will be deleted
  topology_snap_tolerance float -- this is tolerance used as base when creating the the postgis topolayer
);


CREATE TYPE resolve_overlap_data_clean_type AS (
  min_area_to_keep float, -- if this a polygon  is below this limit it will merge into a neighbour polygon. The area is sqare meter. 
  simplify_tolerance float, -- is this is more than zero simply will called with
  do_chaikins boolean, -- here we will use chaikins togehter with simply to smooth lines
  chaikins_max_length int, --edge that are longer than this value will not be touched   
  -- The basic idea idea is to use smooth out sharp edges in another way than  
  chaikins_min_degrees int, -- The angle has to be less this given value, This is used to avoid to touch all angles. 
  chaikins_max_degrees int, -- The angle has to be greather than this given value, This is used to avoid to touch all angles 
  chaikins_nIterations int -- A big value here make no sense because the number of points will increaes exponential )
);

CREATE OR REPLACE FUNCTION resolve_overlap_data_clean_type_func(
_min_area_to_keep float default 0, -- if this a polygon  is below this limit it will merge into a neighbour polygon. The area is sqare meter.
_simplify_tolerance float default 0, -- is this is more than zero simply will called with
_do_chaikins boolean default false, -- here we will use chaikins togehter with simply to smooth lines,   -- The basic idea idea is to use smooth out sharp edges in another way than  
_chaikins_max_length int default 0, --edge that are longer than this value will not be touched   
_chaikins_min_degrees int default 0, -- The angle has to be less this given value, This is used to avoid to touch all angles. 
_chaikins_max_degrees int default 0, -- The angle has to be greather than this given value, This is used to avoid to touch all angles 
_chaikins_nIterations int default 0-- A big value here make no sense because the number of points will increaes exponential )
)
RETURNS resolve_overlap_data_clean_type
  AS $$
DECLARE
  ct resolve_overlap_data_clean_type;
BEGIN
  ct = (
    _min_area_to_keep,
    _simplify_tolerance,
    _do_chaikins,
    _chaikins_max_length,
    _chaikins_min_degrees,
    _chaikins_max_degrees,
    _chaikins_nIterations
    );
  
  return ct;
END;
$$
LANGUAGE plpgsql;

