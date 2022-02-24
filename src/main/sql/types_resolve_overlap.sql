CREATE TYPE resolve_overlap_data_input_type AS (
 line_table_name varchar, -- The table with simple feature lines, 
 -- If this has a value then data from table will used to form all valid surfaces.
 -- this may be empty, the polygon_table_geo_collumn must of type polygon to be abale to generate a polygon layer
 
 line_table_pk_column varchar, -- A unique primary column of the line input table
 line_table_geo_collumn varchar, -- The name of geometry column for the line strings


 polygon_table_name varchar, -- The table with simple feature polygons or points attributtes to resolve, imcluding schema name
 -- If we in this tables only have a point and no polygons, the we will need a set of tables the lines also

 polygon_table_pk_column varchar, -- A unique primary column of the polygon input table

 polygon_table_geo_collumn varchar, -- the name of geometry column on the table 
 -- If the type is a point we also need a set of lines to be used as the border the polygons
 
 table_srid int, -- the srid for the given geo column on the table analyze
 utm boolean,
 
 -- This values are computed by default, mayby check if that ius needed is his ass added by the user
 -- should be on this format qms_id_grense character varying,objtype character varying,aravgrtype character varying,maalemetode character varying,noyaktighet integer,synbarhet character varying,verifiseringsdato date,datafangstdato date,kartid character varying,kjoringsident date,arkartstd character varying,opphav character varying,informasjon character varying,registreringsversjon_produkt character varying,registreringsversjon_versjon character varying,registreringsversjon_undertype character varying,qms_navnerom character varying,qms_versjonid character varying,qms_oppdateringsdato timestamp without time zone,qms_prosesshistorie character varying,qms_kopidata_omraadeid integer,qms_kopidata_originaldatavert character varying,qms_kopidata_kopidato timestamp without time zone,sl_dummy_grense_id integer
 line_table_other_collumns_def varchar,  
 -- should be on this format qms_id_grense,objtype,aravgrtype,maalemetode,noyaktighet,synbarhet,verifiseringsdato,datafangstdato,kartid,kjoringsident,arkartstd,opphav,informasjon,registreringsversjon_produkt,registreringsversjon_versjon,registreringsversjon_undertype,qms_navnerom,qms_versjonid,qms_oppdateringsdato,qms_prosesshistorie,qms_kopidata_omraadeid,qms_kopidata_originaldatavert,qms_kopidata_kopidato,sl_dummy_grense_id
 line_table_other_collumns_list varchar, 

 -- This values are computed by default, mayby check if that ius needed is his ass added by the user
 -- should be on this format qms_id_grense character varying,objtype character varying,aravgrtype character varying,maalemetode character varying,noyaktighet integer,synbarhet character varying,verifiseringsdato date,datafangstdato date,kartid character varying,kjoringsident date,arkartstd character varying,opphav character varying,informasjon character varying,registreringsversjon_produkt character varying,registreringsversjon_versjon character varying,registreringsversjon_undertype character varying,qms_navnerom character varying,qms_versjonid character varying,qms_oppdateringsdato timestamp without time zone,qms_prosesshistorie character varying,qms_kopidata_omraadeid integer,qms_kopidata_originaldatavert character varying,qms_kopidata_kopidato timestamp without time zone,sl_dummy_grense_id integer
 polygon_table_other_collumns_def varchar,  
 -- should be on this format qms_id_grense,objtype,aravgrtype,maalemetode,noyaktighet,synbarhet,verifiseringsdato,datafangstdato,kartid,kjoringsident,arkartstd,opphav,informasjon,registreringsversjon_produkt,registreringsversjon_versjon,registreringsversjon_undertype,qms_navnerom,qms_versjonid,qms_oppdateringsdato,qms_prosesshistorie,qms_kopidata_omraadeid,qms_kopidata_originaldatavert,qms_kopidata_kopidato,sl_dummy_grense_id
 polygon_table_other_collumns_list varchar
 
);


CREATE TYPE resolve_overlap_data_topology_type AS (
  topology_name varchar, -- The topology schema name where we store store sufaces and lines from the simple feature dataset and th efinal result
  -- NB. Any exting data will related to topology_name will be deleted
  topology_snap_tolerance float, -- this is tolerance used as base when creating the the postgis topolayer
  create_topology_attrbute_tables boolean, -- if this is true and we value for line_table_name we create attribute tables refferances to  
  -- this tables will have atrbuttes equal to the simple feauture tables for lines and feautures
 
  -- this is computed startup
  topology_attrbute_tables_border_layer_id int,
  -- this is computed startup
  topology_attrbute_tables_surface_layer_id int

);

-----------------------------------------------------------------------------------------------
-- resolve_based_on_attribute_type for attributes that have equal values ------------------------------
-----------------------------------------------------------------------------------------------
CREATE TYPE resolve_based_on_attribute_type AS (
attribute_resolve_list text, -- A list of attributes to resolve on this format 'attr1 attr2 attr3
attribute_min_common_border_length float, -- Min. length of common border before resolving 
attribute_max_common_area_size float -- Max area of objects to resolve   
);

CREATE OR REPLACE FUNCTION resolve_based_on_attribute_type_func(
_attribute_resolve_list text default null, -- A list of attributes to resolve  
_attribute_min_common_border_length float default 0, -- Min. length of common border before resolving 
_attribute_max_common_area_size float default 0 -- Max area of objects to resolve   
)
RETURNS resolve_based_on_attribute_type
  AS $$
DECLARE
  ct resolve_based_on_attribute_type;
BEGIN
  ct = (
    _attribute_resolve_list,
    _attribute_min_common_border_length,
    _attribute_max_common_area_size
    );
  
  return ct;
END;
$$
LANGUAGE plpgsql;


-----------------------------------------------------------------------------------------------
-- resolve_overlap_data_clean_type ------------------------------------------------------------
-----------------------------------------------------------------------------------------------
CREATE TYPE resolve_overlap_data_clean_type AS (
  -- TODO move own type
  min_area_to_keep float, -- if this a polygon  is below this limit it will merge into a neighbour polygon. The area is sqare meter. 

  resolve_based_on_attribute resolve_based_on_attribute_type, -- resolve_based_on_attribute_type for attributes that have equal values 
  
  simplify_tolerance float, -- is this is more than zero ST_simplifyPreserveTopology will called with this tolerance
  simplify_max_average_vertex_length int, -- in meter both for utm and deegrees, this used to avoid running ST_simplifyPreserveTopology for long lines lines with few points

  -- here we will use chaikins togehter with simply to smooth lines
  -- The basic idea idea is to use smooth out sharp edges in another way than  
  chaikins_nIterations int, -- -- IF 0 NO CHAKINS WILL BE DONE,  A big value here make no sense because the number of points will increaes exponential )
  chaikins_max_length int, --edge that are longer than this value will not be touched for chaikins_min_degrees or chaikins_max_degrees
  chaikins_min_degrees int, -- The angle has to be less this given value, This is used to avoid to touch all angles. 
  chaikins_max_degrees int, -- OR rhe angle has to be greather than this given value, This is used to avoid to touch all angles 
  
  -- This is used to round angles that are verry step and does not depend chaikins_max_length 
  chaikins_min_steep_angle_degrees int, -- The angle has to be less this given value, This is used to avoid to touch all angles. 
  chaikins_max_steep_angle_degrees int -- OR the angle has to be greather than this given value, This is used to avoid to touch all angles 
  
);

CREATE OR REPLACE FUNCTION resolve_overlap_data_clean_type_func(
_min_area_to_keep float default 0, -- if this a polygon  is below this limit it will merge into a neighbour polygon. The area is sqare meter.

_resolve_based_on_attribute resolve_based_on_attribute_type default null, -- resolve_based_on_attribute_type for attributes that have equal values 

_simplify_tolerance float default 0, -- is this is more than zero simply will called with
_simplify_max_average_vertex_length int default 0, -- in meter both for utm and deegrees, this used to avoid running ST_simplifyPreserveTopology for long lines lines with few points
_chaikins_nIterations int default 0, -- IF 0 NO CHAKINS WILL BE DONE,  A big value here make no sense because the number of points will increaes exponential )
_chaikins_max_length int default 0, --edge that are longer than this value will not be touched by _chaikins_min_degrees and _chaikins_max_degrees  
_chaikins_min_degrees int default 0, -- The angle has to be less this given value, This is used to avoid to touch all angles. 
_chaikins_max_degrees int default 0, -- OR the angle has to be greather than this given value, This is used to avoid to touch all angles 
_chaikins_min_steep_angle_degrees int default 0, -- The angle has to be less this given value, This is used to avoid to touch all angles. 
_chaikins_max_steep_angle_degrees int default 0-- OR The angle has to be greather than this given value, This is used to avoid to touch all angles 
)
RETURNS resolve_overlap_data_clean_type
  AS $$
DECLARE
  ct resolve_overlap_data_clean_type;
BEGIN
  ct = (
    _min_area_to_keep,
    _resolve_based_on_attribute,
    _simplify_tolerance,
    _simplify_max_average_vertex_length,
    _chaikins_nIterations,
    _chaikins_max_length,
    _chaikins_min_degrees,
    _chaikins_max_degrees,
    _chaikins_nIterations,
    _chaikins_min_steep_angle_degrees,
    _chaikins_max_steep_angle_degrees
    );
  
  return ct;
END;
$$
LANGUAGE plpgsql;




CREATE TYPE resolve_overlap_data_debug_options_type AS (
contiune_after_stat_exception boolean, -- if set to false, it will do topology.ValidateTopology and stop to if the this call returns any rows 
validate_topoplogy_for_each_run boolean, -- if set to true, it will do topology.ValidateTopology at each loop return if it's error 
run_add_border_line_as_single_thread boolean, -- if set to false, it will in many cases generate topo errors beacuse of running in many parralell threads
start_at_job_type int, -- if set to more than 1 it will skip init procces and start at given job_type
start_at_loop_nr int, -- many of jobs are ran in loops beacuse because if get an exception or cell is not allowed handle because cell close to is also started to work , this cell will gandled in the next loop.
stop_at_job_type int, -- if set to more than 0 the job will stop  when this job type is reday to run and display a set sql to run
stop_at_loop_nr int -- if set to more than 0 the job will stop  when this job type is reday to run and display a set sql to run
);

CREATE OR REPLACE FUNCTION resolve_overlap_data_debug_options_func(
_contiune_after_stat_exception boolean default true, -- if set to false, it will do topology.ValidateTopology and stop to if the this call returns any rows 
_validate_topoplogy_for_each_run boolean default false, -- if set to true, it will do topology.ValidateTopology at each loop return if it's error 
_run_add_border_line_as_single_thread boolean default true, --  if set to false, it will in many cases generate topo errors beacuse of running in many parralell threads
_start_at_job_type int default 1, -- if set to more than 1 it will skip init procces and start at given job_type
_start_at_loop_nr int default 1, -- many of jobs are ran in loops beacuse because if get an exception or cell is not allowed handle because cell close to is also started to work , this cell will gandled in the next loop.
_stop_at_job_type int default 0, -- if set to more than 0 the job will stop  when this job type is reday to run and display a set sql to run
_stop_at_loop_nr int default 0 -- if set to more than 0 the job will stop  when this job type is reday to run and display a set sql to run
)
RETURNS resolve_overlap_data_debug_options_type
  AS $$
DECLARE
  ct resolve_overlap_data_debug_options_type;
BEGIN
  ct = (
    _contiune_after_stat_exception,
    _validate_topoplogy_for_each_run,
    _run_add_border_line_as_single_thread,
    _start_at_job_type,
    _start_at_loop_nr,
    _stop_at_job_type,
    _stop_at_loop_nr
    );
  
  return ct;
END;
$$
LANGUAGE plpgsql;
