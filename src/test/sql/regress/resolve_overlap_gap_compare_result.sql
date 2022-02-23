-- This is procedure used testing resolve and overlap
DROP PROCEDURE if exists resolve_overlap_gap_compare_result (
_input resolve_overlap_data_input_type, 
--(_input).polygon_table_name varchar, -- The table to resolv, imcluding schema name
--(_input).polygon_table_pk_column varchar, -- The primary of the input table
--(_input).polygon_table_geo_collumn varchar, -- the name of geometry column on the table to analyze
--(_input).table_srid int, -- the srid for the given geo column on the table analyze
--(_input).utm boolean, 
_topology_info resolve_overlap_data_topology_type,
---(_topology_info).topology_name varchar, -- The topology schema name where we store store sufaces and lines from the simple feature dataset and th efinal result
-- NB. Any exting data will related to topology_name will be deleted
--(_topology_info).topology_snap_tolerance float, -- this is tolerance used as base when creating the the postgis topolayer

_clean_info resolve_overlap_data_clean_type, -- different parameters used if need to clean up your data
--(_clean_info).simplify_tolerance float, -- is this is more than zero simply will called with
--(_clean_info).do_chaikins boolean, -- here we will use chaikins togehter with simply to smooth lines
--(_clean_info).min_area_to_keep float, -- if this a polygon  is below this limit it will merge into a neighbour polygon. The area is sqare meter. 

_max_parallel_jobs int, -- this is the max number of paralell jobs to run. There must be at least the same number of free connections
_max_rows_in_each_cell int -- this is the max number rows that intersects with box before it's split into 4 new boxes, default is 5000
);

CREATE OR REPLACE PROCEDURE resolve_overlap_gap_compare_result (
_input resolve_overlap_data_input_type, 
--(_input).polygon_table_name varchar, -- The table to resolv, imcluding schema name
--(_input).polygon_table_pk_column varchar, -- The primary of the input table
--(_input).polygon_table_geo_collumn varchar, -- the name of geometry column on the table to analyze
--(_input).table_srid int, -- the srid for the given geo column on the table analyze
--(_input).utm boolean, 
_topology_info resolve_overlap_data_topology_type,
---(_topology_info).topology_name varchar, -- The topology schema name where we store store sufaces and lines from the simple feature dataset and th efinal result
-- NB. Any exting data will related to topology_name will be deleted
--(_topology_info).topology_snap_tolerance float, -- this is tolerance used as base when creating the the postgis topolayer

_clean_info resolve_overlap_data_clean_type, -- different parameters used if need to clean up your data
--(_clean_info).simplify_tolerance float, -- is this is more than zero simply will called with
--(_clean_info).do_chaikins boolean, -- here we will use chaikins togehter with simply to smooth lines
--(_clean_info).min_area_to_keep float, -- if this a polygon  is below this limit it will merge into a neighbour polygon. The area is sqare meter. 

_max_parallel_jobs int, -- this is the max number of paralell jobs to run. There must be at least the same number of free connections
_max_rows_in_each_cell int -- this is the max number rows that intersects with box before it's split into 4 new boxes, default is 5000
)
LANGUAGE plpgsql
AS $$
DECLARE
  command_string text;
  sf_table_meta_res text = (_topology_info).topology_name||'.'||'simple_compare_result';
  sf_table_in text = (_input).polygon_table_name;
  sf_table_out text = (_topology_info).topology_name||'.'||split_part((_input).polygon_table_name,'.',2)||'_result' ;
  num_before numeric;
  num_after numeric;
  overlap_gap_before_prefix text = (_topology_info).topology_name||'.'||split_part((_input).polygon_table_name,'.',2)||'_og_before' ;
  overlap_gap_after_prefix text = (_topology_info).topology_name||'.'||split_part((_input).polygon_table_name,'.',2)||'_og_after' ;
  overlap_gap_found boolean;
BEGIN


command_string = Format('select EXISTS (
   SELECT FROM information_schema.tables 
   WHERE  table_schema = %L AND table_name = %L)',
   (_topology_info).topology_name, 
   split_part(overlap_gap_before_prefix,'.',2)||'_overlap'
   );
execute command_string INTO overlap_gap_found;

IF overlap_gap_found = false THEN
  command_string = Format('CALL find_overlap_gap_run(%L,%L,%s,%L,%L,%L)',
  sf_table_in, 
  (_input).polygon_table_geo_collumn, (_input).table_srid , 
  overlap_gap_before_prefix,
  _max_parallel_jobs,
  _max_rows_in_each_cell);
  RAISE NOTICE 'run this first %', command_string;
  return;
END IF;

command_string = Format('select EXISTS (
   SELECT FROM information_schema.tables 
   WHERE  table_schema = %L AND table_name = %L)',
   (_topology_info).topology_name, 
   split_part(overlap_gap_after_prefix,'.',2)||'_overlap'
   );
execute command_string INTO overlap_gap_found;

IF overlap_gap_found = false THEN
  command_string = Format('CALL find_overlap_gap_run(%L,%L,%s,%L,%L,%L)',
  sf_table_out, 
  (_input).polygon_table_geo_collumn, (_input).table_srid , 
  overlap_gap_after_prefix,
  _max_parallel_jobs,
  _max_rows_in_each_cell);
  RAISE NOTICE 'run this first %', command_string;
  return;
END IF;

-- Create result table -----------

command_string = Format('drop table if exists %s',sf_table_meta_res); 
execute command_string;

command_string = Format('create table %s(id serial, description text, num_before numeric, num_after numeric)',sf_table_meta_res); 
execute command_string;

-- Check simple feature table -----------
-----------------------------------------
command_string := Format('select count(o.*) FROM %s o',sf_table_in);
EXECUTE command_string into num_before;
command_string := Format('select count(o.*) FROM %s o where o.%s is not null',sf_table_out,(_input).polygon_table_pk_column);
EXECUTE command_string into num_after;
command_string = Format('insert into %s(description, num_before, num_after) VALUES(%L,%L,%L)',sf_table_meta_res,
'Polygons with attribute values ',num_before,num_after);
execute command_string;

command_string := Format('select sum(ST_NPoints(o.%s)) FROM %s o',(_input).polygon_table_geo_collumn,sf_table_in);
EXECUTE command_string into num_before;
command_string := Format('select sum((ST_NPoints(o.%s))) FROM %s o where o.%s is not null',(_input).polygon_table_geo_collumn,sf_table_out,(_input).polygon_table_pk_column);
EXECUTE command_string into num_after;
command_string = Format('insert into %s(description, num_before, num_after) VALUES(%L,%L,%L)',sf_table_meta_res,
'Total points in polygons with attribute values',num_before,num_after);
execute command_string;

command_string := Format('select count(o.*) FROM %s o',sf_table_in);
EXECUTE command_string into num_before;
command_string := Format('select count(o.*) FROM %s o',sf_table_out);
EXECUTE command_string into num_after;
command_string = Format('insert into %s(description, num_before, num_after) VALUES(%L,%L,%L)',sf_table_meta_res,
'All polygons ',num_before,num_after);
execute command_string;

command_string := Format('select sum(ST_NPoints(o.%s)) FROM %s o',(_input).polygon_table_geo_collumn,sf_table_in);
EXECUTE command_string into num_before;
command_string := Format('select sum((ST_NPoints(o.%s))) FROM %s o',(_input).polygon_table_geo_collumn,sf_table_out,(_input).polygon_table_pk_column);
EXECUTE command_string into num_after;
command_string = Format('insert into %s(description, num_before, num_after) VALUES(%L,%L,%L)',sf_table_meta_res,
'Total points in all polygons',num_before,num_after);
execute command_string;


command_string := Format('select count(o.*) FROM %s o where o.%s is null',sf_table_out,(_input).polygon_table_pk_column);
EXECUTE command_string into num_after;
command_string = Format('insert into %s(description, num_before, num_after) VALUES(%L,%L,%L)',sf_table_meta_res,
'Polygons with no attribute value(new polygons)',0,num_after);
execute command_string;

command_string := Format('select count(o.*) from 
(select CASE WHEN %L = false THEN ST_Area(o.%s,TRUE) ELSE ST_Area(o.%s) END AS area FROM %s o ) as o 
where area < %s ',
(_input).utm, (_input).polygon_table_geo_collumn, (_input).polygon_table_geo_collumn, sf_table_in, (_clean_info).min_area_to_keep);
EXECUTE command_string into num_before;
command_string := Format('select count(o.*) from 
(select CASE WHEN %L = false THEN ST_Area(o.%s,TRUE) ELSE ST_Area(o.%s) END AS area FROM %s o ) as o 
where area < %s ',
(_input).utm, (_input).polygon_table_geo_collumn, (_input).polygon_table_geo_collumn, sf_table_out, (_clean_info).min_area_to_keep);
EXECUTE command_string into num_after;
command_string = Format('insert into %s(description, num_before, num_after) VALUES(%L,%L,%L)',sf_table_meta_res,
'Polygons with area less than '||(_clean_info).min_area_to_keep||' m2. (minarea) ',num_before,num_after);
execute command_string;


--command_string := Format('select count(*) FROM %s o where ST_IsValid(o.%s) = false',sf_table_in,(_input).polygon_table_geo_collumn);
--EXECUTE command_string into num_before;
--command_string := Format('select count(o.*) FROM %s o where o.%s is null and _input_geo_is_valid = false',sf_table_out,(_input).polygon_table_pk_column);
--EXECUTE command_string into num_after;
--command_string = Format('insert into %s(description, num_before, num_after) VALUES(%L,%L,%L)',sf_table_meta_res,'Result polygons with no attr. because of invalid input ',num_before,num_after);
--execute command_string;


-- Check Topology table -----------
-----------------------------------------
command_string := Format('select count(o.*) FROM %s o',(_topology_info).topology_name||'.edge_data');
EXECUTE command_string into num_after;
command_string = Format('insert into %s(description, num_before, num_after) VALUES(%L,%L,%L)',sf_table_meta_res,
'Toplogy Num edges with attribute values ',null,num_after);
execute command_string;

command_string := Format('select sum((ST_NPoints(o.geom))) FROM %s o',(_topology_info).topology_name||'.edge_data');
EXECUTE command_string into num_after;
command_string = Format('insert into %s(description, num_before, num_after) VALUES(%L,%L,%L)',sf_table_meta_res,
'Topology sPoints inedges with attribute values',null,num_after);
execute command_string;

-- Check overlaps result from overlap and gap-----------
--------------------------------------------------------
command_string := Format('select count(*) from 
(select * FROM 
  (SELECT (ST_Dump(%s)).geom from %s) as o where ST_GeometryType(geom) in (%L,%L)
) as o where (CASE WHEN %L = false THEN ST_Area(o.geom,TRUE) ELSE ST_Area(o.geom) END) > 0.0',
(_input).polygon_table_geo_collumn,
overlap_gap_before_prefix||'_overlap',
'ST_Polygon','ST_MultiPolygon',
(_input).utm);
EXECUTE command_string into num_before;
command_string := Format('select count(*) from 
(select * FROM 
  (SELECT (ST_Dump(%s)).geom from %s) as o where ST_GeometryType(geom) in (%L,%L)
) as o where (CASE WHEN %L = false THEN ST_Area(o.geom,TRUE) ELSE ST_Area(o.geom) END) > 0.0',
(_input).polygon_table_geo_collumn,
overlap_gap_after_prefix||'_overlap',
'ST_Polygon','ST_MultiPolygon',
(_input).utm);
EXECUTE command_string into num_after;
command_string = Format('insert into %s(description, num_before, num_after) VALUES(%L,%L,%L)',sf_table_meta_res,
'Number of overlaps with area',num_before,num_after);
execute command_string;


command_string := Format('select Round(sum(area)::numeric,0) from 
(select CASE WHEN %L = false THEN ST_Area(o.geom,TRUE) ELSE ST_Area(o.geom) END AS area FROM 
  (SELECT (ST_Dump(%s)).geom from %s) as o where  ST_GeometryType(geom) in (%L,%L)
) as o',
(_input).utm, 
(_input).polygon_table_geo_collumn,
overlap_gap_before_prefix||'_overlap',
'ST_Polygon','ST_MultiPolygon');
EXECUTE command_string into num_before;
command_string := Format('select Round(sum(area)::numeric,0) from 
(select CASE WHEN %L = false THEN ST_Area(o.geom,TRUE) ELSE ST_Area(o.geom) END AS area FROM 
  (SELECT (ST_Dump(%s)).geom from %s) as o where  ST_GeometryType(geom) in (%L,%L)
) as o',
(_input).utm,
(_input).polygon_table_geo_collumn,
overlap_gap_after_prefix||'_overlap',
'ST_Polygon','ST_MultiPolygon');
EXECUTE command_string into num_after;
command_string = Format('insert into %s(description, num_before, num_after) VALUES(%L,%L,%L)',sf_table_meta_res,
'Area m2. with overlap',num_before,num_after);
execute command_string;

-- Check gaps result from overlap and gap-----------
----------------------------------------------------
command_string := Format('select count(*) from 
(select * FROM 
  (SELECT (ST_Dump(%s)).geom from %s) as o 
) as o',
(_input).polygon_table_geo_collumn,
overlap_gap_before_prefix||'_gap');
EXECUTE command_string into num_before;
command_string := Format('select count(*) from 
(select * FROM 
  (SELECT (ST_Dump(%s)).geom from %s) as o 
) as o',
(_input).polygon_table_geo_collumn,
overlap_gap_after_prefix||'_gap');
EXECUTE command_string into num_after;
command_string = Format('insert into %s(description, num_before, num_after) VALUES(%L,%L,%L)',sf_table_meta_res,
'Number of gaps (maybe polygons,lines,points)',num_before,num_after);
execute command_string;


command_string := Format('select Round(sum(area)::numeric,0) from 
(select CASE WHEN %L = false THEN ST_Area(o.geom,TRUE) ELSE ST_Area(o.geom) END AS area FROM 
  (SELECT (ST_Dump(%s)).geom from %s) as o 
) as o',
(_input).utm, 
(_input).polygon_table_geo_collumn,
overlap_gap_before_prefix||'_gap');
--EXECUTE command_string into num_before;
command_string := Format('select Round(sum(area)::numeric,0) from 
(select CASE WHEN %L = false THEN ST_Area(o.geom,TRUE) ELSE ST_Area(o.geom) END AS area FROM 
  (SELECT (ST_Dump(%s)).geom from %s) as o 
) as o',
(_input).utm, 
(_input).polygon_table_geo_collumn,
overlap_gap_after_prefix||'_gap');
--EXECUTE command_string into num_after;
command_string = Format('insert into %s(description, num_before, num_after) VALUES(%L,%L,%L)',sf_table_meta_res,
'Area m2. with gaps',num_before,num_after);
--execute command_string;



RAISE NOTICE 'select  description,  num_before as %, num_after as % from  %', 
quote_ident(sf_table_in),
quote_ident(sf_table_out),
sf_table_meta_res;



END
$$;

drop table if exists test_topo_t2.simple_compare_result;


CALL resolve_overlap_gap_compare_result(
(null,null,null, -- The simple line feature info with attributtes 
'test_data.overlap_gap_input_t2','c1','geom' -- The simple polygons feature info with attributtes
,4258,false, -- info about srid and utm or not
null,null
), -- TYPE resolve_overlap_data_input
('test_topo_t2',0.00001,false,null), -- TYPE resolve_overlap_data_topology
  resolve_overlap_data_clean_type_func(  -- TYPE resolve_overlap_data_clean
  49,  -- if this a polygon  is below this limit it will merge into a neighbour polygon. The area is sqare meter.
  0, -- is this is more than zero simply will called with
  null, -- _max_average_vertex_length, in meter both for utm and deegrees, this used to avoid running ST_simplifyPreserveTopology for long lines lines with few points
  0, -- IF 0 NO CHAKINS WILL BE DONE A big value here make no sense because the number of points will increaes exponential )
  10000, --edge that are longer than this value will not be touched by _chaikins_min_degrees and _chaikins_max_degrees  
  120, -- The angle has to be less this given value, This is used to avoid to touch all angles. 
  240, -- OR the angle has to be greather than this given value, This is used to avoid to touch all angles 
  40, -- The angle has to be less this given value, This is used to avoid to touch all angles. 
  320 -- OR The angle has to be greather than this given value, This is used to avoid to touch all angles 
)
,5,4);

select  description,  num_before as "test_data.overlap_gap_input_t2", num_after as "test_topo_t2.overlap_gap_input_t2_result" from  test_topo_t2.simple_compare_result
