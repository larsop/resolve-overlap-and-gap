DROP FUNCTION IF EXISTS topo_update.get_simplified_border_lines (input_table_name varchar, input_table_geo_column_name varchar, _bb geometry, _snap_tolerance float8, _do_chaikins boolean);

CREATE OR REPLACE FUNCTION topo_update.get_simplified_border_lines (input_table_name varchar, input_table_geo_column_name varchar, _bb geometry, _snap_tolerance float8, _do_chaikins boolean)
  RETURNS TABLE (
    json text,
    geo geometry,
    objectid integer,
    line_type integer)
  LANGUAGE 'plpgsql'
  AS $function$
DECLARE
  command_string text;
  -- This is the boundary geom that contains lines pieces that will added after each single cell is done
  boundary_geom geometry;
  bb_boundary_inner geometry;
  bb_boundary_outer geometry;
  -- This is is a box used to make small glue lines. This lines is needed to make that we don't do any snap out side our own cell
  bb_inner_glue_geom geometry;
  boundary_glue_geom geometry;
  -- TODO add as parameter
  --boundary_with real = 1.5;
  --glue_boundary_with real = 0.5;
  --overlap_width_inner real = 1;
  boundary_with real = _snap_tolerance * 1.5;
  glue_boundary_with real = _snap_tolerance * 0.5;
  overlap_width_inner real = _snap_tolerance;
BEGIN
  -- buffer in to work with geom that lines are only meter from the border
  -- will only work with polygons
  -- make the the polygon that contains lines	that will be added in the post process
  bb_boundary_outer := ST_Expand (_bb, boundary_with);
  bb_boundary_inner := ST_Expand (_bb, (boundary_with * - 1));
  boundary_geom := ST_MakePolygon ((
      SELECT ST_ExteriorRing (ST_Expand (_bb, boundary_with)) AS outer_ring), ARRAY (
        SELECT ST_ExteriorRing (ST_Expand (_bb, ((boundary_with + overlap_width_inner) * - 1))) AS inner_rings));
  -- make the the polygon that contains lines is used a glue lines
  bb_inner_glue_geom := ST_Expand (_bb, ((boundary_with + glue_boundary_with) * - 1));
  boundary_glue_geom := ST_MakePolygon ((
      SELECT ST_ExteriorRing (bb_boundary_inner) AS outer_ring), ARRAY (
        SELECT ST_ExteriorRing (bb_inner_glue_geom) AS inner_rings));
  -- holds the lines inside bb_boundary_inner
  DROP TABLE IF EXISTS tmp_data_all_lines;
  -- get the all the line parts based the bb_boundary_outer
  command_string := Format('CREATE temp table tmp_data_all_lines AS 
 	WITH rings AS (
 	SELECT ST_ExteriorRing((ST_DumpRings((st_dump(%3$s)).geom)).geom) as geom
 	FROM %1$s v
 	where ST_Intersects(v.%3$s,%2$L)
 	),
 	lines as (select distinct (ST_Dump(geom)).geom as geom from rings)
 	select geom from lines 
 	where  ST_IsEmpty(geom) is false', input_table_name, bb_boundary_outer, input_table_geo_column_name);
  EXECUTE command_string;
  command_string := Format('create index %1$s on tmp_data_all_lines using gist(geom)', 'idx1' || Md5(ST_AsBinary (_bb)));
  EXECUTE command_string;
  -- 1 make line parts for inner box
  -- holds the lines inside bb_boundary_inner
  --#############################
  DROP TABLE IF EXISTS tmp_inner_line_parts;
  CREATE temp TABLE tmp_inner_line_parts AS (
    SELECT (ST_Dump (ST_Intersection (rings.geom, bb_inner_glue_geom ) ) ).geom AS geo
    FROM tmp_data_all_lines AS rings
    );
  DROP TABLE IF EXISTS tmp_inner_lines_merged;
  CREATE temp TABLE tmp_inner_lines_merged AS (
    SELECT (ST_Dump (ST_LineMerge (ST_Union (lg.geo ) ) ) ).geom AS geo, 0 AS line_type
    FROM tmp_inner_line_parts AS lg
    );
  IF (_snap_tolerance > 0 AND _do_chaikins IS TRUE) THEN
    UPDATE
      tmp_inner_lines_merged lg
    SET geo = ST_simplifyPreserveTopology (topo_update.chaikinsAcuteAngle (lg.geo, 120, 240), _snap_tolerance);
    RAISE NOTICE ' do snap_tolerance % and do do_chaikins %', _snap_tolerance, _do_chaikins;
    -- TODO send paratmeter if this org data or not. _do_chaikins
    --		insert into tmp_inner_lines_merged(geo,line_type)
    --		SELECT e1.geom as geo , 2 as line_type from  topo_ar5_forest_sysdata.edge e1
    --		where e1.geom && bb_inner_glue_geom;
  ELSE
    IF (_snap_tolerance > 0) THEN
      UPDATE
        tmp_inner_lines_merged lg
      SET geo = ST_simplifyPreserveTopology (lg.geo, _snap_tolerance);
      RAISE NOTICE ' do snap_tolerance % and not do do_chaikins %', _snap_tolerance, _do_chaikins;
    END IF;
    --	update tmp_inner_lines_merged lg
    --	set geo = ST_Segmentize(geo, 1);
  END IF;
  -- log error lines
  INSERT INTO topo_update.no_cut_line_failed (error_info, geo)
  SELECT 'Failed to make valid input line ' AS error_info, r.geo
  FROM tmp_inner_lines_merged r
  WHERE ST_IsValid (r.geo) = FALSE;
  -- make linns for glue parts.
  --#############################
  DROP TABLE IF EXISTS tmp_boundary_line_type_parts;
  CREATE temp TABLE tmp_boundary_line_type_parts AS (
    SELECT (ST_Dump (ST_Intersection (rings.geom, boundary_glue_geom ) ) ).geom AS geo
    FROM tmp_data_all_lines AS rings
    );
  DROP TABLE IF EXISTS tmp_boundary_line_types_merged;
  CREATE temp TABLE tmp_boundary_line_types_merged AS (
    SELECT r.geo, 1 AS line_type
    FROM (
    SELECT (ST_Dump (ST_LineMerge (ST_Union (lg.geo ) ) ) ).geom AS geo
    FROM tmp_boundary_line_type_parts AS lg ) r
  );
INSERT INTO tmp_inner_lines_merged (geo, line_type)
SELECT r.geo, 1 AS line_type
FROM tmp_boundary_line_types_merged r
WHERE ST_ISvalid (r.geo);
  -- log error lines
  INSERT INTO topo_update.no_cut_line_failed (error_info, geo)
  SELECT 'Failed to make valid input border line ' AS error_info, r.geo
  FROM tmp_boundary_line_types_merged r
  WHERE ST_IsValid (r.geo) = FALSE;
  -- make line part for outer box, that contains the line parts will be added add the final stage when all the cell are done.
  --#############################
  DROP TABLE IF EXISTS tmp_boundary_line_parts;
  CREATE temp TABLE tmp_boundary_line_parts AS (
    SELECT (ST_Dump (ST_Intersection (rings.geom, boundary_geom ) ) ).geom AS geo
    FROM tmp_data_all_lines AS rings
    );
  DROP TABLE IF EXISTS tmp_boundary_lines_merged;
  CREATE temp TABLE tmp_boundary_lines_merged AS (
    SELECT (ST_Dump (ST_LineMerge (ST_Union (lg.geo ) ) ) ).geom AS geo
    FROM tmp_boundary_line_parts AS lg
    );
INSERT INTO topo_update.border_line_segments (geo, point_geo)
SELECT r.geo, NULL AS point_geo
FROM (
  SELECT r.geo
  FROM tmp_boundary_lines_merged r
  WHERE ST_IsValid (r.geo) IS TRUE) AS r;
  -- log error lines
  INSERT INTO topo_update.no_cut_line_failed (error_info, geo)
  SELECT 'Failed to make valid input border line ' AS error_info, r.geo
  FROM tmp_boundary_lines_merged r
  WHERE ST_IsValid (r.geo) = FALSE;
  -- return the result of inner geos to handled imediatly
  RETURN QUERY
  SELECT *
  FROM (
    SELECT '{"type": "Feature",' || '"geometry":' || ST_AsGeoJSON (lg3.geo, 10, 2)::json || ',' || '"properties":' || Row_to_json((
        SELECT l FROM (
            SELECT NULL AS "oppdateringsdato") AS l)) || '}' AS json, lg3.geo, 1 AS objectid, lg3.line_type
    FROM (
      SELECT l1.geo, l1.line_type
      FROM tmp_inner_lines_merged l1
      WHERE ST_IsValid (l1.geo)) AS lg3) AS f;
END
$function$;

--truncate table topo_update.border_line_segments;
--select count(g.*) FROM topo_update.get_simplified_border_lines('tmp_sf_ar5_forest_input.not_selected_forest_area','wkb_geometry','0103000020E8640000010000000500000000000000E4152141000000C0E313594100000000E4152141000000000317594100000000CA2F2141000000000317594100000000CA2F2141000000C0E313594100000000E4152141000000C0E3135941',1,false) g;
--select count(g.*) FROM topo_update.get_simplified_border_lines('tmp_sf_ar5_forest_input.not_selected_forest_area','wkb_geometry','0103000020E8640000010000000500000000000000E4152141000000C0E313594100000000E4152141000000000317594100000000CA2F2141000000000317594100000000CA2F2141000000C0E313594100000000E4152141000000C0E3135941',1,true) g;
--drop table sss_1;
--create table sss_1 as (select g.* FROM topo_update.get_simplified_border_lines('tmp_sf_ar5_forest_input.not_selected_forest_area','wkb_geometry','0103000020E8640000010000000500000000000000E4152141000000C0E313594100000000E4152141000000000317594100000000CA2F2141000000000317594100000000CA2F2141000000C0E313594100000000E4152141000000C0E3135941',1,true) as g);
--select * from sss_1;
--SELECT distinct geo from topo_update.border_line_segments;
