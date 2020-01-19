-- The name is woring used for adding both border lines and other lines
DROP FUNCTION IF EXISTS topo_update.add_border_lines (_topology_name character varying, _new_line geometry, _snap_tolerance float);

-- TODO add code for simple_add_v2
-- TODO add code for get_single_lineparts
CREATE OR REPLACE FUNCTION topo_update.add_border_lines (_topology_name character varying, _new_line geometry, _snap_tolerance float)
  RETURNS integer
  AS $$
DECLARE
  update_egde_id int;
  old_egde_geom geometry;
  new_egde_geom geometry;
  command_string text;
BEGIN
  BEGIN
    update_egde_id = 0;
    command_string := Format('SELECT e.edge_id, e.geom FROM %1$s.edge_data e where e.geom && %2$L and ST_Intersects(e.geom,%2$L) limit 1', _topology_name, _new_line);
    RAISE NOTICE '%s, command_string test %s', ST_Length (_new_line), command_string;
    --		SELECT e.edge_id, e.geom FROM test_topo.edge_data e where e.geom && 0102000020A2100000020000007E2ED15B6788194000E6ABFBFF314E40F0D5450C668819405156FE0700324E40 and ST_Intersects(e.geom,0102000020A2100000020000007E2ED15B6788194000E6ABFBFF314E40F0D5450C668819405156FE0700324E40) limit 1
    EXECUTE command_string INTO update_egde_id, old_egde_geom;
    IF update_egde_id IS NULL THEN
      new_egde_geom := _new_line;
      --			RAISE NOTICE 'new topo edge wil lbe added no intersecion found';
    ELSE
      --			RAISE NOTICE 'Intersectoin found with edge_id %', update_egde_id;
      --			SELECT ST_Union(e.geom,_new_line)
      --			from topo_ar5_forest_sysdata.edge_data e
      --			where e.geom && _new_line
      --			and ST_Intersects(e.geom,_new_line)
      --			into old_egde_geom;
      --			new_egde_geom := ST_LineMerge(old_egde_geom);
      --			IF ST_geometryType(new_egde_geom) = 'ST_LineString'  THEN
      -- 				RAISE NOTICE 'update edge id %, with %', update_egde_id,ST_AsText(new_egde_geom);
      --				perform topology.ST_ChangeEdgeGeom(
      --				'topo_ar5_forest_sysdata', update_egde_id , new_egde_geom) ;
      --				new_egde_geom := null;
      --			ELSE
      --				RAISE NOTICE 'new topo edge will be added no single linstring added';
      new_egde_geom := _new_line;
      --			END IF;
    END IF;
    IF new_egde_geom IS NOT NULL THEN
      command_string := Format('select topology.TopoGeo_addLinestring(%s,%L,%s)', Quote_literal(_topology_name), new_egde_geom, _snap_tolerance);
      EXECUTE command_string;
    END IF;
    EXCEPTION
    WHEN OTHERS THEN
      RAISE NOTICE 'failed topo_update.add_border_lines ::::::::::::::::::::::::::::::::::::::::::::::::::: % for edge_id % ', ST_GeometryType (new_egde_geom), update_egde_id;
    -- ERROR:  XX000: SQL/MM Spatial exception - coincident node
    BEGIN
      PERFORM topo_update.simple_add_v2 (geom)
      FROM (
        SELECT (ST_Dump (ST_Boundary (ST_Buffer (geom, 1)))).geom
          --                     SELECT (ST_Dump(topo_update.extend_line(geom,2))).geom
        FROM (
          SELECT topo_update.get_single_lineparts ((ST_Dump (_new_line)).geom) AS geom) AS r) AS r
    WHERE ST_length (geom) > 0;
      EXCEPTION
      WHEN OTHERS THEN
        BEGIN
          PERFORM topo_update.simple_add_v2 (topo_update.extend_line (geom, 2))
          FROM (
            SELECT (ST_Dump (topo_update.get_single_lineparts ((ST_Dump (_new_line)).geom))).geom) AS r;
          EXCEPTION
          WHEN OTHERS THEN
            RAISE NOTICE 'failed  ::::::::::::::::::::::::::::::::::::::::::::::::::: ';
          -- select TopoGeo_addLinestring('topo_ar5_forest_sysdata','0102000020E86400000200000000F0FF2748422341FDFF008045125941001000F8474223410300FF7F48125941',1)
          INSERT INTO topo_update.no_cut_line_failed (error_info, geo)
            VALUES ('Failed for line with length ' || ST_length (new_egde_geom), new_egde_geom);
          END;
      END;
    END;
  RETURN update_egde_id;
END;

$$
LANGUAGE plpgsql;

--select topo_update.add_border_lines('0102000020E864000004000000000000808C392341DA40A70D1B095941000000808A392341000000001B095941000000808A392341FFFFFFFF0D095941000000808C392341FFFFFFFF0D095941');
--select topo_update.add_border_lines('0102000020E864000004000000000000808C392341DA40A70D1B095941000000808A392341000000001B095941000000808A392341FFFFFFFF0D095941000000808C392341FFFFFFFF0D095941');
-- select topo_update.add_border_lines(geo) FROM topo_update.border_line_segments where id = 11295 ;
