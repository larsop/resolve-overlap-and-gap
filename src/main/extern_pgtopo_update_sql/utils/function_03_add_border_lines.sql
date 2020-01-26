/**
 * Add border lines 
 */
DROP FUNCTION topo_update.add_border_lines (_topology_name character varying, _new_line geometry, _snap_tolerance float, _table_name_result_prefix varchar);

-- TODO add code for simple_add_v2
-- TODO add code for get_single_lineparts

CREATE OR REPLACE FUNCTION topo_update.add_border_lines (_topology_name character varying, _new_line geometry, _snap_tolerance float, _table_name_result_prefix varchar)
  RETURNS integer
  AS $$
DECLARE
  update_egde_id int;
  old_egde_geom geometry;
  new_egde_geom geometry;
  command_string text;
  v_state TEXT;
  v_msg TEXT;
  v_detail TEXT;
  v_hint TEXT;
  v_context TEXT;
BEGIN
  BEGIN
    update_egde_id = 0;
    command_string := Format('SELECT e.edge_id, e.geom FROM %1$s.edge_data e where e.geom && %2$L and ST_Intersects(e.geom,%2$L) limit 1', _topology_name, _new_line);
    -- RAISE NOTICE '%s, command_string test %s', ST_Length (_new_line), command_string;
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
    GET STACKED DIAGNOSTICS v_state = RETURNED_SQLSTATE, v_msg = MESSAGE_TEXT, v_detail = PG_EXCEPTION_DETAIL, v_hint = PG_EXCEPTION_HINT,
    v_context = PG_EXCEPTION_CONTEXT;
    RAISE NOTICE 'failed: state  : % message: % detail : % hint   : % context: %', v_state, v_msg, v_detail, v_hint, v_context;
    EXECUTE Format('INSERT INTO %s(line_geo_lost, error_info, d_state, d_msg, d_detail, d_hint, d_context, geo) VALUES(%L, %L, %L, %L, %L, %L, %L, %L)', _table_name_result_prefix || '_no_cut_line_failed', FALSE, 'Warning, will do retry ', v_state, v_msg, v_detail, v_hint, v_context, new_egde_geom);
    -- ERROR:  XX000: SQL/MM Spatial exception - coincident node
    BEGIN
	  PERFORM topology.TopoGeo_addLinestring(_topology_name,geom,_snap_tolerance) 
      FROM (
        SELECT (ST_Dump (geom)).geom
        FROM (
          SELECT topo_update.get_single_lineparts ((ST_Dump (_new_line)).geom) AS geom) AS r) AS r
        WHERE ST_length (geom) > 0;
      EXCEPTION
      WHEN OTHERS THEN
        GET STACKED DIAGNOSTICS v_state = RETURNED_SQLSTATE, v_msg = MESSAGE_TEXT, v_detail = PG_EXCEPTION_DETAIL, v_hint = PG_EXCEPTION_HINT,
        v_context = PG_EXCEPTION_CONTEXT;
      RAISE NOTICE 'failed: state  : % message: % detail : % hint   : % context: %', v_state, v_msg, v_detail, v_hint, v_context;
      EXECUTE Format('INSERT INTO %s(line_geo_lost, error_info, d_state, d_msg, d_detail, d_hint, d_context, geo) VALUES(%L, %L, %L, %L, %L, %L, %L, %L)', _table_name_result_prefix || '_no_cut_line_failed', FALSE, 'Failed topo_update.add_border_lines ', v_state, v_msg, v_detail, v_hint, v_context, _new_line);
      BEGIN
        PERFORM topology.TopoGeo_addLinestring(_topology_name,geom,_snap_tolerance) 
        FROM (
          SELECT (ST_Dump (geom)).geom
          FROM (
            SELECT topo_update.get_single_lineparts ((ST_Dump (topo_update.extend_line(_new_line, _snap_tolerance*2))).geom) AS geom) AS r) AS r
          WHERE ST_length (geom) > 0;  
        EXCEPTION
        WHEN OTHERS THEN
          GET STACKED DIAGNOSTICS v_state = RETURNED_SQLSTATE, v_msg = MESSAGE_TEXT, v_detail = PG_EXCEPTION_DETAIL, v_hint = PG_EXCEPTION_HINT,
          v_context = PG_EXCEPTION_CONTEXT;
        RAISE NOTICE 'failed: state  : % message: % detail : % hint   : % context: %', v_state, v_msg, v_detail, v_hint, v_context;
        EXECUTE Format('INSERT INTO %s(line_geo_lost, error_info, d_state, d_msg, d_detail, d_hint, d_context, geo) VALUES(%L, %L, %L, %L, %L, %L, %L, %L)', _table_name_result_prefix || '_no_cut_line_failed', TRUE, 'Failed topo_update.add_border_lines ', v_state, v_msg, v_detail, v_hint, v_context, _new_line);
        END;
      END;
    END;
  RETURN update_egde_id;
END;

$$
LANGUAGE plpgsql;

UPDATE
  test_topo_jm.jm_ukomm_flate_problem_job_list
SET block_bb = NULL;

TRUNCATE test_topo_jm.jm_ukomm_flate_problem_no_cut_line_failed;

TRUNCATE test_topo_jm.jm_ukomm_flate_problem_job_list_donejobs;

CALL resolve_overlap_gap_single_cell ('test_data.jm_ukomm_flate_problem', 'geo', 'figurid', 'test_topo_jm.jm_ukomm_flate_problem', 'test_topo_jm', 4258, 'false', 2e-06, 1e-06, 'false', 49, 'test_topo_jm.jm_ukomm_flate_problem_job_list', 'test_topo_jm.jm_ukomm_flate_problem_grid', '0103000020A21000000100000005000000019C3592DD2B2640C2A4F8F884C24F40019C3592DD2B2640328E91EC91CE4F4094AA6F9DC0602640328E91EC91CE4F4094AA6F9DC0602640C2A4F8F884C24F40019C3592DD2B2640C2A4F8F884C24F40', 1);

