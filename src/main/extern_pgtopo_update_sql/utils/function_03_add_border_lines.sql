CREATE OR REPLACE FUNCTION topo_update.add_border_lines (_topology_name character varying, _new_line geometry, _snap_tolerance float, _table_name_result_prefix varchar)
  RETURNS integer
  AS $$
DECLARE
  update_egde_id int;
  old_egde_geom geometry;
  new_egde_geom geometry;
  tmp_egde_geom geometry;
  single_line_geo geometry;
  command_string text;
  v_state TEXT;
  v_msg TEXT;
  v_detail TEXT;
  v_hint TEXT;
  v_context TEXT;
  i INT DEFAULT 0;
  dim INT DEFAULT 0;
  i2 INT DEFAULT 0;
  dim2 INT DEFAULT 0;
  no_cutline_filename varchar;
BEGIN
  no_cutline_filename = _table_name_result_prefix || '_no_cut_line_failed';
  BEGIN
    command_string := Format('select topology.TopoGeo_addLinestring(%s,%L,%s)', Quote_literal(_topology_name), _new_line, _snap_tolerance);
    EXECUTE command_string;
    EXCEPTION
    WHEN OTHERS THEN
      RAISE NOTICE 'failed topo_update.add_border_lines ::::::::::::::::::::::::::::::::::::::::::::::::::: % for edge_id % ', ST_GeometryType (new_egde_geom), update_egde_id;
    GET STACKED DIAGNOSTICS v_state = RETURNED_SQLSTATE, v_msg = MESSAGE_TEXT, v_detail = PG_EXCEPTION_DETAIL, v_hint = PG_EXCEPTION_HINT,
    v_context = PG_EXCEPTION_CONTEXT;
    RAISE NOTICE 'failed: state  : % message: % detail : % hint   : % context: %', v_state, v_msg, v_detail, v_hint, v_context;
    EXECUTE Format('INSERT INTO %s(line_geo_lost, error_info, d_state, d_msg, d_detail, d_hint, d_context, geo) VALUES(%L, %L, %L, %L, %L, %L, %L, %L)', no_cutline_filename, FALSE, 'Warn1, will do retry, topo_update.add_border_lines ', v_state, v_msg, v_detail, v_hint, v_context, _new_line);
    BEGIN
      single_line_geo = ST_Multi (topo_update.get_single_lineparts (_new_line));
      SELECT ST_NumGeometries (single_line_geo) INTO dim;
      -- Add eache single line
      WHILE i < dim LOOP
        i := i + 1;
        new_egde_geom := ST_GeometryN (single_line_geo, i);
        ---
        BEGIN
          command_string := Format('select topology.TopoGeo_addLinestring(%s,%L,%s)', Quote_literal(_topology_name), new_egde_geom, _snap_tolerance);
          EXECUTE command_string;
          EXCEPTION
          WHEN OTHERS THEN
            RAISE NOTICE 'failed topo_update.add_border_lines ::::::::::::::::::::::::::::::::::::::::::::::::::: %', ST_GeometryType (new_egde_geom);
          GET STACKED DIAGNOSTICS v_state = RETURNED_SQLSTATE, v_msg = MESSAGE_TEXT, v_detail = PG_EXCEPTION_DETAIL, v_hint = PG_EXCEPTION_HINT,
          v_context = PG_EXCEPTION_CONTEXT;
          RAISE NOTICE 'failed: state  : % message: % detail : % hint   : % context: %', v_state, v_msg, v_detail, v_hint, v_context;
          EXECUTE Format('INSERT INTO %s(line_geo_lost, error_info, d_state, d_msg, d_detail, d_hint, d_context, geo) VALUES(%L, %L, %L, %L, %L, %L, %L, %L)', no_cutline_filename, FALSE, 'Warn2, will do retry, topo_update.add_border_lines ', v_state, v_msg, v_detail, v_hint, v_context, new_egde_geom);
          --1
          BEGIN
            -- try extend line, that may help som time
            tmp_egde_geom := topo_update.extend_line (new_egde_geom, _snap_tolerance * 2);
            command_string := Format('select topology.TopoGeo_addLinestring(%s,%L,%s)', Quote_literal(_topology_name), tmp_egde_geom, _snap_tolerance);
            EXECUTE command_string;
            EXCEPTION
            WHEN OTHERS THEN
              RAISE NOTICE 'failed topo_update.add_border_lines ::::::::::::::::::::::::::::::::::::::::::::::::::: %', ST_GeometryType (new_egde_geom);
            GET STACKED DIAGNOSTICS v_state = RETURNED_SQLSTATE, v_msg = MESSAGE_TEXT, v_detail = PG_EXCEPTION_DETAIL, v_hint = PG_EXCEPTION_HINT,
            v_context = PG_EXCEPTION_CONTEXT;
            RAISE NOTICE 'failed: state  : % message: % detail : % hint   : % context: %', v_state, v_msg, v_detail, v_hint, v_context;
            EXECUTE Format('INSERT INTO %s(line_geo_lost, error_info, d_state, d_msg, d_detail, d_hint, d_context, geo) VALUES(%L, %L, %L, %L, %L, %L, %L, %L)', no_cutline_filename, FALSE, 'Warn3, will do retry, topo_update.add_border_lines ', v_state, v_msg, v_detail, v_hint, v_context, tmp_egde_geom);
            -- 2
            BEGIN
              -- try remove old intersecting egdes and add them again
              tmp_egde_geom := ST_ExteriorRing (ST_buffer (new_egde_geom, _snap_tolerance, 1));
              command_string := Format('CREATE TEMP table temp_table_fix_topo as 
                (
                select ST_LineMerge(ST_Union(ST_SnapToGrid(e.geom,%1$s),ST_SnapToGrid(%2$L,%1$s))) as line, e.edge_id
                from 
                %3$s.edge e
                where e.geom && %4$L and ST_Intersects(e.geom,%4$L)
                )', _snap_tolerance, new_egde_geom, _topology_name, tmp_egde_geom);
              EXECUTE command_string;
              command_string := Format('select topology.ST_RemEdgeNewFace(%1$L,  l.edge_id) from 
                (select distinct edge_id from temp_table_fix_topo) as l,
                %1$s.edge e
                where l.edge_id = e.edge_id', _topology_name);
              EXECUTE command_string;
              command_string := Format('select topology.TopoGeo_addLinestring(%1$L,line,0.00001) from temp_table_fix_topo', _topology_name);
              EXECUTE command_string;
              DROP TABLE temp_table_fix_topo;
              EXCEPTION
              WHEN OTHERS THEN
                RAISE NOTICE 'failed topo_update.add_border_lines ::::::::::::::::::::::::::::::::::::::::::::::::::: %', ST_GeometryType (tmp_egde_geom);
              GET STACKED DIAGNOSTICS v_state = RETURNED_SQLSTATE, v_msg = MESSAGE_TEXT, v_detail = PG_EXCEPTION_DETAIL, v_hint = PG_EXCEPTION_HINT,
              v_context = PG_EXCEPTION_CONTEXT;
              RAISE NOTICE 'failed: state  : % message: % detail : % hint   : % context: %', v_state, v_msg, v_detail, v_hint, v_context;
              EXECUTE Format('INSERT INTO %s(line_geo_lost, error_info, d_state, d_msg, d_detail, d_hint, d_context, geo) VALUES(%L, %L, %L, %L, %L, %L, %L, %L)', no_cutline_filename, TRUE, 'Failed1, topo_update.add_border_lines', v_state, v_msg, v_detail, v_hint, v_context, tmp_egde_geom);
              -- 2
              END;
            --1
            END;
          END;
        ---
      END LOOP;
      EXCEPTION
      WHEN OTHERS THEN
        GET STACKED DIAGNOSTICS v_state = RETURNED_SQLSTATE, v_msg = MESSAGE_TEXT, v_detail = PG_EXCEPTION_DETAIL, v_hint = PG_EXCEPTION_HINT,
        v_context = PG_EXCEPTION_CONTEXT;
      RAISE NOTICE 'failed: state  : % message: % detail : % hint   : % context: %', v_state, v_msg, v_detail, v_hint, v_context;
      EXECUTE Format('INSERT INTO %s(line_geo_lost, error_info, d_state, d_msg, d_detail, d_hint, d_context, geo) VALUES(%L, %L, %L, %L, %L, %L, %L, %L)', no_cutline_filename, TRUE, 'Failed2, topo_update.add_border_lines ', v_state, v_msg, v_detail, v_hint, v_context, _new_line);
      END;
    END;
  RETURN update_egde_id;
END;

$$
LANGUAGE plpgsql;

