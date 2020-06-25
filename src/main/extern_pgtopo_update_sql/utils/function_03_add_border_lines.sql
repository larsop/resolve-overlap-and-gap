drop function if exists topo_update.add_border_lines (_topology_name character varying, _new_line_raw geometry, _snap_tolerance float, _table_name_result_prefix varchar);

CREATE OR REPLACE FUNCTION topo_update.add_border_lines (_topology_name character varying, 
_new_line_raw geometry, 
_snap_tolerance float, 
_table_name_result_prefix varchar,
_do_retry_add boolean)
  RETURNS integer[]
  AS $$
DECLARE
  new_line geometry;
  new_egde_geom geometry;
  tmp_egde_geom geometry;
  tmp_edge_geom2 geometry;
  single_line_geo geometry;
  single_line_geo2 geometry;
  command_string text;
  v_state text;
  v_msg text;
  v_detail text;
  v_hint text;
  v_context text;
  i INT DEFAULT 0;
  dim INT DEFAULT 0;
  i2 INT DEFAULT 0;
  dim2 INT DEFAULT 0;
  no_cutline_filename varchar;
  crosses_edge int;
  crosses_edge_num int;
  done_ok boolean;
  num_done_ok int;
  num_not_done_ok int = 0;
  max_num_not_done_ok int = 4;
  lost_data boolean;
  -- returns a set of edge identifiers forming it up
  edges_added integer[];
  
  tolerance_retry_num int;
  tolerance_retry_diff real;
  tolerance_retry_value real;
  
  deadlock_detected int;
  

BEGIN
	
  no_cutline_filename = _table_name_result_prefix || '_no_cut_line_failed';
  BEGIN
	new_line := ST_RemoveRepeatedPoints (_new_line_raw, _snap_tolerance);

	  
    command_string := Format('SELECT ARRAY(SELECT topology.TopoGeo_addLinestring(%L,%L,%s))', _topology_name, new_line, _snap_tolerance);
    EXECUTE command_string into edges_added;
 
    EXCEPTION
    WHEN OTHERS THEN
    GET STACKED DIAGNOSTICS v_state = RETURNED_SQLSTATE, v_msg = MESSAGE_TEXT, v_detail = PG_EXCEPTION_DETAIL, v_hint = PG_EXCEPTION_HINT,
    v_context = PG_EXCEPTION_CONTEXT;
    RAISE NOTICE 'failed with in default case  : % message: % detail : % hint   : % context: %', v_state, v_msg, v_detail, v_hint, v_context;
    
    SELECT Position('deadlock detected' IN v_msg) INTO deadlock_detected;
    
    IF (deadlock_detected > 0 ) THEN
      RAISE EXCEPTION 'failed: state deadlock detected or _snap_tolerance = 0  : % message: % detail : % hint   : % context: %', v_state, v_msg, v_detail, v_hint, v_context;
    END IF;

    IF (_snap_tolerance = 0)
    THEN
      RAISE NOTICE 'failed: state deadlock detected or _snap_tolerance = 0  : % message: % detail : % hint   : % context: %', v_state, v_msg, v_detail, v_hint, v_context;
      EXECUTE Format('INSERT INTO %s(line_geo_lost, error_info, d_state, d_msg, d_detail, d_hint, d_context, geo) 
                      VALUES(%L, %L, %L, %L, %L, %L, %L, %L)', 
                      no_cutline_filename, TRUE, 'Failed3, topo_update.add_border_lines ', v_state, v_msg, v_detail, v_hint, v_context, new_line);
      RETURN NULL;
    END IF;
     
    tolerance_retry_num := 1;
    tolerance_retry_diff := 0.75;
    tolerance_retry_value := _snap_tolerance*tolerance_retry_num*tolerance_retry_diff;
    
    EXECUTE Format('INSERT INTO %s(line_geo_lost, error_info, d_state, d_msg, d_detail, d_hint, d_context, geo) VALUES(%L, %L, %L, %L, %L, %L, %L, %L)', no_cutline_filename, FALSE, 
    'Warn2, will do retry, topo_update.add_border_lines with tolerance :'||tolerance_retry_value||' and tolerance_retry_num '|| tolerance_retry_num ||'for topology '||_topology_name, 
    v_state, v_msg, v_detail, v_hint, v_context, new_line);
    
   
   IF (_do_retry_add = false) THEN
   
      RAISE NOTICE '_do_retry_add is false, just log  : % message: % detail : % hint   : % context: %', v_state, v_msg, v_detail, v_hint, v_context;
      EXECUTE Format('INSERT INTO %s(line_geo_lost, error_info, d_state, d_msg, d_detail, d_hint, d_context, geo) 
                      VALUES(%L, %L, %L, %L, %L, %L, %L, %L)', 
                      no_cutline_filename, TRUE, 'Will not do retry ', v_state, v_msg, v_detail, v_hint, v_context, new_line);

     RETURN edges_added;
   END IF;
	-- Try with different snap to
	
    LOOP
    
      tolerance_retry_value := _snap_tolerance*tolerance_retry_num*tolerance_retry_diff;
      BEGIN
	    command_string := Format('SELECT ARRAY(SELECT topology.TopoGeo_addLinestring(%L,%L,%s))', 
	    _topology_name, new_line, tolerance_retry_value);
        EXECUTE command_string into edges_added;
        RETURN edges_added;

      EXCEPTION
      WHEN OTHERS THEN
        GET STACKED DIAGNOSTICS v_state = RETURNED_SQLSTATE, v_msg = MESSAGE_TEXT, v_detail = PG_EXCEPTION_DETAIL, v_hint = PG_EXCEPTION_HINT,
        v_context = PG_EXCEPTION_CONTEXT;
        RAISE NOTICE 'failed after trying with tolerance % : % message: % detail : % hint   : % context: %', 
        tolerance_retry_value, v_state, v_msg, v_detail, v_hint, v_context;
        
        SELECT Position('deadlock detected' IN v_msg) INTO deadlock_detected;
        IF (deadlock_detected > 0 )
        THEN
         RAISE EXCEPTION 'failed after trying with tolerance, got dead lock: state  : % message: % detail : % hint   : % context: %', v_state, v_msg, v_detail, v_hint, v_context;
        END IF;
  	
      END;	    


      EXIT WHEN tolerance_retry_num > 8;
      tolerance_retry_num := tolerance_retry_num  + 1;
      
      EXECUTE Format('INSERT INTO %s(line_geo_lost, error_info, d_state, d_msg, d_detail, d_hint, d_context, geo) VALUES(%L, %L, %L, %L, %L, %L, %L, %L)', no_cutline_filename, FALSE, 
      'Warn2, will do retry, topo_update.add_border_lines with tolerance :'||tolerance_retry_value||' and tolerance_retry_num '|| tolerance_retry_num ||'for topology '||_topology_name, 
       v_state, v_msg, v_detail, v_hint, v_context, new_line);

    END LOOP;



      -- Try with break up in smaller parts
    
    BEGIN
      single_line_geo = ST_Multi (topo_update.get_single_lineparts (new_line));
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
          EXECUTE Format('INSERT INTO %s(line_geo_lost, error_info, d_state, d_msg, d_detail, d_hint, d_context, geo) VALUES(%L, %L, %L, %L, %L, %L, %L, %L)', no_cutline_filename, FALSE, 
          'Warn2, will do retry, topo_update.add_border_lines ' || 'for topology '||_topology_name,
          v_state, v_msg, v_detail, v_hint, v_context, new_egde_geom);
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
            EXECUTE Format('INSERT INTO %s(line_geo_lost, error_info, d_state, d_msg, d_detail, d_hint, d_context, geo) VALUES(%L, %L, %L, %L, %L, %L, %L, %L)', no_cutline_filename, FALSE, 
            'Warn3, will do retry, topo_update.add_border_lines ', v_state, v_msg, v_detail, v_hint, v_context, tmp_egde_geom);
            -- 2
            BEGIN
              -- try remove old intersecting egdes and add them again
              -- Check if the last message is 'SQL/MM Spatial exception - geometry crosses edge ****'
              
	          CREATE TEMP table temp_table_fix_topo(line geometry, edge_id int);
	          
              LOOP
                BEGIN
	            crosses_edge_num = - 1;
	            done_ok := true;
                num_not_done_ok := num_not_done_ok + 1;
                i2 := 0;

                EXECUTE Format('INSERT INTO %s(line_geo_lost, error_info, d_state, d_msg, d_detail, d_hint, d_context, geo) VALUES(%L, %L, %L, %L, %L, %L, %L, %L)', no_cutline_filename, FALSE, 
                'Warn4, will do retry, topo_update.add_border_lines ', v_state, v_msg, v_detail, v_hint, v_context, tmp_egde_geom);

                RAISE NOTICE 'num_not_done_ok rrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrr: %', num_not_done_ok;
 
                SELECT Position('geometry crosses edge' IN v_msg) INTO crosses_edge;
                tmp_egde_geom := new_egde_geom;
                IF (crosses_edge > 0) THEN
                  crosses_edge_num := Trim(Substring(v_msg FROM (crosses_edge + Char_length('geometry crosses edge'))))::Int;
                  command_string := Format('INSERT into temp_table_fix_topo(line,edge_id)  
                       select distinct (ST_Dump(ST_LineMerge(ST_Union(ST_SnapToGrid(e.geom,%1$s),ST_SnapToGrid(%2$L,%1$s))))).geom as line, e.edge_id
                       from 
                       %3$s.edge e
                       where e.edge_id = %4$L', _snap_tolerance, tmp_egde_geom, _topology_name, crosses_edge_num);
                ELSE
                  command_string := Format('INSERT into temp_table_fix_topo(line,edge_id)   
                       select distinct (ST_Dump(ST_LineMerge(ST_Union(ST_SnapToGrid(e.geom,%1$s),ST_SnapToGrid(%2$L,%1$s))))).geom as line, e.edge_id
                       from 
                       %3$s.edge e
                       where e.geom && %2$L and ST_Intersects(e.geom,%2$L)
                       ', _snap_tolerance, tmp_egde_geom, _topology_name);
                END IF;
                EXECUTE command_string;
                command_string := Format('select topology.ST_RemEdgeNewFace(%1$L,  l.edge_id) from 
                       (select distinct edge_id from temp_table_fix_topo) as l,
                       %1$s.edge e
                       where l.edge_id = e.edge_id', _topology_name);
                EXECUTE command_string;
                command_string := Format('select ST_CollectionExtract(ST_Collect(distinct line),2) from temp_table_fix_topo');
                EXECUTE command_string INTO single_line_geo2;
                -- start loop throug each
                SELECT ST_NumGeometries (single_line_geo2) INTO dim2;
                --1
                WHILE i2 < dim2 LOOP
                  i2 := i2 + 1;
                  tmp_edge_geom2 := ST_GeometryN (single_line_geo2, i2);
                  BEGIN
                EXECUTE Format('INSERT INTO %s(line_geo_lost, error_info, d_state, d_msg, d_detail, d_hint, d_context, geo) VALUES(%L, %L, %L, %L, %L, %L, %L, %L)', no_cutline_filename, FALSE, 
                'Warn5, will do retry, topo_update.add_border_lines i2:'||i2||' dim2:'||dim2||' num_not_done_ok:'||num_not_done_ok||' max_num_not_done_ok:'||max_num_not_done_ok, 
                v_state, v_msg, v_detail, v_hint, v_context, tmp_edge_geom2);
	                  
                    command_string := Format('select topology.TopoGeo_addLinestring(%s,%L,%s)', Quote_literal(_topology_name), tmp_edge_geom2, _snap_tolerance );
                    EXECUTE command_string;
                    EXCEPTION
                    WHEN OTHERS THEN
                    done_ok := false;

                      RAISE NOTICE 'failed topo_update.add_border_lines ::::::::::::::::::::::::::::::::::::::::::::::::::: %', ST_GeometryType (tmp_edge_geom2);
                    GET STACKED DIAGNOSTICS v_state = RETURNED_SQLSTATE, v_msg = MESSAGE_TEXT, v_detail = PG_EXCEPTION_DETAIL, v_hint = PG_EXCEPTION_HINT,
                    v_context = PG_EXCEPTION_CONTEXT;
                    RAISE NOTICE 'failed1: state  : % message: % detail : % hint   : % context: %', v_state, v_msg, v_detail, v_hint, v_context;
                    IF  (max_num_not_done_ok = num_not_done_ok) THEN 
                      lost_data = true;
                    ELSE
                      lost_data = false;
                    END IF;
                    EXECUTE Format('INSERT INTO %s(line_geo_lost, error_info, d_state, d_msg, d_detail, d_hint, d_context, geo) VALUES(%L, %L, %L, %L, %L, %L, %L, %L)', 
                    no_cutline_filename, lost_data, 
                    'Failed1, at num ' || i2 || ' topo_update.add_border_lines where crosses_edge_num=' || crosses_edge_num || ' num_not_done_ok:' || num_not_done_ok, 
                    v_state, v_msg, v_detail, v_hint, v_context, tmp_edge_geom2);
                  END;
                END LOOP;
                EXCEPTION
                WHEN OTHERS THEN
                  RAISE NOTICE 'failed topo_update.add_border_lines ::::::::::::::::::::::::::::::::::::::::::::::::::: %', ST_GeometryType (tmp_egde_geom);
                GET STACKED DIAGNOSTICS v_state = RETURNED_SQLSTATE, v_msg = MESSAGE_TEXT, v_detail = PG_EXCEPTION_DETAIL, v_hint = PG_EXCEPTION_HINT,
                v_context = PG_EXCEPTION_CONTEXT;
                RAISE NOTICE 'failed2: state  : % message: % detail : % hint   : % context: %', v_state, v_msg, v_detail, v_hint, v_context;
                EXECUTE Format('INSERT INTO %s(line_geo_lost, error_info, d_state, d_msg, d_detail, d_hint, d_context, geo) VALUES(%L, %L, %L, %L, %L, %L, %L, %L)', no_cutline_filename, TRUE, 'Failed2, topo_update.add_border_lines', v_state, v_msg, v_detail, v_hint, v_context, new_egde_geom);
                -- 2
                END;
                EXIT WHEN num_not_done_ok > max_num_not_done_ok or done_ok = true;

              END LOOP;
              
              if (done_ok = false) THEN
                RAISE NOTICE 'failed2, done_ok = false state  : % message: % detail : % hint   : % context: %', v_state, v_msg, v_detail, v_hint, v_context;
                EXECUTE Format('INSERT INTO %s(line_geo_lost, error_info, d_state, d_msg, d_detail, d_hint, d_context, geo) VALUES(%L, %L, %L, %L, %L, %L, %L, %L)', no_cutline_filename, TRUE, 'Failed2, topo_update.add_border_lines', v_state, v_msg, v_detail, v_hint, v_context, new_egde_geom);
              END IF;

              -- done loop throug each
              DROP TABLE temp_table_fix_topo;
              --1
              END;
            END;
          END;
        ---
      END LOOP;
      EXCEPTION
      WHEN OTHERS THEN
        GET STACKED DIAGNOSTICS v_state = RETURNED_SQLSTATE, v_msg = MESSAGE_TEXT, v_detail = PG_EXCEPTION_DETAIL, v_hint = PG_EXCEPTION_HINT,
        v_context = PG_EXCEPTION_CONTEXT;
      RAISE NOTICE 'failed: state  : % message: % detail : % hint   : % context: %', v_state, v_msg, v_detail, v_hint, v_context;
      EXECUTE Format('INSERT INTO %s(line_geo_lost, error_info, d_state, d_msg, d_detail, d_hint, d_context, geo) VALUES(%L, %L, %L, %L, %L, %L, %L, %L)', no_cutline_filename, TRUE, 'Failed3, topo_update.add_border_lines ', v_state, v_msg, v_detail, v_hint, v_context, new_line);
      END;
    END;
  RETURN edges_added;
END;

$$
LANGUAGE plpgsql;

