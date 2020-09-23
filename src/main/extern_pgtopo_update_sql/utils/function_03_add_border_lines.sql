drop function if exists 
topo_update.add_border_lines (_topology_name character varying, 
_new_line_raw geometry, 
_snap_tolerance float, 
_table_name_result_prefix varchar,
_do_retry_add boolean);

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
    RAISE NOTICE 'First error  : % message: % detail : % hint   : % context: %', v_state, v_msg, v_detail, v_hint, v_context;

    -- return if not retry
    IF (_do_retry_add = false) THEN
      RAISE NOTICE '_do_retry_add is false, just log  : % message: % detail : % hint   : % context: %', v_state, v_msg, v_detail, v_hint, v_context;
      EXECUTE Format('INSERT INTO %s(line_geo_lost, error_info, d_state, d_msg, d_detail, d_hint, d_context, geo) 
                      VALUES(%L, %L, %L, %L, %L, %L, %L, %L)', 
                      no_cutline_filename, TRUE, 'Will not do retry because _do_retry_add is false ', v_state, v_msg, v_detail, v_hint, v_context, new_line);
      RETURN edges_added;
    END IF;

    
    -- return if deadlock
    SELECT Position('deadlock detected' IN v_msg) INTO deadlock_detected;
    IF (deadlock_detected > 0 ) THEN
       EXECUTE Format('INSERT INTO %s(line_geo_lost, error_info, d_state, d_msg, d_detail, d_hint, d_context, geo) 
       VALUES(%L, %L, %L, %L, %L, %L, %L, %L)', 
       no_cutline_filename, TRUE, 'Will not do retry because deadlock detected ', v_state, v_msg, v_detail, v_hint, v_context, new_line);
       -- Why do we raise an error here
       RAISE EXCEPTION 'failed: state deadlock detected : % message: % detail : % hint   : % context: %', v_state, v_msg, v_detail, v_hint, v_context;
    END IF;

    SELECT Position('geometry crosses edge' IN v_msg) INTO crosses_edge;
    -- If crosse egde error try to solve without 
    IF (crosses_edge > 0) THEN
      crosses_edge_num := Trim(Substring(v_msg FROM (crosses_edge + Char_length('geometry crosses edge'))))::Int;
      RAISE NOTICE 'crosses_edge % state % message: % detail : % hint   : % context: %', crosses_edge_num, v_state, v_msg, v_detail, v_hint, v_context;
     
      -- try with only linemerge
      BEGIN
        command_string := Format('SELECT ARRAY(SELECT topology.TopoGeo_addLinestring(%3$L,line,%1$s) from 
          (  
          select distinct (ST_Dump(ST_LineMerge(ST_Union(e.geom)))).geom as line
          from (
            select geom from %3$s.edge e where e.edge_id = %4$s
            union
            select %2$L as geom
            ) as e
          ) as e 
        )', 
        _snap_tolerance, _new_line_raw, _topology_name, crosses_edge_num);
        RAISE NOTICE 'command_string %', command_string;
        EXECUTE command_string into edges_added;
        
        EXECUTE Format('INSERT INTO %s(line_geo_lost, error_info, d_state, d_msg, d_detail, d_hint, d_context, geo) VALUES(%L, %L, %L, %L, %L, %L, %L, %L)', 
        no_cutline_filename, FALSE, 'ok to handle crosses_edge with line_merge only for crossing edge, topo_update.add_border_lines ', 
        null, null, null, null, null, new_line);

        RETURN edges_added;
      EXCEPTION
        WHEN OTHERS THEN
          GET STACKED DIAGNOSTICS v_state = RETURNED_SQLSTATE, v_msg = MESSAGE_TEXT, v_detail = PG_EXCEPTION_DETAIL, v_hint = PG_EXCEPTION_HINT,
          v_context = PG_EXCEPTION_CONTEXT;
        RAISE NOTICE 'failed to handle crosses_edge with line_merge only for crossing edge % :% message: % detail : % hint   : % context: %', 
        crosses_edge_num, v_state, v_msg, v_detail, v_hint, v_context;
        EXECUTE Format('INSERT INTO %s(line_geo_lost, error_info, d_state, d_msg, d_detail, d_hint, d_context, geo) VALUES(%L, %L, %L, %L, %L, %L, %L, %L)', 
        no_cutline_filename, FALSE, 'failed to handle crosses_edge with line_merge only for crossing edge, topo_update.add_border_lines ', v_state, v_msg, v_detail, v_hint, v_context, new_line);
      END;
      
      -- try with delete and the line merge
      BEGIN
        CREATE TEMP table temp_table_fix_topo_crosses_edge_delete_delete(line geometry);
        command_string := Format('INSERT into temp_table_fix_topo_crosses_edge_delete_delete(line)  
        select distinct (ST_Dump(ST_LineMerge(ST_Union(e.geom)))).geom as line
        from (
          select geom from %3$s.edge e where e.edge_id = %4$s
          union
          select %2$L as geom
        ) as e', _snap_tolerance, _new_line_raw, _topology_name, crosses_edge_num);
        
        RAISE NOTICE 'command_string %', command_string;
        EXECUTE command_string;

        command_string := Format('select topology.ST_RemEdgeNewFace(%1$L, %2$s )'
        , _topology_name, crosses_edge_num);
        EXECUTE command_string;
        
        command_string := Format('SELECT ARRAY(SELECT topology.TopoGeo_addLinestring(%L,line,%s) 
        from temp_table_fix_topo_crosses_edge_delete_delete )'
        ,_topology_name , _snap_tolerance);
        
        RAISE NOTICE 'will try to % lines, by command_string %', 
        (select count(*) from temp_table_fix_topo_crosses_edge_delete_delete)::int, command_string;
   
        EXECUTE command_string into edges_added;
 
        EXECUTE Format('INSERT INTO %s(line_geo_lost, error_info, d_state, d_msg, d_detail, d_hint, d_context, geo) VALUES(%L, %L, %L, %L, %L, %L, %L, %L)', 
        no_cutline_filename, FALSE, 'ok to handle crosses_edge with delete crossing edge, topo_update.add_border_lines ', 
        null, null, null, null, null, new_line);
 
        drop table if exists temp_table_fix_topo_crosses_edge_delete_delete;
        RETURN edges_added;
      EXCEPTION
        WHEN OTHERS THEN
          GET STACKED DIAGNOSTICS v_state = RETURNED_SQLSTATE, v_msg = MESSAGE_TEXT, v_detail = PG_EXCEPTION_DETAIL, v_hint = PG_EXCEPTION_HINT,
          v_context = PG_EXCEPTION_CONTEXT;
        RAISE NOTICE 'failed to handle crosses_edge with delete crossing edge % :% message: % detail : % hint   : % context: %', 
        crosses_edge_num, v_state, v_msg, v_detail, v_hint, v_context;
        EXECUTE Format('INSERT INTO %s(line_geo_lost, error_info, d_state, d_msg, d_detail, d_hint, d_context, geo) VALUES(%L, %L, %L, %L, %L, %L, %L, %L)', 
        no_cutline_filename, FALSE, 'failed to handle crosses_edge with delete crossing edge, topo_update.add_border_lines ', 
        v_state, v_msg, v_detail, v_hint, v_context, new_line);
        drop table if exists temp_table_fix_topo_crosses_edge_delete_delete;

      END;
    END IF;

    IF (_snap_tolerance = 0)
    THEN
      RAISE NOTICE 'failed: state deadlock detected or _snap_tolerance = 0  : % message: % detail : % hint   : % context: %', v_state, v_msg, v_detail, v_hint, v_context;
      EXECUTE Format('INSERT INTO %s(line_geo_lost, error_info, d_state, d_msg, d_detail, d_hint, d_context, geo) 
                      VALUES(%L, %L, %L, %L, %L, %L, %L, %L)', 
                      no_cutline_filename, TRUE, 'Failed3, topo_update.add_border_lines ', v_state, v_msg, v_detail, v_hint, v_context, new_line);
    END IF;
     
    tolerance_retry_num := 1;
    tolerance_retry_diff := 0.75;
    tolerance_retry_value := _snap_tolerance*tolerance_retry_num*tolerance_retry_diff;
    
    EXECUTE Format('INSERT INTO %s(line_geo_lost, error_info, d_state, d_msg, d_detail, d_hint, d_context, geo) VALUES(%L, %L, %L, %L, %L, %L, %L, %L)', no_cutline_filename, FALSE, 
    'Warn2 before loop, will do retry, topo_update.add_border_lines with tolerance :'||tolerance_retry_value||' and tolerance_retry_num '|| tolerance_retry_num ||'for topology '||_topology_name, 
    v_state, v_msg, v_detail, v_hint, v_context, new_line);
    
   
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
      'Warn2 in loop, will do retry, topo_update.add_border_lines with tolerance :'||tolerance_retry_value||' and tolerance_retry_num '|| tolerance_retry_num ||'for topology '||_topology_name, 
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



--select topology.DropTopology ('test01_fail');
----
--select topology.CreateTopology ('test01_fail', 4258, 1e-06);
--
--select topology.TopoGeo_addLinestring('test01_fail','0102000020A210000019000000ACFF73982FC326404B9D369927344E400127254E38C32640C2A5BE8726344E40D77CA6A844C32640CEAF9C7326344E40D381ACA756C32640E1AAFC7C25344E401DF0AFD469C32640350BB43B24344E408C88BDAB79C32640B862354C23344E40AC7713D78CC32640D006600322344E403AAE4676A5C326402B27EB8120344E40F4A3E194B9C326401FAD7B751F344E408C73E5FDC9C32640259B61591F344E400987DEE2E1C32640F5E27ACA20344E40C64CA25EF0C32640C49A255C23344E40AA0029FBF8C32640453FBF9426344E408EE8F92E00C42640D246BFC72A344E40DF2A99AD06C426404D147C2E2E344E40E0AF13A80FC42640BDC3EDD030344E405458045A15C42640155FA39B33344E4098480E7D1CC426407F37386C36344E40A09630E422C426401894693439344E40CD4AA47E28C426408E4B66063B344E401EC18D942DC42640103EEF213C344E40C896E5EB32C426406F719EFB3C344E406D872B0C36C426409FDB95A73D344E40E6142EF53DC426404531D4BC3E344E406A6F4B3F3CC426405CB521B53F344E40',1e-06);
--select topology.TopoGeo_addLinestring('test01_fail','0102000020A2100000040000006A6F4B3F3CC426405CB521B53F344E40C62A4AAE07C4264090943EBE4E344E40C6CE256607C42640BF2FE4C74E344E407EBF3D74E6C3264032E0D16B58344E40',1e-06);
--select topology.TopoGeo_addLinestring('test01_fail','0102000020A21000000200000040F09D3D3CC4264026158DB53F344E406A6F4B3F3CC426405CB521B53F344E40',1e-06);
--select topology.TopoGeo_addLinestring('test01_fail','0102000020A210000002000000E24A87985CC42640BAF3C47336344E406A6F4B3F3CC426405CB521B53F344E40',1e-06);
--select topology.TopoGeo_addLinestring('test01_fail','0102000020A21000001E0000000EEC42BD2AC526400E61A17BFB334E40CEF4B7A926C52640730F09DFFB334E40B08ADC781CC526406675BC13FD334E40EADB93D112C52640AD94AF15FE334E409558BED309C52640D1F4C8D5FE334E40BC4681F403C52640CAD53494FF334E409A5F28BBF4C426407BD058A002344E40D0CBCDDCE8C42640E5D6FF9405344E40CE5AC0BAE0C4264090FDE20808344E40F37B5171D2C42640643BDF4F0D344E40A58DD948C8C426408C07A57911344E40852BFB09C2C42640CCA843B813344E40CCA66D0DB6C42640DDDFEA4A15344E40B475CB69AAC426400CC0AB8A15344E40D6D127A897C42640ADA0698915344E40765BD88981C4264054104DEA15344E405C68F86063C42640651C7E9216344E4035E4446051C42640EEB7D15B17344E40D20D5E0542C4264094C8F43A18344E40CE9BD4E132C426406A15FDA119344E401F3D8F9B2BC426402DF713DF1A344E405F03C70A23C42640E4C814BD1D344E403CBD529621C42640FB5D335420344E40123EA59421C426400B4A873D23344E406310B3F226C426407461FFD027344E40AA91A7F633C42640361D01DC2C344E40F73768AF3EC426404665790D30344E4081D1408754C426401AD1877835344E4007F98F3B5BC4264049809A5A36344E40E24A87985CC42640BAF3C47336344E40',1e-06);
--select topology.TopoGeo_addLinestring('test01_fail','0102000020A2100000020000000EEC42BD2AC526400E61A17BFB334E40E24A87985CC42640BAF3C47336344E40',1e-06);
--
--
--select topo_update.add_border_lines('test01_fail',
--'0102000020A210000025000000E24A87985CC42640BAF3C47336344E40573B2FBA61C426406163A8D436344E4070C0F8B369C42640E3C85E4A38344E409D746C4E6FC4264077F52A323A344E4077BAF3C473C42640BD569D303C344E40450A0A2879C42640C8FB82273E344E407AB07BE184C42640BAE2981B42344E407E75B05989C42640E341695E44344E40E5F3E56091C4264041FCB26A46344E400A3BD6D699C4264016BCE82B48344E40DEEBFF779FC426402D54A3FC49344E404C7B945BA4C4264097D3AFBE4B344E4060FB6E5FACC4264013D5B6BC4D344E4087AAF303B2C42640669C2BEF4F344E408F0C8343B9C42640F439D27451344E40E03AB5E9BEC426406A1FE16A53344E4013848659C3C42640920B845355344E40B33396FAC3C426406239F87857344E407E969D34C3C42640EA44DD6259344E403E0FA441C0C42640E9C8DB5B5B344E40ED98BA2BBBC42640E202D0285D344E401166248CB0C42640DB28571D5E344E40233031F1A2C42640476C1C565D344E40F35A649698C426408F66D1E05B344E4067E66E7C92C42640C69970F959344E40BEB4F2DC8CC42640BBF48A0258344E40A1BB24CE8AC42640C8A87C1956344E405251AB9E82C426404102902452344E40603F1FC07DC4264036D0D78750344E40F763496F6EC42640E49B6D6E4C344E40AEA29AED65C42640BC81B8614A344E40B91391065CC4264058F0918348344E400AB54BC054C426403B3CDFAA46344E409E094D124BC42640FA3B80EA44344E40D782DE1B43C42640D7B0F03F43344E4085483C8F40C426404F77F93141344E4040F09D3D3CC4264026158DB53F344E40',
--1e-06,
--'test_topo_ar5_t5.ar5_2019_komm_flate',
--true
--);

