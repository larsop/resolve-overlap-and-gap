-- CREATE : OK 07/09/2017
-- TEST :

DROP FUNCTION IF EXISTS  topo_update.simplefeature_2_topo_surface_border_retry(input_table_name character varying, 
input_table_geo_column_name character varying, 
input_table_pk_column_name character varying, 
_topology_name character varying,
_simplify_tolerance double precision, 
_snap_tolerance double precision, _do_chaikins boolean, 
_job_list_name character varying, 
bb geometry, inside_cell_data boolean
); 


-- "SELECT topo_update.simplefeature_c2_topo_surface_border_retry(
---'test_data.overlap_gap_input_t1',
--'geom',
--'t1',
--'test_topo',
--1,
--1,
--'false'
--,'test_data.overlap_gap_input_t1_res_job_list'
--,'0103000020A210000001000000050000001C6A3831245D1040AA67C75EA8274E401C6A3831245D10405E4A05E660944E4032D869330E4817405E4A05E660944E4032D869330E481740AA67C75EA8274E401C6A3831245D1040AA67C75EA8274E40'
--,true);

CREATE OR REPLACE FUNCTION topo_update.simplefeature_c2_topo_surface_border_retry(
input_table_name character varying, 
input_table_geo_column_name character varying, 
input_table_pk_column_name character varying, 
_topology_name character varying,
_simplify_tolerance double precision, 
_snap_tolerance double precision, 
_do_chaikins boolean, 
_job_list_name character varying, 
bb geometry, inside_cell_data boolean)
 RETURNS integer
AS $$
DECLARE

        border_topo_info topo_update.input_meta_info ;
        
  -- holds dynamic sql to be able to use the same code for different
        command_string text;
    added_rows int = 0;

    start_time timestamp with time zone;
    done_time timestamp with time zone;
    used_time real;
    start_remove_small timestamp with time zone;

 is_done integer = 0;

   area_to_block geometry;
   
   num_boxes_intersect integer;
   num_boxes_free integer;
   
   num_rows_removed integer;
   box_id integer;
   face_table_name varchar;
   -- This is used when adding lines hte tolrannce is different when adding lines inside and box and the border;
   snap_tolerance_fixed float = 0.000001;
   glue_snap_tolerance_fixed float = 0.0000001;
   

BEGIN 

        -- check if job is done already
        command_string := format('select count(*) from %s as gt, %s as done
  where gt.cell_geo && ST_PointOnSurface(%3$L) and gt.id = done.id',_job_list_name,_job_list_name||'_donejobs',bb);
        execute command_string into is_done;
        
        IF is_done = 1 THEN
             RAISE NOTICE 'Job is_done for  : %', ST_astext(bb);
             RETURN 0;
        END IF;

        start_time  := clock_timestamp();

        RAISE NOTICE 'enter work at timeofday:% for layer %, with inside_cell_data %', timeofday(),  _topology_name||'_'||box_id, inside_cell_data;


        IF bb is NULL and input_table_name is not null  THEN
                command_string := format('select ST_Envelope(ST_Collect(geo)) from %s',input_table_name);
                EXECUTE command_string into bb;
        END IF;
    
        -- get area to block and set
        area_to_block := topo_update.set_blocked_area(input_table_name,input_table_geo_column_name,input_table_pk_column_name,_job_list_name,bb);
        RAISE NOTICE 'area to block:% ', area_to_block;


        border_topo_info.snap_tolerance :=  _simplify_tolerance;
--      --border_topo_info.border_layer_id = 317;
        

        RAISE NOTICE 'start work at timeofday:% for layer %, with inside_cell_data %', timeofday(), _topology_name||'_'||box_id, inside_cell_data;

        
        IF inside_cell_data THEN

                
                command_string := format('select id from %1$s where cell_geo = %2$L',_job_list_name,bb);
                RAISE NOTICE '% ', command_string;
                EXECUTE command_string into box_id;

        
                border_topo_info.topology_name := _topology_name||'_'||box_id;
                

                RAISE NOTICE 'use border_topo_info.topology_name %', border_topo_info.topology_name;


                BEGIN
                        perform topology.DropTopology(border_topo_info.topology_name);
                        EXECUTE command_string into box_id;
                EXCEPTION WHEN OTHERS THEN
                        RAISE NOTICE 'failed to drop topology % ', border_topo_info.topology_name;
                END;
                
                        

                perform topology.CreateTopology(border_topo_info.topology_name,4258,snap_tolerance_fixed);

                EXECUTE format('ALTER table %s.edge_data set unlogged',border_topo_info.topology_name);
                EXECUTE format('ALTER table %s.node set unlogged',border_topo_info.topology_name);
                EXECUTE format('ALTER table %s.face set unlogged',border_topo_info.topology_name);
                EXECUTE format('ALTER table %s.relation set unlogged',border_topo_info.topology_name);

                -- get the siple feature data both the line_types and the inner lines.
                -- the boundery linnes are saved in a table for later usage
                drop table if exists tmp_simplified_border_lines;
                command_string := format(
                'create temp table tmp_simplified_border_lines as (select g.* FROM topo_update.get_simplified_border_lines(%L,%L,%L,%L,%L) g)',
                input_table_name,input_table_geo_column_name,bb,_simplify_tolerance,_do_chaikins);
                RAISE NOTICE 'command_string %' , command_string;
                EXECUTE command_string;
        
                -- add the glue line with no/small tolerance
                border_topo_info.snap_tolerance :=  glue_snap_tolerance_fixed;
                command_string := format(
                'SELECT topo_update.create_nocutline_edge_domain_obj_retry(json::text, %L) 
                FROM tmp_simplified_border_lines g where line_type = 1',
                border_topo_info);
                RAISE NOTICE 'command_string %' , command_string;
                EXECUTE command_string;
                
                -- add lines aleday added som we get he same break
                border_topo_info.snap_tolerance :=  snap_tolerance_fixed;
                command_string := format(
                'SELECT topo_update.create_nocutline_edge_domain_obj_retry(json::text, %L) 
                FROM tmp_simplified_border_lines g where line_type = 2',
                border_topo_info);
--              RAISE NOTICE 'command_string %' , command_string;
--              EXECUTE command_string;
                

                -- add to glue lijnes to the finale result
                -- NB We have to use snap less that one meter to avpid snapping across cell
                command_string := format('SELECT topo_update.add_border_lines(%3$L,r.geom,%1$s) FROM (
                SELECT geom from  %2$s.edge ) as r'
                , snap_tolerance_fixed, border_topo_info.topology_name,_topology_name);
                
        
                -- using the input tolreance for adding
                border_topo_info.snap_tolerance :=  snap_tolerance_fixed;
                command_string := format(
                'SELECT topo_update.create_nocutline_edge_domain_obj_retry(json::text, %L) 
                FROM tmp_simplified_border_lines g where line_type = 0',
                border_topo_info);
                RAISE NOTICE 'command_string %' , command_string;
                EXECUTE command_string;

                
                   
                face_table_name = border_topo_info.topology_name||'.face';
                
                start_remove_small := clock_timestamp();

                RAISE NOTICE 'Start clean small polygons for face_table_name % at %' , face_table_name, clock_timestamp();
            -- remove small polygons in temp 
            num_rows_removed := topo_update.do_remove_small_areas_no_block(border_topo_info.topology_name,face_table_name ,'mbr','face_id',_job_list_name, bb, true );
            used_time :=  (EXTRACT(EPOCH FROM (clock_timestamp() - start_remove_small)));

                RAISE NOTICE 'Removed % clean small polygons for face_table_name % at % used_time: %', num_rows_removed, face_table_name, clock_timestamp(), used_time;
        
                -- get valid faces and thise eges that touch out biedery 
                -------------- this does not work
                command_string := format('
WITH lg as (
SELECT 
topology.ST_GetFaceGeometry(%2$s,lg.face_id) as geom 
from  %3$s.face lg where ST_Area(mbr) > 100
),
lg2 as (
select (ST_DumpRings((st_dump(lg.geom)).geom)).geom from lg where lg.geom is not null and ST_area(lg.geom) > 49
),
r as (SELECT (ST_Dump(ST_LineMerge(ST_ExteriorRing(lg2.geom)))).geom
from lg2)
SELECT topo_update.add_border_lines(%4$L,r.geom,%1$s) FROM r', 
_snap_tolerance, quote_literal(border_topo_info.topology_name), border_topo_info.topology_name, _topology_name);
--              RAISE NOTICE 'command_string %' , command_string;
--              EXECUTE command_string;
                
                -- add to finale result
                ------- this does not work 
                command_string := format('SELECT topo_update.add_border_lines(%4$L,r.geom,%1$s) FROM (
                SELECT geom from  %2$s.edge where ST_DWithin(geom,%3$L,0.6) is true) as r'
                , _snap_tolerance, border_topo_info.topology_name,ST_ExteriorRing(bb),_topology_name);
--              RAISE NOTICE 'command_string %' , command_string;
--              EXECUTE command_string;

-- add to finale result
--TODO make a test for final result or not
--if (_do_chaikins is false) THEN
--              command_string := format('SELECT topo_update.add_border_lines(r.geom,%1$s) FROM (
--              SELECT geom from  %2$s.edge) as r'
--              , _snap_tolerance, border_topo_info.topology_name,ST_ExteriorRing(bb));
--              RAISE NOTICE 'command_string %' , command_string;
--              EXECUTE command_string;
--ELSE
--              ------- this does not work 
--              command_string := format('SELECT topo_update.add_border_lines(r.geom,%1$s) FROM (
--              SELECT e1.geom from  %2$s.edge e1, tmp_sf_ar5_forest_input.not_selected_forest_area p 
--where ST_CoveredBy(p.wkb_geometry,%3$L) and ST_CoveredBy(e1.geom,p.wkb_geometry) is false) as r'
--              , _snap_tolerance, border_topo_info.topology_name,bb);
--              RAISE NOTICE 'command_string %' , command_string;
--              EXECUTE command_string;
--
--END IF;

                command_string := format('SELECT topo_update.add_border_lines(%4$L,r.geom,%1$s) FROM (
                SELECT geom from  %2$s.edge) as r'
                , _snap_tolerance, border_topo_info.topology_name,ST_ExteriorRing(bb),_topology_name);
                RAISE NOTICE 'command_string %' , command_string;
                EXECUTE command_string;

                -- analyze table topo_ar5_forest_sysdata.face;
            -- remove small polygons in main table
--              num_rows_removed := topo_update.do_remove_small_areas_no_block(border_topo_info.topology_name,'topo_ar5_forest_sysdata.face' ,'mbr','face_id',_job_list_name ,bb );
--              RAISE NOTICE 'Removed % small polygons in face_table_name %', num_rows_removed, 'topo_ar5_forest_sysdata.face';

                BEGIN
                        perform topology.DropTopology(border_topo_info.topology_name);
                        EXECUTE command_string into box_id;
                EXCEPTION WHEN OTHERS THEN
                        RAISE NOTICE 'failed to drop topology % ', border_topo_info.topology_name;
                END;
        ELSE

                -- test with  area to block like bb
        -- area_to_block := bb;
        -- count the number of rows that intersects
        command_string := format('select count(*) from %1$s where cell_geo && %2$L and ST_intersects(cell_geo,%2$L);',_job_list_name,area_to_block);
        EXECUTE command_string INTO num_boxes_intersect;

                command_string := format('select count(*) from (select * from %1$s where cell_geo && %2$L and ST_intersects(cell_geo,%2$L) for update SKIP LOCKED) as r;',_job_list_name,area_to_block);
                EXECUTE command_string into num_boxes_free;
            IF num_boxes_intersect != num_boxes_free THEN
                RETURN -1;
            END IF;
                
        
                border_topo_info.topology_name := _topology_name;

                -- NB We have to use fixed snap to here to be sure that lines snapp
                command_string := format(
                'SELECT topo_update.do_add_border_lines(_topology_name,%L,%s)',bb,snap_tolerance_fixed);
                EXECUTE command_string;

                
        
        END IF;

        RAISE NOTICE 'done work at timeofday:% for layer %, with inside_cell_data %', timeofday(), border_topo_info.topology_name, inside_cell_data;

        command_string := format('update %1$s set block_bb = %2$L where cell_geo = %3$L',_job_list_name,bb,bb);
        RAISE NOTICE '% ', command_string;
        EXECUTE command_string;
        

                        
        RAISE NOTICE 'timeofday:% ,done job nocutline ready to start next', timeofday();

        done_time  := clock_timestamp();
        used_time :=  (EXTRACT(EPOCH FROM (done_time - start_time)));
        RAISE NOTICE 'work done proc :% border_layer_id %, using % sec', done_time, border_topo_info.border_layer_id, used_time;

-- This is a list of lines that fails
-- this is used for debug

        IF used_time > 10 THEN
                RAISE NOTICE 'very long a set of lines % time with geo for bb % ', used_time, bb;
                insert into topo_update.long_time_log2(execute_time,info,sql,geo) 
                values(used_time,'simplefeature_c2_topo_surface_border_retry',command_string, bb);
        END IF;

 

    perform topo_update.clear_blocked_area(bb,_job_list_name);
    
        RAISE NOTICE 'leave work at timeofday:% for layer %, with inside_cell_data %', timeofday(), border_topo_info.topology_name, inside_cell_data;

    return added_rows;
    
    END;
$$ LANGUAGE plpgsql;

--some testing
--SELECT topo_update.simplefeature_c2_topo_surface_border_retry('test_data.overlap_gap_input_t1','geom','c1','test_topo',1e-05,1e-05,'false','test_data.overlap_gap_input_t1_res_job_list','0103000020A210000001000000050000001C6A3831245D1040AA67C75EA8274E401C6A3831245D10405E4A05E660944E4032D869330E4817405E4A05E660944E4032D869330E481740AA67C75EA8274E401C6A3831245D1040AA67C75EA8274E40',true);
--SELECT topo_update.simplefeature_c2_topo_surface_border_retry('test_data.overlap_gap_input_t1','geom','c1','test_topo',1e-05,1e-05,'false','test_data.overlap_gap_input_t1_res_job_list','0103000020A210000001000000050000001C6A3831245D1040AA67C75EA8274E401C6A3831245D10405E4A05E660944E4032D869330E4817405E4A05E660944E4032D869330E481740AA67C75EA8274E401C6A3831245D1040AA67C75EA8274E40',true);
--SELECT topo_update.simplefeature_c2_topo_surface_border_retry('test_data.overlap_gap_input_t1','geom','c1','test_topo',1e-05,1e-05,'false','test_data.overlap_gap_input_t1_res_job_list','0103000020A210000001000000050000003911FF1C66042640AA67C75EA8274E403911FF1C660426405E4A05E660944E4044C8171EDB7929405E4A05E660944E4044C8171EDB792940AA67C75EA8274E403911FF1C66042640AA67C75EA8274E40',true);
--SELECT topo_update.simplefeature_c2_topo_surface_border_retry('test_data.overlap_gap_input_t1','geom','c1','test_topo',1e-05,1e-05,'false','test_data.overlap_gap_input_t1_res_job_list','0103000020A21000000100000005000000B4B5729CAB492440F68489D7EFBA4D40B4B5729CAB492440A3FD58F91DD64D4076E3B8DC08272540A3FD58F91DD64D4076E3B8DC08272540F68489D7EFBA4D40B4B5729CAB492440F68489D7EFBA4D40',true);
--SELECT topo_update.simplefeature_c2_topo_surface_border_retry('test_data.overlap_gap_input_t1','geom','c1','test_topo',1e-05,1e-05,'false','test_data.overlap_gap_input_t1_res_job_list','0103000020A210000001000000050000003C8F823483BD1A406C1EE2B075374F403C8F823483BD1A40C60F81F4D16D4F4047469B35F8321E40C60F81F4D16D4F4047469B35F8321E406C1EE2B075374F403C8F823483BD1A406C1EE2B075374F40',true);
--SELECT topo_update.simplefeature_c2_topo_surface_border_retry('test_data.overlap_gap_input_t1','geom','c1','test_topo',1e-05,1e-05,'false','test_data.overlap_gap_input_t1_res_job_list','0103000020A21000000100000005000000BE6C8B9D20BF2740F68489D7EFBA4D40BE6C8B9D20BF27405076281B4CF14D4044C8171EDB7929405076281B4CF14D4044C8171EDB792940F68489D7EFBA4D40BE6C8B9D20BF2740F68489D7EFBA4D40',true);
--SELECT topo_update.simplefeature_c2_topo_surface_border_retry('test_data.overlap_gap_input_t1','geom','c1','test_topo',1e-05,1e-05,'false','test_data.overlap_gap_input_t1_res_job_list','0103000020A21000000100000005000000C864E21207D83840976A7E81A1235040C864E21207D83840FF2FFA8F12FD5040DED21315F1C23F40FF2FFA8F12FD5040DED21315F1C23F40976A7E81A1235040C864E21207D83840976A7E81A1235040',true);
--SELECT topo_update.simplefeature_c2_topo_surface_border_retry('test_data.overlap_gap_input_t1','geom','c1','test_topo',1e-05,1e-05,'false','test_data.overlap_gap_input_t1_res_job_list','0103000020A210000001000000050000004E7F301F50EF2C40B3123817CB6951404E7F301F50EF2C4067F5759E83D65140B2F6B0101DED314067F5759E83D65140B2F6B0101DED3140B3123817CB6951404E7F301F50EF2C40B3123817CB695140',true);
--SELECT topo_update.simplefeature_c2_topo_surface_border_retry('test_data.overlap_gap_input_t1','geom','c1','test_topo',1e-05,1e-05,'false','test_data.overlap_gap_input_t1_res_job_list','0103000020A2100000010000000500000076E3B8DC08272540F68489D7EFBA4D4076E3B8DC08272540A3FD58F91DD64D403911FF1C66042640A3FD58F91DD64D403911FF1C66042640F68489D7EFBA4D4076E3B8DC08272540F68489D7EFBA4D40',true);
--SELECT topo_update.simplefeature_c2_topo_surface_border_retry('test_data.overlap_gap_input_t1','geom','c1','test_topo',1e-05,1e-05,'false','test_data.overlap_gap_input_t1_res_job_list','0103000020A2100000010000000500000032D869330E4817406C1EE2B075374F4032D869330E481740C60F81F4D16D4F403C8F823483BD1A40C60F81F4D16D4F403C8F823483BD1A406C1EE2B075374F4032D869330E4817406C1EE2B075374F40',true);
--SELECT topo_update.simplefeature_c2_topo_surface_border_retry('test_data.overlap_gap_input_t1','geom','c1','test_topo',1e-05,1e-05,'false','test_data.overlap_gap_input_t1_res_job_list','0103000020A210000001000000050000002E5AE61BF18E22409C93EA9393844D402E5AE61BF18E2240F68489D7EFBA4D40B4B5729CAB492440F68489D7EFBA4D40B4B5729CAB4924409C93EA9393844D402E5AE61BF18E22409C93EA9393844D40',true);
--SELECT topo_update.simplefeature_c2_topo_surface_border_retry('test_data.overlap_gap_input_t1','geom','c1','test_topo',1e-05,1e-05,'false','test_data.overlap_gap_input_t1_res_job_list','0103000020A21000000100000005000000B2F6B0101DED3140976A7E81A1235040B2F6B0101DED3140FF2FFA8F12FD5040C864E21207D83840FF2FFA8F12FD5040C864E21207D83840976A7E81A1235040B2F6B0101DED3140976A7E81A1235040',true);
--SELECT topo_update.simplefeature_c2_topo_surface_border_retry('test_data.overlap_gap_input_t1','geom','c1','test_topo',1e-05,1e-05,'false','test_data.overlap_gap_input_t1_res_job_list','0103000020A2100000010000000500000044C8171EDB792940976A7E81A123504044C8171EDB792940F15B1DC5FD5950404E7F301F50EF2C40F15B1DC5FD5950404E7F301F50EF2C40976A7E81A123504044C8171EDB792940976A7E81A1235040',true);
--SELECT topo_update.simplefeature_c2_topo_surface_border_retry('test_data.overlap_gap_input_t1','geom','c1','test_topo',1e-05,1e-05,'false','test_data.overlap_gap_input_t1_res_job_list','0103000020A2100000010000000500000047469B35F8321E407AF2BE7B8ADA4F4047469B35F8321E40976A7E81A12350402E5AE61BF18E2240976A7E81A12350402E5AE61BF18E22407AF2BE7B8ADA4F4047469B35F8321E407AF2BE7B8ADA4F40',true);
--SELECT topo_update.simplefeature_c2_topo_surface_border_retry('test_data.overlap_gap_input_t1','geom','c1','test_topo',1e-05,1e-05,'false','test_data.overlap_gap_input_t1_res_job_list','0103000020A210000001000000050000004E7F301F50EF2C404B4DBC085A9050404E7F301F50EF2C40FF2FFA8F12FD5040B2F6B0101DED3140FF2FFA8F12FD5040B2F6B0101DED31404B4DBC085A9050404E7F301F50EF2C404B4DBC085A905040',true);
--SELECT topo_update.simplefeature_c2_topo_surface_border_retry('test_data.overlap_gap_input_t1','geom','c1','test_topo',1e-05,1e-05,'false','test_data.overlap_gap_input_t1_res_job_list','0103000020A2100000010000000500000032D869330E481740AA67C75EA8274E4032D869330E4817405E4A05E660944E4047469B35F8321E405E4A05E660944E4047469B35F8321E40AA67C75EA8274E4032D869330E481740AA67C75EA8274E40',true);
--SELECT topo_update.simplefeature_c2_topo_surface_border_retry('test_data.overlap_gap_input_t1','geom','c1','test_topo',1e-05,1e-05,'false','test_data.overlap_gap_input_t1_res_job_list','0103000020A2100000010000000500000047469B35F8321E405E4A05E660944E4047469B35F8321E40122D436D19014F402E5AE61BF18E2240122D436D19014F402E5AE61BF18E22405E4A05E660944E4047469B35F8321E405E4A05E660944E40',true);
--SELECT topo_update.simplefeature_c2_topo_surface_border_retry('test_data.overlap_gap_input_t1','geom','c1','test_topo',1e-05,1e-05,'false','test_data.overlap_gap_input_t1_res_job_list','0103000020A210000001000000050000004E7F301F50EF2C40FF2FFA8F12FD50404E7F301F50EF2C40592199D36E3351402C9B249062323040592199D36E3351402C9B249062323040FF2FFA8F12FD50404E7F301F50EF2C40FF2FFA8F12FD5040',true);
--SELECT topo_update.simplefeature_c2_topo_surface_border_retry('test_data.overlap_gap_input_t1','geom','c1','test_topo',1e-05,1e-05,'false','test_data.overlap_gap_input_t1_res_job_list','0103000020A210000001000000050000004E7F301F50EF2C40976A7E81A12350404E7F301F50EF2C404B4DBC085A905040B2F6B0