CREATE OR REPLACE FUNCTION topo_update.do_healedges_no_block (_atopology varchar, _bb geometry, _outer_cell_boundary_lines geometry default null)
  RETURNS integer
  LANGUAGE 'plpgsql'
  AS $function$
DECLARE
  command_string text;
  loop_nr int = 0;
  max_loops int = 15;
  
  edges_to_fix  integer[][]; 
  edges integer[]; 
  edge_ids_found int; 
  
  heal_result int; 
  edges_fixed int; 
  edges_mising int; 

  start_time_delta_job timestamp WITH time zone;

  
BEGIN
	
  LOOP
    start_time_delta_job := Clock_timestamp();

    loop_nr := loop_nr + 1;
    
    command_string := Format('SELECT ARRAY( SELECT ARRAY[r.edge_to_live, r.edge_to_eat] 
    FROM (
        SELECT DISTINCT ON (r.edge_to_eat) r.edge_to_eat, r.edge_to_live
        from (
        SELECT 
        r.edge_to_live,
        r.edge_to_eat
        FROM 
        (   
            SELECT e1.geom as edge_to_eat_geom, e1.edge_id as edge_to_eat, e2.edge_id as edge_to_live
            FROM
            %1$s.edge_data e1,
            %1$s.edge_data e2,
    		%1$s.node n1,
            (
                select r.node_id as node_id 
                from (
                    select count(n1.node_id) num_edges_end_here, n1.node_id as node_id 
                    from
                    %1$s.node n1,
                    %1$s.edge_data e1
                    where e1.geom &&  %2$L and
    				n1.geom &&  %2$L and
    				ST_intersects(e1.geom,%2$L) and 
                    (ST_Disjoint(e1.geom,%3$L) OR %3$L is null) 
                    and
                    (e1.start_node = n1.node_id or e1.end_node = n1.node_id)
                    group by n1.node_id
                ) as r
                where r.num_edges_end_here = 2
            ) as r
            where (e1.start_node = r.node_id or e1.end_node = r.node_id) and
            (e2.start_node = r.node_id or e2.end_node = r.node_id) and
    		r.node_id = n1.node_id and
            e1.start_node != e1.end_node and
            e2.start_node != e2.end_node and --to avoid a closed surface ending in a line 

            -- we have to add this because we have big face that covers very man polygons
            -- and then an edge should be healded even if two face on the sane are not equal
            --(
            --  (e2.left_face = e1.left_face and e2.right_face = e1.right_face ) or 
            --  (e2.left_face = e2.right_face or e1.left_face = e1.right_face)
            --) and
            e2.left_face = e1.left_face and e2.right_face = e1.right_face and
            ST_CoveredBy(n1.geom,%2$L) and 
            (ST_Disjoint(e1.geom,%3$L) OR %3$L is null) and (ST_Disjoint(e2.geom,%3$L) OR %3$L is null)
        ) as r
        ) as r
        order by edge_to_eat
    ) as r
    where r.edge_to_live != r.edge_to_eat)', _atopology, _bb, _outer_cell_boundary_lines);
    EXECUTE command_string into edges_to_fix;

    RAISE NOTICE 'In heal loop nr % , did find % edes to heal lines for topo % and bb % at % used_time %', 
    loop_nr, (Array_length(edges_to_fix, 1)), _atopology, _bb, Clock_timestamp(), (Extract(EPOCH FROM (Clock_timestamp() - start_time_delta_job)));

    -- if I heal more than one each time I get a lot this  NOTICE:  00000: FAILED select ST_ModEdgeHeal('test_topo_ar50_t3', 852, 11601) state  : XX000  message: SQL/MM Spatial exception - non-existent edge 852 detail :  hint   :  context: SQL statement "SELECT topology.ST_ModEdgeHeal (_atopology, _edge_to_live, _edge_to_eat)"
    --	RAISE NOTICE 'execute command_string; %', command_string;
    
   edges_fixed := 0;
   edges_mising := 0;
   
   IF (Array_length(edges_to_fix, 1) IS NULL OR edges_to_fix IS NULL) THEN
      EXIT;
   END IF;
 
   start_time_delta_job := Clock_timestamp();

   FOREACH edges SLICE 1 IN ARRAY edges_to_fix
   LOOP
      command_string := FORMAT('SELECT count(*) from %1$s.edge_data where edge_id in (%2$s, %3$s)',
      _atopology, edges[1],edges[2]);
      execute command_string into edge_ids_found;
      IF edge_ids_found = 2 THEN
        select topo_update.try_ST_ModEdgeHeal(_atopology, edges[1],edges[2]) into heal_result; 
        --RAISE NOTICE 'healed result % for this_edge_to_live %, this_edge_to_eat % at loop number % for _atopology %' ,heal_result, edges[1],edges[2], loop_nr, _atopology;
        IF heal_result > 0 THEN
          edges_fixed := edges_fixed + 1;
        END IF;
      ELSE
        edges_mising := edges_mising + 1;
        RAISE NOTICE 'Missing edge data, found % of, for this_edge_to_live %, this_edge_to_eat % at loop number % for _atopology % , edges_mising %' ,
        edge_ids_found, edges[1],edges[2], loop_nr, _atopology, edges_mising;
      END IF;
      
      IF edges_mising > 50 THEN
        EXIT;
      END IF;
   END LOOP;

   RAISE NOTICE 'In heal loop nr % , did heal % of % edes to heal lines for topo % and bb % at % used_time %', 
   loop_nr, edges_fixed, (Array_length(edges_to_fix, 1)), _atopology, _bb, Clock_timestamp(), (Extract(EPOCH FROM (Clock_timestamp() - start_time_delta_job)));

   IF edges_fixed = 0 OR loop_nr > max_loops THEN
      EXIT;
   END IF;
  END LOOP;
  RETURN loop_nr;
END
$function$;



SELECT topo_update.do_healedges_no_block('topo_sr16_mdata_05_365','0103000020E9640000010000000500000000000000A0EA0A4162A964474BDD5A4100000000A0EA0A4142C6ED8430E25A4100000000B49E0B4142C6ED8430E25A4100000000B49E0B4162A964474BDD5A4100000000A0EA0A4162A964474BDD5A41','0104000020E964000053000000010100000000000000802F0B410000000024E25A4101010000000000000040340B41000000001FE25A41010100000000000000C0380B410000000062DD5A4101010000000000000000390B410000000062DD5A41010100000000000000E0400B41000000002BE25A4101010000000000000060530B410000000016E25A41010100000000000000205A0B410000000025E25A41010100000000000000805E0B410000000030E25A4101010000000000000000610B410000000023E25A4101010000000000000000630B41000000002FE25A41010100000000000000A0650B410000000026E25A4101010000000000000020670B410000000014E25A41010100000000000000406D0B410000000026E25A41010100000000000000806D0B410000000011E25A41010100000000000000006F0B410000000064DD5A4101010000000000000040710B41000000005ADD5A4101010000000000000020730B410000000067DD5A4101010000000000000060750B41000000001DE25A4101010000000000000000790B410000000059DD5A4101010000000000000000790B41000000002EE25A41010100000000000000A07A0B41000000005ADD5A41010100000000000000A07C0B414CE9964DF5E15A41010100000000000000A07D0B410000000015E25A4101010000000000000080800B41000000004FDD5A4101010000000000000060820B410000000050DD5A41010100000000000000E0860B41000000004CDD5A41010100000000000000E0870B41000000000CE25A41010100000000000000A08A0B4100000000D1E15A41010100000000000000408B0B410000000055DD5A41010100000000000000608C0B41000000004EDD5A41010100000000000000808C0B410000000067DD5A41010100000000000000C08C0B410000000067DD5A41010100000000000000608E0B41000000004FDD5A41010100000000000000408F0B41000000004EDD5A41010100000000000000A08F0B41000000004EDD5A41010100000000000000A08F0B41000000002DE25A4101010000000000000020910B41000000005ADD5A4101010000000000000080910B410000000014E25A41010100000000000000E0920B410000000029E25A4101010000000000000040930B41000000004DDD5A41010100000000000000E0930B41000000001EE25A4101010000000000000040950B410000000047DF5A4101010000000000000040970B410000000014E25A41010100000000000000A0970B4100000000C2E15A4101010000000000000060980B4100000000E1DD5A4101010000000000000080980B410000000071DD5A4101010000000000000080990B410000000067E05A41010100000000000000009A0B41000000006FDF5A41010100000000000000809A0B410000000066E05A41010100000000000000209B0B4100000000A5E05A41010100000000000000809B0B41000000000BDE5A41010100000000000000809B0B410000000088DE5A41010100000000000000A09B0B4100000000C3E05A41010100000000000000C09B0B410000000094DD5A41010100000000000000C09B0B410000000087DF5A41010100000000000000C09B0B410000000094DF5A41010100000000000000E09B0B41000000005DDD5A41010100000000000000009C0B4100000000B3DE5A41010100000000000000209C0B410000000094E05A41010100000000000000409C0B410000000042DE5A41010100000000000000409C0B41000000005ADF5A41010100000000000000609C0B4100000000F7E15A41010100000000000000C09C0B410000000013DE5A41010100000000000000E09C0B410000000026E25A41010100000000000000609D0B4100000000CBDE5A41010100000000000000609D0B4100000000E1DE5A41010100000000000000609D0B4100000000BADF5A41010100000000000000A09D0B4100000000CCDF5A41010100000005CE4197D99D0B410000000008DE5A41010100000000000000E09D0B410000000066DE5A410101000000DA5B1E93F69D0B410000000012E05A41010100000000000000009E0B4100000000EDDF5A41010100000000000000209E0B410000000076E05A41010100000000000000409E0B41000000007EE05A41010100000000000000409E0B4100000000DEE05A41010100000000000000609E0B4100000000D9DD5A4101010000005A404499649E0B4100000000F5DF5A41010100000000000000809E0B4100000000CFE05A41010100000000000000A09E0B4100000000F4DD5A41010100000000000000A09E0B4100000000D5DE5A41010100000000000000A09E0B4100000000D4E05A41010100000000000000A09E0B4100000000F0E05A41010100000000000000A09E0B410000000011E25A41') ;

