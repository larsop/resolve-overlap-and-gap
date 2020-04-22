CREATE OR REPLACE FUNCTION topo_update.do_healedges_no_block (_atopology varchar, _bb geometry)
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
            (
              (e2.left_face = e1.left_face and e2.right_face = e1.right_face ) or 
              (e2.left_face = e2.right_face or e1.left_face = e1.right_face)
            ) and

            ST_CoveredBy(n1.geom,%2$L) 
        ) as r
        ) as r
        order by edge_to_eat
    ) as r
    where r.edge_to_live != r.edge_to_eat)', _atopology, _bb);
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


