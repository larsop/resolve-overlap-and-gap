
CREATE OR REPLACE FUNCTION topo_update.heal_cellborder_edges_no_block(_atopology varchar, _bb Geometry, _valid_edges_list integer[] default null)
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
  

  
BEGIN
   LOOP
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
            %1$s.face e1fl,
            %1$s.face e1fr,
            %1$s.face e2fl,
            %1$s.face e2fr,
            (
                select r.node_id as node_id 
                from (
                    select count(n1.node_id) num_edges_end_here, n1.node_id as node_id 
                    from
                    %1$s.node n1,
                    %1$s.edge_data e1,
                    %1$s.face e1fl,
                    %1$s.face e1fr
                    where
   				    (e1fl.mbr && %2$L and e1fr.mbr && %2$L) and 
   				    (ST_Intersects(e1fl.mbr,%2$L) and ST_Intersects(e1fr.mbr,%2$L)) and 
                    e1.left_face != e1.right_face and 
                    e1fl.face_id = e1.left_face and e1fr.face_id = e1.right_face and
                    (e1.start_node = n1.node_id or e1.end_node = n1.node_id)
                    group by n1.node_id
                ) as r
                where r.num_edges_end_here = 2
            ) as r
            where 

   			(e1fl.mbr && %2$L and e1fr.mbr && %2$L) and 
   			(ST_Intersects(e1fl.mbr,%2$L) and ST_Intersects(e1fr.mbr,%2$L)) and 
            e1.left_face != e1.right_face and
            e1fl.face_id = e1.left_face and e1fr.face_id = e1.right_face and

   			(e2fl.mbr && %2$L and e2fr.mbr && %2$L) and 
   			(ST_Intersects(e2fl.mbr,%2$L) and ST_Intersects(e2fr.mbr,%2$L)) and 
            e2.left_face != e2.right_face and
            e2fl.face_id = e2.left_face and e2fr.face_id = e2.right_face and

            e2.left_face = e1.left_face and
            e2.right_face = e1.right_face and

            (e1.start_node = r.node_id or e1.end_node = r.node_id) and
            (e2.start_node = r.node_id or e2.end_node = r.node_id) and

    		r.node_id = n1.node_id and
            e1.start_node != e1.end_node and
            e2.start_node != e2.end_node --to avoid a closed surface ending in a line 
            -- ST_intersects(n1.geom,%2$L) and 
        ) as r
        ) as r
        order by edge_to_eat
    ) as r
    where r.edge_to_live != r.edge_to_eat)', _atopology, _bb); 
    EXECUTE command_string into edges_to_fix;

   IF (Array_length(edges_to_fix, 1) IS NULL OR edges_to_fix IS NULL) THEN
      EXIT;
   END IF;

   edges_fixed := 0;
   edges_mising := 0;
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

   RAISE NOTICE 'Healed % of edges found % for _atopology %' , edges_fixed, Array_length(edges_to_fix,1), _atopology; 

   IF edges_fixed = 0 OR loop_nr > max_loops THEN
      EXIT;
   END IF;
    
   END LOOP;
  RETURN loop_nr;
END
$function$;

