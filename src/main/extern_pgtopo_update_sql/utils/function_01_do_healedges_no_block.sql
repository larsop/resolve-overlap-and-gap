
CREATE OR REPLACE FUNCTION topo_update.do_healedges_no_block (_atopology varchar, _bb geometry)
  RETURNS integer
  LANGUAGE 'plpgsql'
  AS $function$
DECLARE
  command_string text;
  loop_nr int = 0;
  max_loops int = 3000;
  num_rows int;
  
  this_edge_to_live int;
  this_edge_to_eat int;
  heal_result int;
  
  
BEGIN
	
  LOOP
    loop_nr := loop_nr + 1;
    
    this_edge_to_live := null;
    this_edge_to_eat := null;

    command_string := Format('select r.edge_to_live, r.edge_to_eat 
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

            e2.left_face = e1.left_face and
            e2.right_face = e1.right_face and

            ST_CoveredBy(n1.geom,%2$L) 
        ) as r
        ) as r
        order by edge_to_eat
    ) as r
    where r.edge_to_live != r.edge_to_eat limit 1', _atopology, _bb);
    EXECUTE command_string into this_edge_to_live,this_edge_to_eat;

    GET DIAGNOSTICS num_rows = ROW_COUNT;
    -- if I heal more than one each time I get a lot this  NOTICE:  00000: FAILED select ST_ModEdgeHeal('test_topo_ar50_t3', 852, 11601) state  : XX000  message: SQL/MM Spatial exception - non-existent edge 852 detail :  hint   :  context: SQL statement "SELECT topology.ST_ModEdgeHeal (_atopology, _edge_to_live, _edge_to_eat)"
    --	RAISE NOTICE 'execute command_string; %', command_string;

    IF num_rows = 0 OR this_edge_to_live is null THEN
      EXIT;
    END IF;
    
    EXECUTE command_string into this_edge_to_live,this_edge_to_eat;
    select topo_update.try_ST_ModEdgeHeal(_atopology, this_edge_to_live,this_edge_to_eat) into heal_result; 
    
    RAISE NOTICE 'healed result % for this_edge_to_live %, this_edge_to_eat % at loop number % for _atopology %' ,
    heal_result, this_edge_to_live, this_edge_to_eat, loop_nr, _atopology;
    
    IF heal_result = -1 OR loop_nr > max_loops THEN
      EXIT;
    END IF;
  END LOOP;
  RETURN loop_nr;
END
$function$;


--select topo_update.do_healedges_no_block('test_topo_ar50_t3','0103000020E9640000010000000500000000000000804F0241000000005ACC5A4100000000804F024100000000C4E45A4100000000C05C054100000000C4E45A4100000000C05C0541000000005ACC5A4100000000804F0241000000005ACC5A41');
