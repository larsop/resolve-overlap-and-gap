
CREATE OR REPLACE FUNCTION topo_update.heal_cellborder_edges_no_block(_atopology varchar, bb_outer_geom Geometry)
  RETURNS integer
  LANGUAGE 'plpgsql'
  AS $function$
DECLARE
  command_string text;
  data_env geometry;
  num_rows int;
  num_rows_total int = 0;
  maxtolerance float8 = 5.0;
  area_to_block geometry;
  is_done integer = 0;
  num_boxes_intersect integer;
  num_boxes_free integer;
  loop_nr int = 0;
  max_loops int = 20;
BEGIN
  LOOP
    loop_nr := loop_nr + 1;
    command_string := Format( 'select topo_update.try_ST_ModEdgeHeal(%1$L, r.edge_to_live,r.edge_to_eat) 
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
            ST_intersects(n1.geom,%2$L) 
        ) as r
        ) as r
        order by edge_to_eat
    ) as r
    where r.edge_to_live != r.edge_to_eat', _atopology, bb_outer_geom);
    --	RAISE NOTICE 'execute command_string; %', command_string;
    EXECUTE command_string;
    RAISE NOTICE 'num edes healed % at loop number %', num_rows, loop_nr;
    GET DIAGNOSTICS num_rows = ROW_COUNT;
    IF num_rows = 0 OR num_rows IS NULL OR loop_nr > max_loops THEN
      EXIT;
      -- exit loop
    END IF;
    num_rows_total := num_rows_total + num_rows;
  END LOOP;
  RETURN num_rows_total;
END
$function$;

