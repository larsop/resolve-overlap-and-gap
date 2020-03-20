
CREATE OR REPLACE FUNCTION topo_update.heal_cellborder_edges_no_block(_atopology varchar, bb_outer_geom Geometry, _valid_edges_list integer[] default null)
  RETURNS integer
  LANGUAGE 'plpgsql'
  AS $function$
DECLARE
  command_string text;
  data_env geometry;
  last_num_rows int;
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
                    -- (%3$L is null OR e1.edge_id=ANY(%3$L)) and
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
            (e1.start_node = r.node_id or e1.end_node = r.node_id) and
            (e2.start_node = r.node_id or e2.end_node = r.node_id) and

    		r.node_id = n1.node_id and
            e1.start_node != e1.end_node and
            e2.start_node != e2.end_node --to avoid a closed surface ending in a line 
            -- ST_intersects(n1.geom,%2$L) and 
            --(%3$L is null OR (e1.edge_id=ANY(%3$L) AND e2.edge_id=ANY(%3$L)) )
        ) as r
        ) as r
        order by edge_to_eat
    ) as r
    where r.edge_to_live != r.edge_to_eat', _atopology, bb_outer_geom, _valid_edges_list );
    -- RAISE NOTICE 'execute command_string; %', command_string;
    EXECUTE command_string;
    GET DIAGNOSTICS num_rows = ROW_COUNT;
    IF last_num_rows = num_rows OR num_rows = 0 OR num_rows IS NULL OR loop_nr > max_loops THEN
      RAISE NOTICE 'Done healing, num edes tried to heal now % , num edges healed in privous loop % , at loop number % for topology %', num_rows, last_num_rows, loop_nr, _atopology;
      EXIT;
      -- exit loop
    END IF;
    RAISE NOTICE 'Contiune healing, num edes tried to heal now % , num edges healed in privous loop % , at loop number % for topology %', num_rows, last_num_rows, loop_nr, _atopology;
    last_num_rows = num_rows;
    
    num_rows_total := num_rows_total + num_rows;
  END LOOP;
  RETURN num_rows_total;
END
$function$;

