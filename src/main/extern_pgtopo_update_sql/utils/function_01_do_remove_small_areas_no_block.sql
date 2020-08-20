/**
 * This is safe to use inside a cell not connedted to other cell or using single threa on a single layer 
 * 
 */

-- TODO _table_name find based om topolygy name
-- TODO add  _min_area float as parameter and use relative mbr area


CREATE OR REPLACE FUNCTION topo_update.do_remove_small_areas_no_block (_atopology varchar, _min_area float, _table_name varchar, _bb geometry, 
_utm boolean,
_outer_cell_boundary_lines geometry default null)
  RETURNS integer
  LANGUAGE 'plpgsql'
  AS $function$
DECLARE
  command_string text;
  num_rows int;
  num_rows_total int = 0;
  -- Based on testing and it's not accurate at all
  min_mbr_area float = _min_area * 1000;
BEGIN
  command_string := Format('select sum(topo_update.removes_tiny_polygons(%1$s,face_id,topo_area,%2$s)) 
 	  from ( 
 		select g.*, topo_update.get_face_area(%1$s,face_id, %6$L) as topo_area 
 		from (
 			select g.* FROM (
                SELECT CASE 
                WHEN %6$L = false THEN 
                  ST_Area(g.mbr,TRUE) 
                ELSE 
                  ST_Area(g.mbr)
                END AS mbr_area,
                g.face_id
 				from ( 
 					select g1.face_id , g1.mbr 
                    from %3$s g1 
 					where g1.mbr && %4$L and ST_Intersects(g1.mbr,%4$L)
 				) as g WHERE (ST_Disjoint(g.mbr,%7$L) OR %7$L is null)
 			) as g 
 			where  g.mbr_area < %5$s 
 		) as g
 	  ) as g
  where g.topo_area < %2$s', Quote_literal(_atopology), _min_area, _table_name, _bb, min_mbr_area, _utm, _outer_cell_boundary_lines);
 	 
  LOOP
    -- RAISE NOTICE 'execute command_string; %', command_string;
    EXECUTE command_string INTO num_rows;
    RAISE NOTICE 'Removed num_rows %  (num_rows_total %) tiny polygons from % using min_mbr_area %', num_rows, num_rows_total, _table_name, min_mbr_area;
    IF num_rows = 0 OR num_rows IS NULL THEN
      EXIT;
      -- exit loop
    END IF;
    num_rows_total := num_rows_total + num_rows;
  END LOOP;
  RETURN num_rows_total;
END
$function$;

