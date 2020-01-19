/**
 * This is safe to use inside a cell not connedted to other cell or using single threa on a single layer 
 * 
 */

-- TODO _table_name find based om topolygy name
-- TODO add  _min_area float as parameter and use relative mbr area


CREATE OR REPLACE FUNCTION topo_update.do_remove_small_areas_no_block (_atopology varchar, _min_area float, _table_name varchar, _bb geometry)
  RETURNS integer
  LANGUAGE 'plpgsql'
  AS $function$
DECLARE
  command_string text;
  num_rows int;
  num_rows_total int = 0;
  -- Based on testing and it's not accurate at all
  min_mbr_area float = _min_area * 20;
BEGIN
  LOOP
    command_string := Format('select sum(topo_update.removes_tiny_polygons(%1$s,face_id,topo_area,%2$s)) 
 	from ( 
 		select g.*, topo_update.get_face_area(%1$s,face_id) as topo_area 
 		from (
 			select g.* FROM (	
 				select ST_Area(g.mbr,false) as mbr_area, g.face_id 
 				from ( 
 					select g.face_id , g.mbr from %3$s g 
 					where g.mbr && %4$L and ST_Intersects(g.mbr,%4$L) 
 				) as g
 			) as g
 			where  g.mbr_area < %5$s 
 		) as g
 	) as g
 	where g.topo_area < %2$s', Quote_literal(_atopology), _min_area, _table_name, _bb, min_mbr_area);
    -- with 4000 it's to slow
    RAISE NOTICE 'execute command_string; %', command_string;
    EXECUTE command_string INTO num_rows;
    RAISE NOTICE 'removed num_rows v3 % tiny polygons from %', num_rows, _table_name;
    IF num_rows = 0 OR num_rows IS NULL THEN
      EXIT;
      -- exit loop
    END IF;
    num_rows_total := num_rows_total + num_rows;
  END LOOP;
  RETURN num_rows_total;
END
$function$;

