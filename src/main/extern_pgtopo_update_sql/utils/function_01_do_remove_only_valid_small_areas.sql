/**
 * This is safe to use inside a cell not connedted to other cell or using single threa on a single layer 
 * 
 */

-- TODO _table_name find based om topolygy name
-- TODO add  _min_area float as parameter and use relative mbr area


CREATE OR REPLACE FUNCTION topo_update.do_remove_only_valid_small_areas (_atopology varchar, _min_area float, _table_name varchar, _bb geometry, _utm boolean)
  RETURNS integer
  LANGUAGE 'plpgsql'
  AS $function$
DECLARE
  command_string text;
  num_rows int;
  num_rows_total int = 0;
  -- Based on testing and it's not accurate at all
  min_mbr_area float = _min_area * 30;
  invalid_face_topo_table text = 'temp_invalid_topos_table_'||_atopology;
  v_cnt int;
BEGIN

  command_string := Format('create temp table %s(face_id int)',invalid_face_topo_table);
  EXECUTE command_string ;
	

  LOOP
  command_string := Format('insert into %s(face_id) select id1 as face_id from ValidateTopology(%L) where error like %L',
  invalid_face_topo_table, _atopology, 'face%' );
  EXECUTE command_string ;

  RAISE NOTICE 'execute insert command_string; %', command_string;

  get diagnostics v_cnt = row_count;
  
  RAISE NOTICE 'num invalid faces % for toplogy % ', v_cnt, _atopology;

    command_string := Format('select sum(topo_update.removes_tiny_polygons(%1$s,face_id,topo_area,%2$s)) 
 	from ( 
 		select g.*, topo_update.get_face_area(%1$s,face_id, %6$L) as topo_area 
 		from (
 			select g.* FROM (
                SELECT CASE 
                WHEN %6$L = false THEN 
                  ST_Area(g.mbr,FALSE) 
                ELSE 
                  ST_Area(g.mbr)
                END AS mbr_area,
                g.face_id 
 				from ( 
 					select g.face_id , g.mbr 
                    from %3$s g, %7$s e 
 					where e.face_id != g.face_id and g.mbr && %4$L and ST_Intersects(g.mbr,%4$L) 
 				) as g
 			) as g
 			where  g.mbr_area < %5$s 
 		) as g
 	) as g
 	where g.topo_area < %2$s', 
 	Quote_literal(_atopology), _min_area, _table_name, _bb, min_mbr_area, _utm, invalid_face_topo_table);
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

