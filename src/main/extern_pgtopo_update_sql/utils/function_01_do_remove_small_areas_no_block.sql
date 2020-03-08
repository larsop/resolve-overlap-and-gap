/**
 * This is safe to use inside a cell not connedted to other cell or using single threa on a single layer 
 * 
 */

-- TODO _table_name find based om topolygy name
-- TODO add  _min_area float as parameter and use relative mbr area


CREATE OR REPLACE FUNCTION topo_update.do_remove_small_areas_no_block (_atopology varchar, _min_area float, _table_name varchar, _bb geometry, _utm boolean)
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
 					select g.face_id , g.mbr from %3$s g 
 					where g.mbr && %4$L and ST_Intersects(g.mbr,%4$L) 
 				) as g
 			) as g
 			where  g.mbr_area < %5$s 
 		) as g
 	  ) as g
  where g.topo_area < %2$s', Quote_literal(_atopology), _min_area, _table_name, _bb, min_mbr_area, _utm);
 	 
  LOOP
    -- RAISE NOTICE 'execute command_string; %', command_string;
    EXECUTE command_string INTO num_rows;
    RAISE NOTICE 'removed num_rows %  (num_rows_total %) tiny polygons from % using min_mbr_area %', num_rows, num_rows_total, _table_name, min_mbr_area;
    IF num_rows = 0 OR num_rows IS NULL THEN
      EXIT;
      -- exit loop
    END IF;
    num_rows_total := num_rows_total + num_rows;
  END LOOP;
  RETURN num_rows_total;
END
$function$;

--SELECT face_ST_area(mbr,true) as t from test_topo_jm.face g where ST_Area(g.mbr,true) < 3000000 and topo_update.get_face_area('test_topo_jm', face_id, false) < 49 and '0103000020A2100000010000000500000055C79BC51EAC154060761052225F4D4055C79BC51EAC15407BDC4F921F6C4D408FB9A38BB47916407BDC4F921F6C4D408FB9A38BB479164060761052225F4D4055C79BC51EAC154060761052225F4D40' && mbr; 

select topo_update.do_remove_small_areas_no_block ('test_topo_jm', 49, 'test_topo_jm.face', ST_buffer('0103000020A210000001000000050000004490F4321DAC15407E8FBB1F225F4D404490F4321DAC15405DC3A4C41F6C4D40A0F04A1EB67916405DC3A4C41F6C4D40A0F04A1EB67916407E8FBB1F225F4D404490F4321DAC15407E8FBB1F225F4D40',(1e-06 * -6)),false);

