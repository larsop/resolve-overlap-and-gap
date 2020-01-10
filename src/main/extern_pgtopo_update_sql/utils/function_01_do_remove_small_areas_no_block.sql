
CREATE OR REPLACE FUNCTION topo_update.do_remove_small_areas_no_block(
_atopology varchar,
input_table_name varchar,
input_table_geo_column_name varchar,
input_table_pk_column_name varchar,
_job_list_name varchar,
bb geometry,
remove_next_small_poly boolean default false 
) RETURNS integer 
LANGUAGE 'plpgsql' AS $function$ 
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

BEGIN 
	
	-- remove small poluygins if they dont't overlap
	LOOP

	command_string := format('select sum(topo_update.removes_small_polygons(%1$s,face_id,topo_area)) 
	from ( 
		select g.*, topo_update.get_face_area(%1$s,face_id) as topo_area 
		from (
			select g.* FROM (	
				select ST_Area(g.%2$s,false) as mbr_area, g.face_id 
				from ( 
					select g.face_id , g.mbr from %4$s g 
					where ST_Intersects(g.%2$s,%3$L) 
				) as g
			) as g
			where  g.mbr_area < 10000 and g.mbr_area > 49
		) as g
	) as g
	where g.topo_area < 1501 and g.topo_area > 49 ',
	quote_literal(_atopology),
	input_table_geo_column_name,
	bb,
	input_table_name

	);


	RAISE NOTICE 'execute command_string; %', command_string;
	-- why do we need to remove this area ????????????	
	if (remove_next_small_poly is true) then
--		execute command_string into num_rows;
	end if;

	RAISE NOTICE 'removed num_rows v2 % small polygons from %', num_rows, input_table_name;

    IF num_rows = 0 or num_rows is null THEN
        EXIT;  -- exit loop
    END IF;
    
    num_rows_total := num_rows_total + num_rows;
    
	END LOOP;
	  
	LOOP

	command_string := format('select sum(topo_update.removes_tiny_polygons(%1$s,face_id,topo_area,49.0)) 
	from ( 
		select g.*, topo_update.get_face_area(%1$s,face_id) as topo_area 
		from (
			select g.* FROM (	
				select ST_Area(g.%2$s,false) as mbr_area, g.face_id 
				from ( 
					select g.face_id , g.mbr from %4$s g 
					where g.%2$s && %3$L and ST_Intersects(g.%2$s,%3$L) 
				) as g
			) as g
			where  g.mbr_area < 1000 
		) as g
	) as g
	where g.topo_area < 49',
	quote_literal(_atopology),
	input_table_geo_column_name,
	bb,
	input_table_name

	);
-- with 4000 it's to slow
	RAISE NOTICE 'execute command_string; %', command_string;
	 execute command_string into num_rows;
	 
	 RAISE NOTICE 'removed num_rows v3 % tiny polygons from %', num_rows, input_table_name;

  IF num_rows = 0 or num_rows is null THEN
       EXIT;  -- exit loop
    END IF;
    
    num_rows_total := num_rows_total + num_rows;
    
	END LOOP;

	  return num_rows_total;
	
END $function$;




