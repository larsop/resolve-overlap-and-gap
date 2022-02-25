/**
 * This is safe to use inside a cell not connedted to other cell or using single threa on a single layer 
 * 
 */

-- TODO _table_name find based om topolygy name
-- TODO add  _min_area float as parameter and use relative mbr area
drop FUNCTION if exists topo_update.do_merge_based_on_attribute_type_no_block (_atopology varchar, _min_area float, _table_name varchar, _bb geometry, 
_utm boolean,
_outer_cell_boundary_lines geometry);

CREATE OR REPLACE PROCEDURE topo_update.do_merge_based_on_attribute_type_no_block (_atopology varchar, _min_area float, _table_name varchar, _bb geometry, 
_utm boolean,
_outer_cell_boundary_lines geometry default null)
LANGUAGE plpgsql
AS $$
DECLARE
  command_string_find text;
  command_string text;
  num_rows int;
  num_rows_total int = 0;
  -- Based on testing and it's not accurate at all
  min_mbr_area float = _min_area * 1000;
  face_ids_to_remove integer[]; 
  face_id_tmp integer;
  remove_edge integer;
  edge_geo Geometry;
  lf_tmp integer;
  rf_tmp integer;
  
BEGIN

  command_string_find := Format('SELECT ARRAY(SELECT g.face_id
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
  where g.topo_area < %2$s and g.topo_area is not null )', Quote_literal(_atopology), _min_area, _table_name, _bb, min_mbr_area, _utm, _outer_cell_boundary_lines);
 	 
  LOOP
    RAISE NOTICE 'execute command_string; %', command_string_find;
    face_ids_to_remove := null;
    
    EXECUTE command_string_find INTO face_ids_to_remove;
    num_rows = 0;
    
    RAISE NOTICE 'Found % smalle area from % using min_mbr_area %', (Array_length(face_ids_to_remove, 1)), _table_name, min_mbr_area;

    IF face_ids_to_remove IS NOT NULL AND (Array_length(face_ids_to_remove, 1)) IS NOT NULL THEN 
       FOREACH face_id_tmp IN ARRAY face_ids_to_remove 
         LOOP
            command_string := Format('select geom, left_face, right_face FROM (                                                     
            SELECT edge_id, ST_length(geom)  as edge_length, geom, left_face, right_face from %1$s.edge_data 
            WHERE left_face != 0 AND right_face != 0 AND  
            ((%2$L = left_face AND left_face != right_face) or (%2$L = right_face AND left_face != right_face)) 
            order by edge_length desc
            ) as r limit 1', _atopology, face_id_tmp);
            EXECUTE command_string INTO edge_geo,lf_tmp,rf_tmp; 
            
            RAISE NOTICE 'lf_tmp % rf_tmp % edge_geo % ',lf_tmp,rf_tmp, edge_geo;
            
            command_string := Format('select edge_id FROM (                                                     
            SELECT edge_id, ST_length(geom)  as edge_length from %1$s.edge_data 
            WHERE left_face != 0 AND right_face != 0 AND  
            ((%2$L = left_face AND left_face != right_face) or (%2$L = right_face AND left_face != right_face)) 
            order by edge_length desc
            ) as r limit 1', _atopology, face_id_tmp);
            EXECUTE command_string INTO remove_edge;
            IF (remove_edge > 0) THEN
              -- using perform ST_RemEdgeModFace(_atopology, remove_edge);  seem make invalid faces somtimes
              BEGIN

                PERFORM ST_RemEdgeNewFace (_atopology, remove_edge);
                num_rows := num_rows + 1;
                RAISE NOTICE 'For merge face face_id % has egde_id % been removed',face_id_tmp, remove_edge;
                EXCEPTION
                WHEN OTHERS THEN
                  RAISE NOTICE 'ERROR failed to merge tiny face % for % ', face_id_tmp, _atopology;
              END;
            END IF;
 
          END LOOP;
        END IF;


    RAISE NOTICE 'Removed % (total %) edges for tiny faces tiny polygons from % using min_mbr_area %', num_rows, num_rows_total, _table_name, min_mbr_area;
    IF num_rows = 0 OR num_rows IS NULL THEN
      EXIT;
      -- exit loop
    END IF;
    num_rows_total := num_rows_total + num_rows;
  END LOOP;
END
$$;

