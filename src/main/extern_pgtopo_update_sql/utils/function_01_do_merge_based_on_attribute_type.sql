/**
 * This is safe to use inside a cell not connedted to other cell or using single threa on a single layer 
 * 
 */

-- TODO _table_name find based om topolygy name
-- TODO add  _min_area float as parameter and use relative mbr area
drop FUNCTION if exists topo_update.do_merge_based_on_attribute_type_no_block (_atopology varchar, _min_area float, _table_name varchar, _bb geometry, 
_utm boolean,
_outer_cell_boundary_lines geometry);

drop FUNCTION if exists topo_update.do_merge_based_on_attribute_type_no_block (
_atopology varchar, 
_min_area float, 
_table_name varchar, 
_bb geometry, 
_utm boolean,
_outer_cell_boundary_lines geometry
);

drop FUNCTION if exists topo_update.do_merge_based_on_attribute_type_no_block (
_input_data resolve_overlap_data_input_type, 
_clean_info resolve_overlap_data_clean_type,
_atopology varchar, 
_topology_snap_tolerance float,
_table_name varchar, 
_bb geometry, 
_outer_cell_boundary_lines geometry
);

CREATE OR REPLACE PROCEDURE topo_update.do_merge_based_on_attribute_type_no_block (
_input_data resolve_overlap_data_input_type, 
--(_input_data).line_table_name varchar, -- The table with simple feature lines, 
	-- If this has a value then data from table will used to form all valid surfaces.
	-- this may be empty, the polygon_table_geo_collumn must of type polygon to be abale to generate a polygon layer
--(_input_data).line_table_pk_column varchar, -- A unique primary column of the line input table
--(_input_data).line_table_geo_collumn varchar, -- The name of geometry column for the line strings
--(_input_data).polygon_table_name varchar, -- The table to resolv, imcluding schema name
--(_input_data).polygon_table_pk_column varchar, -- The primary of the input table
--(_input_data).polygon_table_geo_collumn varchar, -- the name of geometry column on the table to analyze
--(_input_data).table_srid int, -- the srid for the given geo column on the table analyze
--(_input_data).utm boolean, 

_clean_info resolve_overlap_data_clean_type,

_atopology varchar, 
_topology_snap_tolerance float,
_table_name varchar, 
_bb geometry, 
_table_name_result_prefix varchar, 
_outer_cell_boundary_lines geometry default null)
LANGUAGE plpgsql
AS $$
DECLARE
  command_string_find text;
  command_string text;
  num_rows int;
  num_rows_total int = 0;
  face_ids_to_remove integer[]; 
  face_id_tmp integer;
  remove_edge integer;
  edge_geo Geometry;
  lf_tmp integer;
  rf_tmp integer;
  _min_area float = ((_clean_info).resolve_based_on_attribute).attribute_max_common_area_size;
  -- Based on testing and it's not accurate at all
  min_mbr_area float = _min_area * 1000;
  
BEGIN

IF _table_name_result_prefix IS NULL OR LENGTH(_table_name_result_prefix) = 0 THEN
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
  where g.topo_area < %2$s and g.topo_area is not null )', 
  Quote_literal(_atopology), 
  _min_area, 
  _table_name, 
  _bb, 
  min_mbr_area, 
  (_input_data).utm, 
  _outer_cell_boundary_lines);
ELSE
  command_string_find := Format('SELECT ARRAY(SELECT g.face_id
 	  from ( 
	    SELECT DISTINCT g.face_id
		from ( 
			select g1.face_id , g1.mbr 
	        FROM 
	        %3$s g1,
	        %7$s b1 
			where g1.mbr && %4$L and g1.mbr && b1.geo
			UNION
			select g1.face_id , g1.mbr 
	        FROM 
	        %3$s g1, 
	        %8$s b1 
			where g1.mbr && %4$L and g1.mbr && b1.geo
		) as g ) as g
		)', 
  Quote_literal(_atopology), 
  _min_area, 
  _table_name, 
  _bb, 
  min_mbr_area, 
  (_input_data).utm, 
  _table_name_result_prefix||'_border_line_segments',
  _table_name_result_prefix||'_border_line_many_points'
  );
END IF;
 	 
  LOOP
    RAISE NOTICE 'execute command_string; %', command_string_find;
    face_ids_to_remove := null;
    
    EXECUTE command_string_find INTO face_ids_to_remove;
    num_rows = 0;
    
    RAISE NOTICE 'Found % do_merge_based_on_attribute area from % using min_mbr_area %', (Array_length(face_ids_to_remove, 1)), _table_name, min_mbr_area;

    IF face_ids_to_remove IS NOT NULL AND (Array_length(face_ids_to_remove, 1)) IS NOT NULL THEN 
       FOREACH face_id_tmp IN ARRAY face_ids_to_remove 
         LOOP
               
            command_string := Format('
            SELECT r2.edge_id, r2.small_bb FROM (
	            SELECT edge_id, small_bb, edge_length, geom  FROM (                                                     
	            SELECT edge_id, ST_length(geom)  AS edge_length, ST_Expand(ST_LineInterpolatePoint(geom,0.5),%3$L) AS small_bb, geom 
	            FROM %1$s.edge_data 
	            WHERE left_face != 0 AND right_face != 0 AND  
	            ((%2$L = left_face AND left_face != right_face) OR (%2$L = right_face AND left_face != right_face)) 
	            ORDER BY edge_length desc
	            ) AS a1
            ) AS r2
            WHERE (
            SELECT count(*) FROM 
            (SELECT * FROM (SELECT %4$s, count(DISTINCT %6$s) num_unique_id FROM %5$s s WHERE ST_Intersects(%7$s,r2.small_bb) GROUP BY %4$s) AS r WHERE num_unique_id = 2) AS re
            ) = 1
            ORDER BY r2.edge_length desc LIMIT 1', 
            _atopology, 
            face_id_tmp,
            _topology_snap_tolerance,
		    ((_clean_info).resolve_based_on_attribute).attribute_resolve_list,
            (_input_data).polygon_table_name,
            (_input_data).polygon_table_pk_column,
            (_input_data).polygon_table_geo_collumn
            );
            
            
            RAISE NOTICE 'ccccommand_string %',command_string;
            
            EXECUTE command_string INTO remove_edge, edge_geo;
            
            RAISE NOTICE 'remove_edge % edge_geo %',remove_edge, edge_geo;
            
            IF (remove_edge > 0) THEN
              -- using perform ST_RemEdgeModFace(_atopology, remove_edge);  seem make invalid faces somtimes
              BEGIN

                --using PERFORM ST_RemEdgeNewFace (_atopology, remove_edge) is causing 
                --2022-02-27 05:17:14.989 UTC [1046116] postgres@aeg_02_lars LOG:  duration: 1660.081 ms
				--2022-02-27 05:17:14.995 UTC [751831] LOG:  server process (PID 1046113) exited with exit code 245

                PERFORM ST_RemEdgeModFace (_atopology, remove_edge);
                num_rows := num_rows + 1;
                RAISE NOTICE 'For merge face face_id % has egde_id % been removed',face_id_tmp, remove_edge;
                EXCEPTION
                WHEN OTHERS THEN
                  RAISE NOTICE 'ERROR failed to merge tiny face % for % ', face_id_tmp, _atopology;
              END;
            END IF;
 
          END LOOP;
        END IF;


--    RAISE NOTICE 'Removed % (total %) edges for do_merge_based_on_attribute from % using min_mbr_area % and bb %', num_rows, num_rows_total, _table_name, min_mbr_area, ST_Centroid(_bb);
--    IF num_rows = 0 OR num_rows IS NULL THEN
      EXIT;
      -- exit loop
--    END IF;
    num_rows_total := num_rows_total + num_rows;
  END LOOP;
END
$$;

