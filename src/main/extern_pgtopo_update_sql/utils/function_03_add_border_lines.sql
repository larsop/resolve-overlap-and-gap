-- The name is woring used for adding both border lines and other lines

drop
	function if exists topo_update.add_border_lines(_topology_name character varying,
		_new_line geometry, _snap_tolerance float);
	


CREATE OR REPLACE FUNCTION topo_update.add_border_lines(_topology_name character varying,
		_new_line geometry,_snap_tolerance float
	) returns integer as $$ 

declare 
update_egde_id int;
old_egde_geom geometry;
new_egde_geom geometry;
command_string text;
begin 
	
update_egde_id = 0;

	
	BEGIN
		

		command_string := FORMAT('SELECT e.edge_id, e.geom
		from %s.edge_data e
		where e.geom && _new_line
		and ST_Intersects(e.geom,_new_line)
		limit 1',_topology_name);
		execute command_string into update_egde_id,old_egde_geom;


		IF update_egde_id is null THEN
			new_egde_geom := _new_line;
--			RAISE NOTICE 'new topo edge wil lbe added no intersecion found';
		ELSE 

--			RAISE NOTICE 'Intersectoin found with edge_id %', update_egde_id;

--			SELECT ST_Union(e.geom,_new_line)
--			from topo_ar5_forest_sysdata.edge_data e
--			where e.geom && _new_line
 --			and ST_Intersects(e.geom,_new_line)
--			into old_egde_geom;
			
--			new_egde_geom := ST_LineMerge(old_egde_geom);

--			IF ST_geometryType(new_egde_geom) = 'ST_LineString'  THEN
 -- 				RAISE NOTICE 'update edge id %, with %', update_egde_id,ST_AsText(new_egde_geom);
--				perform topology.ST_ChangeEdgeGeom(
--				'topo_ar5_forest_sysdata', update_egde_id , new_egde_geom) ;
--				new_egde_geom := null;
--			ELSE 
--				RAISE NOTICE 'new topo edge will be added no single linstring added';
				new_egde_geom := _new_line;
--			END IF;
		
      
		END IF;
		

		IF new_egde_geom is NOT null THEN
			command_string := format('select topology.TopoGeo_addLinestring(%s,%L,%s)',new_egde_geom,_snap_tolerance,quote_literal(_topology_name));
			execute command_string;
		END IF;
		
		-- Sjeck if new egde intersecst with 

		

	EXCEPTION WHEN OTHERS THEN

		RAISE NOTICE 'failed topo_update.add_border_lines ::::::::::::::::::::::::::::::::::::::::::::::::::: % for edge_id % ', ST_GeometryType(new_egde_geom), update_egde_id;
		    
	-- ERROR:  XX000: SQL/MM Spatial exception - coincident node
		BEGIN
		perform topo_update.simple_add_v2(geom)
		FROM (
			SELECT (ST_Dump(ST_Boundary(ST_Buffer(geom,1)))).geom
--			SELECT (ST_Dump(topo_update.extend_line(geom,2))).geom
			FROM (
			SELECT topo_update.get_single_lineparts((ST_Dump(_new_line)).geom) as geom		
			) as r
		) as r
		where ST_length(geom) > 0;	
		EXCEPTION WHEN OTHERS THEN
			BEGIN
				perform topo_update.simple_add_v2(topo_update.extend_line(geom,2))	
				FROM (
				SELECT (ST_Dump(topo_update.get_single_lineparts((ST_Dump(_new_line)).geom) )).geom	
				) as r;

			EXCEPTION WHEN OTHERS THEN
				RAISE NOTICE 'failed  ::::::::::::::::::::::::::::::::::::::::::::::::::: ';
				-- select TopoGeo_addLinestring('topo_ar5_forest_sysdata','0102000020E86400000200000000F0FF2748422341FDFF008045125941001000F8474223410300FF7F48125941',1)
				insert into topo_update.no_cut_line_failed(error_info,geo) 
				values('Failed for line with length ' || ST_length(new_egde_geom) , new_egde_geom);
			END;
			
		END;



	END;


return update_egde_id;
end;

$$ language plpgsql;

--select topo_update.add_border_lines('0102000020E864000004000000000000808C392341DA40A70D1B095941000000808A392341000000001B095941000000808A392341FFFFFFFF0D095941000000808C392341FFFFFFFF0D095941');

--select topo_update.add_border_lines('0102000020E864000004000000000000808C392341DA40A70D1B095941000000808A392341000000001B095941000000808A392341FFFFFFFF0D095941000000808C392341FFFFFFFF0D095941');

-- select topo_update.add_border_lines(geo) FROM topo_update.border_line_segments where id = 11295 ;
