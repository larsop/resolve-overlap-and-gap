/**
 * Get face area in meter, exception return 0
 */
CREATE OR REPLACE FUNCTION topo_update.get_face_geo(_atopology varchar, _face_id int, tolerance real)
  RETURNS Geometry
  AS $$
DECLARE
  -- holds dynamic sql to be able to use the same code for different
  command_string text;
  v_state text;
  v_msg text;
  v_detail text;
  v_hint text;
  v_context text;
  face_geo Geometry;
  i int;
  i2 int;
  g Geometry;
  g2 Geometry;
  g_face_geo Geometry;
  face_id_found int;
  face_id_list_tmp int[];
  glist Geometry[];
  face_id_mbr Geometry;
BEGIN
  BEGIN
	face_geo := st_getFaceGeometry (_atopology, _face_id);

	IF face_geo is NULL THEN
	  command_string = FORMAT('select ST_BuildArea(ST_Collect(e.geom)) from %1$s.face f, %1$s.edge_data e  
      where ST_Covers(ST_Expand(f.mbr,%3$s),e.geom) and face_id = %2$s',
	  _atopology, _face_id,tolerance);
	  execute command_string into face_geo;
	  
	  RAISE NOTICE 'Face for toplogy % with id % not found , try build manualy by using the edges, got %',
	  _atopology, _face_id, ST_AsText(face_geo);
	  
	  IF ST_NumGeometries(face_geo) = 1 THEN
	  	   RAISE NOTICE 'Face % for toplogy % is single polygon, we will check it',_atopology, _face_id;
 	
	  	   g := face_geo;
	       command_string = FORMAT('select ARRAY(select f.face_id from %1$s.face f where ST_Covers(%2$L,f.mbr))',
           _atopology, ST_Expand(ST_Envelope(g),tolerance));
	       execute command_string into face_id_list_tmp;

	 	   FOREACH face_id_found IN ARRAY face_id_list_tmp LOOP
	 	     RAISE NOTICE 'Face % for toplogy %  we test %',_atopology, face_id_found, ST_Area(g);
	 	     g_face_geo := st_getFaceGeometry (_atopology, face_id_found) ;

             IF g_face_geo IS NULL THEN
	           RAISE NOTICE 'Face % for toplogy %  we found mising geo %',_atopology, _face_id, ST_GeometryType(g);
	         ELSIF ST_Intersects(g_face_geo,g) THEN
	           g := ST_Difference(g,g_face_geo);
             END IF;
           END loop;

           IF ST_IsEmpty(g) = false THEN
             glist := glist || g;
           END IF;

           face_geo := ST_Collect(glist);
	
	  END IF;

	END IF;  
	
	
	IF ST_NumGeometries(face_geo) > 1 THEN
 	  RAISE NOTICE 'Face % for toplogy % is a multipolygon, we will check it, has % sub geos',_atopology, _face_id, ST_NumGeometries(face_geo);
 	  -- maybe we need to checkit
 	  --command_string = FORMAT('select f.mr from %1$s.face f where face_id = %2$s', _atopology, _face_id);
	  --execute command_string into face_id_mbr;
 	  
	  FOR i IN 1..ST_NumGeometries(face_geo) LOOP
	     g2 := ST_GeometryN(face_geo, i);
	     
	       FOR i2 IN 1..ST_NumGeometries(g2) LOOP
	       g := ST_GeometryN(g2, i2);

	       command_string = FORMAT('select ARRAY(select f.face_id from %1$s.face f where ST_Covers(%2$L,f.mbr))',
           _atopology, ST_Expand(ST_Envelope(g),tolerance));
	       execute command_string into face_id_list_tmp;

	 	   FOREACH face_id_found IN ARRAY face_id_list_tmp LOOP
	 	     RAISE NOTICE 'Face % for toplogy %  we test %',_atopology, face_id_found, ST_Area(g);
	 	     g_face_geo := st_getFaceGeometry (_atopology, face_id_found) ;

             IF g_face_geo IS NULL THEN
	           RAISE NOTICE 'Face % for toplogy %  we found mising geo %',_atopology, _face_id, ST_GeometryType(g);
	         ELSIF ST_Intersects(g_face_geo,g) THEN
	           g := ST_Difference(g,g_face_geo);
             END IF;
           END loop;
           
           IF ST_IsEmpty(g) = false THEN
             glist := glist || g;
           END IF;
	     END LOOP;
	   END LOOP;
      
	   
      face_geo := ST_Collect(glist);
	END IF;
	  
	
    EXCEPTION WHEN OTHERS THEN
	    GET STACKED DIAGNOSTICS v_state = RETURNED_SQLSTATE, v_msg = MESSAGE_TEXT, v_detail = PG_EXCEPTION_DETAIL, v_hint = PG_EXCEPTION_HINT,
                    v_context = PG_EXCEPTION_CONTEXT;
        RAISE NOTICE 'Failed failed to area for face_id % in topo % state  : %  message: % detail : % hint   : % context: %', 
        _face_id, _atopology, v_state, v_msg, v_detail, v_hint, v_context;
      face_geo := null;
    END;
  RETURN face_geo;
END;

$$
LANGUAGE plpgsql
STABLE;


--SELECT ST_NumGeometries(topo_update.get_face_geo('topo_sr16_trl_06',1491164,1));
--SELECT ST_NumGeometries(topo_update.get_face_geo('topo_sr16_trl_06',1528693,1));
--SELECT ST_AsText(topo_update.get_face_geo('topo_sr16_trl_06',1518294,1));


--SELECT ST_NumGeometries(topo_update.get_face_geo('topo_sr16_trl_06',1520234,1));
--SELECT ST_asText(topo_update.get_face_geo('topo_sr16_trl_06',1520021,1));

--SELECT ST_Area(topo_update.get_face_geo('topo_sr16_trl_06',1520135,1));

--SELECT ST_Area(topo_update.get_face_geo('topo_sr16_trl_06',1527949,1));
