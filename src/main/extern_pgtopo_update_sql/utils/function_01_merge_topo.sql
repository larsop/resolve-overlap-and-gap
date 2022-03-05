
-- Remove a edge between to topo objects and clean up

DROP FUNCTION IF EXISTS topo_update.merge_topo(
	_topo_objects_table regclass, 
	_topo_object_to_expand int,
	_topo_object_to_remove int,
	_edge_id_to_remove int,
	_surface_topo_info topo_update.input_meta_info
) ;


CREATE OR REPLACE FUNCTION topo_update.merge_topo(
	_topo_objects_table regclass, 
	_topo_object_to_expand int,
	_topo_object_to_remove int,
	_edge_id_to_remove int,
	_surface_topo_info topo_update.input_meta_info
) 
RETURNS int AS $$DECLARE
DECLARE 
command_string text;
face_to_use int = -1;
found_topo_object int;
topo_object_edge int;
BEGIN

command_string := format('SELECT count(*) FROM %1$s f_attr WHERE f_attr.gid in (%2$L, %3$L)',
_topo_objects_table,
_topo_object_to_remove,
_topo_object_to_expand
);
EXECUTE command_string INTO found_topo_object ;
IF found_topo_object != 2 THEN
	RETURN face_to_use;
END IF;

command_string := format('SELECT count(*)
FROM %1$s.edge_data e 
WHERE e.edge_id = %2$L',
_surface_topo_info.topology_name,
_edge_id_to_remove
);
EXECUTE command_string INTO topo_object_edge;
IF topo_object_edge = 0 THEN
	RETURN face_to_use;
END IF;

command_string := format('SELECT topology.clearTopoGeom(f_attr.geo)
FROM %1$s f_attr
WHERE f_attr.gid in (%2$L, %3$L)',
_topo_objects_table,
_topo_object_to_remove,
_topo_object_to_expand
);
EXECUTE command_string;

command_string := format('delete from %1$s f_attr WHERE f_attr.gid = %2$L',
_topo_objects_table,
_topo_object_to_remove
);
EXECUTE command_string;

command_string := format('SELECT st_remedgemodface FROM ST_RemEdgeModFace (%1$L, %2$s)',
_surface_topo_info.topology_name,
_edge_id_to_remove
);
EXECUTE command_string INTO face_to_use;
 
command_string := format('UPDATE %1$s f_attr
set geo = r.geo
FROM 
(SELECT topology.CreateTopoGeom(%2$L,3,2, topology.TopoElementArray_Agg(ARRAY[f.face_id,3])) AS geo
FROM %2$s.face f where face_id = %3$s) 
as r
WHERE f_attr.gid in (%4$s)',
_topo_objects_table,
_surface_topo_info.topology_name,
face_to_use,
_topo_object_to_expand
);
EXECUTE command_string;

command_string := format('SELECT ST_RemEdgeModFace(%1$L, e.edge_id)
FROM %1$s.edge_data e 
WHERE e.left_face = e.right_face AND e.right_face > 0 AND e.left_face = %2$L',
_surface_topo_info.topology_name,
face_to_use
);
EXECUTE command_string;


command_string := format('SELECT ST_RemoveIsoNode(%1$L, n.node_id)
FROM %1$s.node n 
WHERE n.containing_face = %2$L',
_surface_topo_info.topology_name,
face_to_use
);
EXECUTE command_string;


RETURN face_to_use;

END;
$$ LANGUAGE plpgsql;


--\timing

 
--SELECT * FROM topo_update.merge_topo('test_ar50_flate_lars_06.face_attributes',80724,79285,3573,'("test_ar50_flate_lars_06","a","b","geo",3,0.1,2,25833)');
--SELECT * FROM topo_update.merge_topo('test_ar50_flate_lars_06.face_attributes',3579,79681,79704,'("test_ar50_flate_lars_06","a","b","geo",3,0.1,2,25833)');
--SELECT * FROM topo_update.merge_topo('test_ar50_flate_lars_06.face_attributes',3580,79285,79704,'("test_ar50_flate_lars_06","a","b","geo",3,0.1,2,25833)');
--SELECT * FROM topo_update.merge_topo('test_ar50_flate_lars_06.face_attributes',2657,79314,79336,'("test_ar50_flate_lars_06","a","b","geo",3,0.1,2,25833)');
--SELECT * FROM topo_update.merge_topo('test_ar50_flate_lars_06.face_attributes',3536,79295,79336,'("test_ar50_flate_lars_06","a","b","geo",3,0.1,2,25833)');


