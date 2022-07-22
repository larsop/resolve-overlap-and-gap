
-- find one row that intersecst with the row is sent using the 
-- The id_to_check check reffers to row with that comtains a face object (a topo object), get faceid form this object 
-- TODO find teh one with loongts egde

-- DROP FUNCTION IF EXISTS topo_update.touches(_new_topo_objects regclass,id_to_check int) ;

DROP FUNCTION IF EXISTS topo_update.touches(
	_face_attributes regclass,
	_face_attributes_pk_column_name text,  
	_face_attributes_topology_column_name text,  
	_surface_topo_info topo_update.input_meta_info
);

DROP FUNCTION IF EXISTS topo_update.touches(
	_face_attributes regclass,
	_face_attributes_pk_column_name text,  
	_face_attributes_topology_column_name text,  
	_face_attributes_equal_attr_namelist text,  
	_surface_topo_info topo_update.input_meta_info
);

DROP FUNCTION IF EXISTS topo_update.touches(
	_face_attributes regclass,
	_face_attributes_pk_column_name text,  
	_face_attributes_topology_column_name text,  
	_face_attributes_equal_attr_namelist text,  
	_max_new_mbr_area_m2 int, -- this is max new mbr for merged area when bigger than _min_input_mbr_area_m2
	_min_input_mbr_area_m2 int, -- mbr area smaller than this area should always try to be merged
	_utm boolean,
	_surface_topo_info topo_update.input_meta_info
); 

CREATE OR REPLACE FUNCTION topo_update.touches(
	_face_attributes regclass,
	_face_attributes_pk_column_name text,  
	_face_attributes_topology_column_name text,  
	_face_attributes_equal_attr_namelist text,  
	_max_new_mbr_area_m2 int, -- this is max new mbr for merged area when bigger than _min_input_mbr_area_m2
	_min_input_mbr_area_m2 int, -- mbr area smaller than this area should always try to be merged
	_utm boolean,
	_surface_topo_info topo_update.input_meta_info
) 
RETURNS TABLE (
topo_object_to_expand text,
topo_object_to_remove text,
edge_id_to_remove int
)

AS $$
DECLARE
command_string text;

attr_name_cond text = '';
part text;
srid_check_latlong text = ')';

BEGIN

-- set attribute cond
foreach part in array string_to_array(_face_attributes_equal_attr_namelist,' ')
loop
	attr_name_cond := attr_name_cond || format('AND face_attr_01.%1$s = face_attr_02.%1$s ', part,part);
END loop;

-- set 
IF _utm = false THEN
	srid_check_latlong = ',true)';
END IF;

command_string := FORMAT('SELECT 
CASE 
      WHEN r.f1_mbr > r.f2_mbr  THEN r.gid1
      ELSE r.gid2
END::text AS topo_object_to_expand,
CASE 
      WHEN r.f1_mbr > r.f2_mbr  THEN r.gid2
      ELSE r.gid1
END::text AS topo_object_to_remove,
r.edge_id_to_remove
FROM (
SELECT face_attr_01.%3$s AS gid1, face_attr_02.%3$s AS gid2, fa.edge_id as edge_id_to_remove, fa.mbr_area, f1_mbr, f2_mbr 
FROM 
(
	SELECT DISTINCT ON (f1_face_id) f1_face_id, f2_face_id, edge_id, mbr_area, f1_mbr, f2_mbr
	FROM (
		SELECT 
		f1.face_id f1_face_id, 
		ST_Area(f1.mbr %8$s AS f1_mbr,  
		f2.face_id f2_face_id, 
		ST_Area(f2.mbr %8$s AS f2_mbr,
		e1.edge_id,
		CASE WHEN ST_Area(f1.mbr %8$s > ST_Area(f2.mbr %8$s THEN ST_Area(f1.mbr %8$s ELSE ST_Area(f2.mbr %8$s END AS mbr_area
		FROM
		%2$s.face f1,
		%2$s.face f2,
		%2$s.edge e1
		WHERE 
		f1.face_id > 0 AND 
		f2.face_id > 0 AND 
		(e1.right_face = f2.face_id AND e1.left_face = f1.face_id) --OR (e1.right_face = f2.face_id AND e1.left_face = f1.face_id)
	) r
	ORDER BY f1_face_id, mbr_area
) fa,
%2$s.relation r1,
%2$s.relation r2,
%1$s face_attr_01,
%1$s face_attr_02
WHERE fa.f1_face_id = r1.element_id AND r1.element_type = %6$L AND r1.layer_id = %5$L
AND fa.f2_face_id = r2.element_id AND r2.element_type = %6$L AND r2.layer_id = %5$L
AND r1.topogeo_id = ((face_attr_01.%4$s).id)
AND r2.topogeo_id = ((face_attr_02.%4$s).id)
AND face_attr_01.%3$s != face_attr_02.%3$s 
%7$s 
AND (((f1_mbr + f1_mbr) < %9$L) OR f1_mbr < %10$L OR f2_mbr < %10$L) 
) r ORDER BY mbr_area',
_face_attributes, --1 
_surface_topo_info.topology_name, --2
_face_attributes_pk_column_name, --3
_face_attributes_topology_column_name, --4
_surface_topo_info.border_layer_id, --5
_surface_topo_info.element_type, --6
attr_name_cond, --7
srid_check_latlong, --8 
_max_new_mbr_area_m2, --9
_min_input_mbr_area_m2 --10  
);


RETURN QUERY EXECUTE command_string;

END
$$ LANGUAGE plpgsql;


--SELECT * FROM topo_update.touches('test_ar50_flate_lars_06.face_attributes',
--'gid','geo','artype arskogbon artreslag arjordbr arveget',
--(200*1000*1000),10000,true,
--'("test_ar50_flate_lars_05","a","b","geo",3,0.1,2,25833)'
--);

--SELECT * FROM topo_update.touches('test_ar5_2022_01.face_attributes',
--'new_id','geo','artype arskogbon artreslag argrunnf',
--10,10,true,
--'("test_ar5_2022_01","a","b","new_id",3,1e-05,2,4258)'
--) limit 10;

--real    742m37.423s
