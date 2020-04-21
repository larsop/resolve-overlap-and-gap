-- This handles big, but is slower

CREATE OR REPLACE FUNCTION topo_update.get_single_lineparts(_geom geometry)
RETURNS geometry AS $$
DECLARE
simplfied_geom geometry;
num_points int;
BEGIN

num_points := ST_NumPoints(_geom);

					SELECT ST_Collect(lp) into simplfied_geom   from (
						SELECT r.*, ST_MakeLine(p1, p2) as lp
						FROM (
							SELECT  (dp).path[1] As org_index, (dp).geom As p1, lead((dp).geom) OVER () AS p2
							FROM (
								SELECT ST_DumpPoints(_geom) as dp
							) as r
						) as r
					) as r;

return simplfied_geom;
    
END;
$$ LANGUAGE plpgsql IMMUTABLE;
