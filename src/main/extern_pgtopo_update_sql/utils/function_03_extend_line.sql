--from https://gis.stackexchange.com/questions/104439/how-to-extend-a-straight-line-in-postgis

CREATE FUNCTION topo_update.extend_line (_new_line geometry, _extend_len float)
  RETURNS geometry
  AS $$
DECLARE
  new_egde_geom geometry;
BEGIN
  new_egde_geom := ST_MakeLine (ST_TRANSLATE (a, Sin(az1) * len, Cos(az1) * len), ST_TRANSLATE (b, Sin(az2) * len, Cos(az2) * len))
FROM (
  SELECT a, b, ST_Azimuth (a, b) AS az1, ST_Azimuth (b, a) AS az2,
    ST_Distance (a, b) + _extend_len AS len
  FROM (
    SELECT ST_StartPoint (_new_line) AS a, ST_EndPoint (_new_line) AS b) AS sub) AS sub2;
  RETURN new_egde_geom;
END;
$$
LANGUAGE plpgsql
STABLE STRICT;

--SELECT ST_asText (topo_update.extend_line (ST_MakeLine (ST_MakePoint (1, 2), ST_MakePoint (3, 4)), 2));

