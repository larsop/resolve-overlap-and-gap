-- TOOD find out how to handle log tables used for debug
--DROP TABLE IF EXISTS topo_update.no_cut_line_failed;
-- This is a list of lines that fails
-- this is used for debug

CREATE UNLOGGED TABLE topo_update.no_cut_line_failed (
  id serial PRIMARY KEY NOT NULL, log_time timestamp DEFAULT Now(), error_info text, geo Geometry(LineString, 4258)
);

--DROP TABLE IF EXISTS  topo_update.long_time_log1;
-- This is a list of lines that fails
-- this is used for debug

CREATE UNLOGGED TABLE topo_update.long_time_log1 (
  id serial PRIMARY KEY NOT NULL, log_time timestamp DEFAULT Now(), execute_time real, info text,
  geo Geometry(LineString, 4258)
);

--DROP TABLE IF EXISTS  topo_update.long_time_log2;
-- This is a list of lines that fails
-- this is used for debug

CREATE UNLOGGED TABLE topo_update.long_time_log2 (
  id serial PRIMARY KEY NOT NULL, log_time timestamp DEFAULT Now(), execute_time real, info text,
  sql text, geo Geometry(Polygon, 4258)
);

--DROP TABLE IF EXISTS  topo_update.border_line_segments;
CREATE TABLE topo_update.border_line_segments (
  id serial PRIMARY KEY NOT NULL, log_time timestamp DEFAULT Now(), geo Geometry(LineString, 4258), point_geo Geometry(Point, 4258)
);

