-- TOOD find out how to handle log tables used for debug

--DROP TABLE IF EXISTS topo_update.no_cut_line_failed;

-- This is a list of lines that fails
-- this is used for debug
CREATE UNLOGGED TABLE topo_update.no_cut_line_failed(
id serial PRIMARY KEY not null,
log_time timestamp default now(),
error_info text,
geo geometry(LineString,25832) 
);


--DROP TABLE IF EXISTS  topo_update.long_time_log1;

-- This is a list of lines that fails
-- this is used for debug
CREATE UNLOGGED TABLE topo_update.long_time_log1(
id serial PRIMARY KEY not null,
log_time timestamp default now(),
execute_time real,
info text,
geo geometry(LineString,25832) 
);


--DROP TABLE IF EXISTS  topo_update.long_time_log2;

-- This is a list of lines that fails
-- this is used for debug
CREATE UNLOGGED TABLE topo_update.long_time_log2(
id serial PRIMARY KEY not null,
log_time timestamp default now(),
execute_time real,
info text,
sql text,
geo geometry(Polygon,25832) 
);

