# this code is now moving to https://gitlab.com/nibioopensource/resolve-overlap-and-gap

# resolve-overlap-and-gap
The plan here is use Postgis Topology to resolve overlaps and gaps for a simple feature layer. 

For the input
- It handles only geometry polygon 
- It must be a single primary key
- If we have surfaces of simple feature  and st_valid attributes on the surfaces will not be used.



This function now depend on 
- dblink (this replaced code from https://www.gnu.org/software/parallel)
- Postgres 10 or higher
- https://github.com/larsop/postgres_execute_parallel
- https://github.com/larsop/content_balanced_grid

# To checkout code
git clone --recursive https://github.com/larsop/resolve-overlap-and-gap.git

# To test code
cd resolve-overlap-and-gap
make check 

# To install code (must run make check first)
psql postgres -c "CREATE DATABASE aeg_02 template=template0;"
psql aeg_02 -c "create extension pg_stat_statements; create extension postgis; create extension postgis_topology; create extension dblink;"
psql aeg_02 -f ./src/test/sql/regress/resolve_overlap_and_gap-install.sql




[![Build Status](https://travis-ci.org/larsop/resolve-overlap-and-gap.svg?branch=master)](https://travis-ci.org/larsop/resolve-overlap-and-gap)

The following is implemented
- Load all lines into Postgis Topology
- Smooth lines if requested
- Collapse small/tiny surfaces if requested
- Collapse small/tiny gaps if requested
- Create new a new simple Feature layer 
- Add attributes and assign values 




