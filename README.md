
# resolve-overlap-and-gap
The plan here is use Postgis Topology to resolve overlaps and gaps for a simple feature layer. 

This function now depend on 
- dblink (this replaced code from https://www.gnu.org/software/parallel)
- Postgres 10 or higher
- https://github.com/larsop/postgres_execute_parallel
- https://github.com/larsop/content_balanced_grid

To test checkout code
git clone --recursive https://github.com/larsop/resolve-overlap-and-gap.git

[![Build Status](https://travis-ci.org/larsop/resolve-overlap-and-gap.svg?branch=master)](https://travis-ci.org/larsop/resolve-overlap-and-gap)

The following is planed to implement
- Load all lines into Postgis Topology
- Smooth lines if requested
- Collapse small/tiny surfaces if requested
- Collapse small/tiny gaps if requested
- Create new a new simple Feature layer 
- Add attributes and assign values 

#Pre stage 01
- Install the needed helper code 
- Schemas topo_update, topo_rein added/used by https://github.com/NibioOpenSource/pgtopo_update_sql.git


