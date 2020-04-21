
# resolve-overlap-and-gap
The plan here is use Postgis Topology to resolve overlaps and gaps for a simple feature layer. 

[![Build Status](https://travis-ci.org/larsop/resolve-overlap-and-gap.svg?branch=master)](https://travis-ci.org/larsop/resolve-overlap-and-gap)

The following is planed to implement
- Load all lines into Postgis Topology
- Smooth lines if requested
- Collapse small/tiny surfaces if requested
- Collapse small/tiny gaps if requested
- Create new a new simple Feature layer 
- Add attributes and assign values 

