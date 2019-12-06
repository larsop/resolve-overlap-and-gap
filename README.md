# resolve-overlap-and-gap
The plan here is use Postgis Topology to resolve overlaps and gaps for a simple feature layer. 

The following is planed to implement
- Load all lines into Postgis Topology
- Smooth lines if requested
- Collapse small/tiny surfaces if requested
- Collapse small/tiny gaps if requested
- Create new a new simple Feature layer 
- Add attributtes and assign values 

