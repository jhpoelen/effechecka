# effechecka
Taxon checklist generator: creates a list of organisms that satisfy specified spatial, temporal or trait based constraints.

# spatial indexing
A combination of quadtree and taxonomic tree is used to retrieve taxonomic check-list for given spatial and taxonomic contraints. For quadtree encoding, the tiling scheme of OpenStreetMap is used. For taxonomic trees, only kingdom, phylum, family, genus and species are considered. 

tiling scheme:
(x,y,z) where x,y are integers encoding the location and z the zoom level (or size of the covered area). Each tile has a node id. 

taxonomic schema:
(name, rank, externalId) - name is the scientific name of the taxon, rank is the family name.

Pre-populate checklists in tree representation -> checklists as lists referenced as assets.





