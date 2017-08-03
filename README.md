This is a component of [MagicBox](https://github.com/unicef/magicbox/wiki)

### Introduction

Code in this repository aggregates raster data by administrative boundaries or shapefiles.

Rasters can be either country specific, i.e. a bounding box, or a swath of the planet that includes multiple whole countries.

Prior to using these methods, you must have installed postgres and used [shapefile-ingest](https://github.com/unicef/shapefile-ingest) to download shapefiles from [worldpop](www.worldpop.org.uk) and import to postGIS enabled postgres.

### Set up
    npm install
    cp config-sample.js config.js


### Aggregate population per country
	node aggregate_country_specific_raster.js
