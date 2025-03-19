# California Crop Type Assets

Manually download the crop mapping shapefiles from the California Statewide Crop Mapping website to the "california/sources/" folder.
https://data.cnra.ca.gov/dataset/statewide-crop-mapping

Then run the "ca_shp_to_image_asset.py" tool to convert the shapefiles to GEE image assets.

### Remap tables
There is now a single remap table for the period of 2016-2023 based on DWR documentation. The remap of 2014 will use a year-specific tablle table.

~~There are separate remap tables for each year.  This should make it easier to support adding future years or to make year specific changes.  The remaps for 2014 and 2016 are identical, and the remaps for 2018-2021 are basically the same except for some classes that were in the metadata but not present in the earlier year shapefiles.~~