# Crop Type Image Asset Tools

These tools are used to generate the crop type image assets needed to run the SIMS ET model.   The crop type images are built in tiles following the MGRS tiling scheme used by the ET export tool.

## Shapefile vs Collection Tools

The main difference between the two tools is the source/location of the field polygons and crop type image assets.  The "shapefile" version of the tool will read field shapefiles and CDL image stored locally, whereas the "collection" version of the tool will read the CDL image assets and field feature collections stored in Earth Engine.  

The "collection" tool will generally be much faster, but may require the extra step of ingesting the fields into Earth Engine.  For the "shapefile" tool, the scripts in the "cdl" folder should be used to download and preprocess the CDL images.

## Running the Tools

The following command will start separate export tasks for each MGRS tile intersecting the study area specified in the parameter file.  This script will generate images for each MGRS tile and year and write these to a local folder.  The script will then upload them to a cloud storage bucket and start Earth Engine image upload calls for each image (using the earthengine command line tool).

```
python crop_type_asset_mgrs_shapefile.py --years 2018
```

## Collection ID

Currently the crop_type images are being written to the image collection "projects/openet/crop_type/annual".  Separate images are created for each MGRS tile and for each year.  The naming of the image ID is currently "<MGRS_ID>\_<YEAR_DATE>" (e.g. "10SFJ_20170101").


## Input Shapefiles

The crop_type asset workflow is assuming (for now) that all of the shapefiles you want to use have already been downloaded from the master bucket.  Note, the naming and folder structure of this bucket will likely change so it will be important to double check the files and paths.  This process could be done manually from the GCP Storage website (https://console.cloud.google.com/storage/browser?project=openet-dri) or using the gsutil tool.  The following command will copy all of the files in the field_shapefiles folder to a local folder called "shapefiles".   You will need to manually create this folder or you could save the shapefiles to a different location.  Currently the only file in the bucket folder is a zipped sample shapefile that will need to be manually extracted.

```
gsutil -m cp gs://openet/field_shapefiles/* .\shapefiles\
```

### Crop Type Fields

The crop type tools are currently assuming that the crop type fields are named "CROP_YYYY" where YYYY is the 4 digit year.  For example, the crop type values for 2017 would be stored in a field called "CROP_2017".

## Command line arguments 

### Help

The "-h"/"--help" argument can be passed to the script in order to see details about all of the possible command line arguments.

```
(python3) C:\Projects\openet-tools\crop_type>python crop_type_asset_mgrs_shapefile.py -h

usage: crop_type_asset_mgrs_shapefile.py [-h] [--tiles TILES [TILES ...]]
                                         [--years YEARS [YEARS ...]]
                                         [--key FILE] [--overwrite] [--debug]
                                         
Build crop type MGRS tiles from a shapefile

optional arguments:
  -h, --help            show this help message and exit
  --tiles TILES [TILES ...]
                        Comma/space separated list of MGRS tiles (default: )
  --years YEARS [YEARS ...]
                        Comma separated list and/or range of years (default: )
  --key FILE            JSON key file (default: None)
  --overwrite           Force overwrite of existing files (default: False)
  --debug               Debug level logging (default: 20)

```
### Tiles

The "--tiles" argument can be used to ...

### Years

The "--years" argument can be used to ...

### Overwrite

The "--overwrite" argument must be passed to the script in order to overwrite any existing image assets.  If the overwrite flag is not set, any tiles that already have a crop_type image will be skipped.

### Key

The "--key" argument can be used to initialize Earth Engine using a service account JSON key file.

### Debug

The "--debug" argument can be passed to the script in order to activate Debug level logging.  This will print additional output to the command prompt and may help if the script is not running as expected.
