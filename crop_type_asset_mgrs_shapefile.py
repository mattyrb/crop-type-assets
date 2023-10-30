#--------------------------------
# Name:         crop_type_asset_mgrs_shapefile.py
# Purpose:      Build and ingest crop type MGRS tiles from a shapefile
#--------------------------------

import argparse
import datetime
import logging
import math
import os
import pprint
import shutil
import subprocess
import sys
from time import sleep

import ee
from osgeo import gdal, ogr, osr

# import openet.core
import openet.core.utils as utils

TOOL_NAME = 'crop_type_asset_mgrs_shapefile'
# TOOL_NAME = os.path.basename(__file__)
TOOL_VERSION = '0.1.1'


def main(years=[], mgrs_tiles=[], overwrite_flag=False, gee_key_file=None):
    """Build and ingest crop type MGRS tiles from a shapefile

    Parameters
    ----------
    years : list, optional
    mgrs_tiles : list, optional
    overwrite_flag : bool, optional
        If True, overwrite existing files (the default is False).
    gee_key_file : str, None, optional
        Earth Engine service account JSON key file (the default is None).

    Returns
    -------
    None

    """
    logging.info('\nBuild and ingest crop type MGRS tiles from a shapefile')

    # Hardcoded parameters
    # These may eventually be made script or INI parameters
    rasterize_flag = True
    upload_flag = True
    ingest_flag = True

    cdl_fill_flag = True

    input_ws = os.path.join(os.getcwd(), 'shapefiles')
    output_ws = os.path.join(os.getcwd(), 'images')

    bucket_name = 'gs://openet'
    bucket_folder = 'crop_type'

    export_coll_id = 'projects/earthengine-legacy/assets/' \
                     'projects/openet/crop_type/annual_provisional'
    # export_coll_id = 'projects/earthengine-legacy/assets/' \
    #                  'projects/openet/crop_type/annual'
    export_band_name = 'crop_type'

    mgrs_ftr_coll_id = 'projects/earthengine-legacy/assets/' \
                       'projects/openet/mgrs/conus_gridmet/zones'
    mgrs_mask_coll_id = 'projects/earthengine-legacy/assets/' \
                        'projects/openet/mgrs/conus_gridmet/zone_mask'

    # For now, hardcode the study area as the CONUS
    # Allow the user to set a subset of states also
    # study_area_coll = projects/climate-engine/featureCollections/shp_new/cb_2017_us_state_5m
    study_area_coll_id = 'TIGER/2018/States'
    study_area_property = 'STUSPS'
    # study_area_features = 'CONUS'
    # study_area_features = 'AZ, CA, CO, ID, MT, NM, NV, OR, UT, WA, WY'
    study_area_features = 'NV'

    cell_size = 30
    snap_x, snap_y = 15, 15

    output_nodata = 255
    output_gtype = gdal.GDT_Byte

    # Limit the CDL images to years when full CONUS images are available
    year_min = 2008
    year_max = 2020
    if not years:
        years = list(range(year_min, year_max+1))
    else:
        years = sorted(list(set(
            int(year) for year_str in years
            for year in utils.str_ranges_2_list(year_str)
            if ((year <= year_max) and (year >= year_min))
        )))

    logging.info('Years:  {}'.format(', '.join(map(str, years))))

    # mgrs_skip_list = []
    # utm_zones = []

    # Parse user inputs
    mgrs_tiles = sorted([y.strip() for x in mgrs_tiles for y in x.split(',')])
    # mgrs_tiles = sorted([x.strip() for x in mgrs_tiles.split(',')])
    study_area_features = sorted([x.strip() for x in study_area_features.split(',')])
    logging.info('Tiles:    {}'.format(', '.join(mgrs_tiles)))
    logging.info('Features: {}'.format(', '.join(study_area_features)))
    logging.info('Years:    {}'.format(', '.join(years)))
    # input('ENTER')



    # # Hardcoding MGRS GeoJSON file name and folder (for now)
    # # GeoJSON built from MGRS shapefile using following command:
    # #   ogr2ogr MGRS_100kmSQ_ID_conus.geojson MGRS_100kmSQ_ID_conus.shp
    # #   -preserve_fid -lco RFC7946=YES
    # mgrs_geojson = os.path.join(os.getcwd(), 'MGRS_100kmSQ_ID_conus.geojson')
    # if not os.path.isfile(mgrs_geojson):
    #     logging.error(
    #         '\nMGRS geojson must be in the current working directory, exiting'
    #         '\n  {}'.format(mgrs_geojson))
    #     sys.exit()
    # mgrs_id_field = 'MGRS'
    # utm_zone_field = 'UTM_ZONE'
    # northing_field = 'NORTHING'
    # easting_field = 'EASTING'
    #
    # utm_geojson = os.path.join(os.getcwd(), 'utm_zones.geojson')
    # utm_field = 'UTM_ZONE'



    # # Parse mandatory INI parameters
    # try:
    #     crop_type_ws = str(ini['INPUTS']['crop_type_folder'])
    #     # logging.debug('crop_type_folder: {}'.format(crop_type_folder))
    # except KeyError:
    #     logging.error('  crop_type_folder: must be set in INI')
    #     sys.exit()
    #
    # try:
    #     years = utils.parse_int_set(str(ini['EXPORT']['years']))
    #     logging.debug('years: {}'.format(years))
    # except KeyError:
    #     logging.error('  years: must be set in INI')
    #     sys.exit()
    #
    # try:
    #     export_coll_id = str(ini['EXPORT']['export_coll'])
    #     # logging.debug('export_coll_id: {}'.format(export_coll_id))
    # except KeyError:
    #     logging.error('  export_coll: must be set in INI')
    #     sys.exit()
    #
    # # Parse INI for parameters with default values
    # try:
    #     output_ws = str(ini['EXPORT']['output_folder'])
    # except KeyError:
    #     output_ws = os.path.join(os.getcwd(), 'images')
    #     logging.debug('  output_folder: not set in INI, '
    #                   'defaulting to {}'.format(output_ws))
    # except Exception as e:
    #     raise e


    if os.name == 'posix':
        shell_flag = False
    else:
        shell_flag = True


    # Check inputs
    if not os.path.isdir(input_ws):
        logging.error('\nSource folder "{}" does not exist'.format(input_ws))
        sys.exit()

    # logging.info('\nRemoving existing local files')
    # if overwrite_flag and os.path.isdir(output_ws):
    #     shutil.rmtree(output_ws)
    #     sleep(1)
    if not os.path.isdir(output_ws):
        os.makedirs(output_ws)


    # logging.info('\nRemoving existing files from bucket')
    # args = ['gsutil', '-m', 'rm', '{}/{}/*.tif'.format(bucket_name, bucket_folder)]
    # if not logging.getLogger().isEnabledFor(logging.DEBUG):
    #     args.insert(1, '-q')
    # try:
    #     subprocess.run(args, shell=shell_flag, check=True)
    # except Exception as e:
    #     logging.exception('    Exception: {}'.format(e))


    logging.info('\nInitializing Earth Engine')
    if gee_key_file:
        logging.info('  Using service account key file: {}'.format(gee_key_file))
        # The "EE_ACCOUNT" parameter is not used if the key file is valid
        ee.Initialize(ee.ServiceAccountCredentials('', key_file=gee_key_file))
    else:
        ee.Initialize()
    ee.Number(1).getInfo()


    # Get current running tasks
    tasks = utils.get_ee_tasks()
    if logging.getLogger().getEffectiveLevel() == logging.DEBUG:
        logging.debug('  Tasks: {}'.format(len(tasks)))
        input('ENTER')


    # Build the export collection if it doesn't exist
    if not ee.data.getInfo(export_coll_id.rsplit('/', 1)[0]):
        logging.info('\nFolder does not exist and will be built'
                      '\n  {}'.format(export_coll_id.rsplit('/', 1)[0]))
        input('Press ENTER to continue')
        ee.data.createAsset({'type': 'FOLDER'}, export_coll_id.rsplit('/', 1)[0])
    if not ee.data.getInfo(export_coll_id):
        logging.info('\nExport collection does not exist and will be built'
                     '\n  {}'.format(export_coll_id))
        input('Press ENTER to continue')
        ee.data.createAsset({'type': 'IMAGE_COLLECTION'}, export_coll_id)


    # Get list of existing images/files
    logging.debug('\nGetting GEE asset list')
    asset_list = utils.get_ee_assets(export_coll_id)
    logging.debug('Displaying first 10 images in collection')
    logging.debug(asset_list[:10])


    # Get list of MGRS tiles that intersect the study area
    logging.debug('\nBuilding export list')
    export_list = mgrs_export_tiles(
        study_area_coll_id=study_area_coll_id,
        mgrs_coll_id=mgrs_ftr_coll_id,
        study_area_property=study_area_property,
        study_area_features=study_area_features,
        mgrs_tiles=mgrs_tiles,
        # mgrs_skip_list=mgrs_skip_list,
        # utm_zones=utm_zones,
    )
    if not export_list:
        logging.error('\nEmpty export list, exiting')
        return False

    # export_list = sorted(export_list, reverse=reverse_flag,
    #                      key=lambda i: i['index'])


    # DEADBEEF - Commenting out for now
    # # Process each tile separately
    # logging.info('\nImage Exports')
    # for export_n, export_info in enumerate(export_list):
    #     mgrs_tile = export_info['index'].upper()
    #     logging.info('MGRS Tile: {} ({}/{})'.format(
    #         mgrs_tile, export_n + 1, len(export_list)))
    #     logging.info('  Shape:      {}'.format(export_info['shape_str']))
    #     logging.info('  Transform:  {}'.format(export_info['geo_str']))
    #     logging.info('  Extent:     {}'.format(export_info['extent']))
    #     logging.info('  MaxPixels:  {}'.format(export_info['maxpixels']))
    #
    #     mgrs_geom = ee.Geometry.Rectangle(
    #         export_info['extent'], proj=export_info['crs'], geodesic=False)
    #
    #     mgrs_mask_id = f'{mgrs_mask_coll_id}/{mgrs_tile.lower()}'
    #     mgrs_mask_img = ee.Image(mgrs_mask_id)
    #     # logging.debug(f'{mgrs_mask_id}')
    #     # pprint.pprint(mgrs_mask_img.getInfo())
    #
    #     # Get a list of states that intersect the MGRS tile
    #     # Use this state list to select the field collections
    #     state_coll = ee.FeatureCollection(study_area_coll_id) \
    #         .filterBounds(mgrs_geom)
    #     mgrs_states = sorted(
    #         state_coll.aggregate_array(study_area_property).getInfo())
    #     logging.info('  States: {}'.format(', '.join(mgrs_states)))
    #
    #     # logging.debug('\nProcessing crop type shapefiles')
    #     # for crop_type_name in os.listdir(input_ws):
    #     #     if not crop_type_name.lower().endswith('.shp'):
    #     #         continue
    #     #     crop_type_path = os.path.join(input_ws, crop_type_name)
    #     #     logging.info('  {}'.format(crop_type_path))
    #     #
    #     #     crop_type_ds = ogr.Open(os.path.join(input_ws, crop_type_path), 0)
    #     #     crop_type_lyr = crop_type_ds.GetLayer()
    #     #     crop_type_osr = crop_type_lyr.GetSpatialRef()
    #     #
    #     #     # Spatially filter the crop type shapefile if the study area was set
    #     #     if study_area_geom:
    #     #         crop_type_lyr.SetSpatialFilter(study_area_geom)
    #     #
    #     #     # Build an extent geometry that can be projected/transformed below
    #     #     crop_type_extent = crop_type_lyr.GetExtent()
    #     #     crop_type_geom = utils.extent_geom(crop_type_extent, swap_ul_lr=True)
    #     #
    #     #     # Get the convex hull of the dissolved/unioned crop type geometries
    #     #     # study_area_geom = ogr.Geometry(ogr.wkbMultiPolygon)
    #     #     # for crop_type_ftr in crop_type_lyr:
    #     #     #     crop_type_geom =crop_type_geom.Union(study_area_ftr.GetGeometryRef())
    #     #     # study_area_geom = study_area_geom.ConvexHull()
    #     #     logging.debug('  Extent: {}'.format(crop_type_geom))
    #     #     logging.debug('  {}'.format(crop_type_osr.ExportToWkt()))
    #     #     crop_type_ds = None
    #     #
    #     #     # Project the study area to the MGRS coordinate system
    #     #     mgrs_tx = osr.CoordinateTransformation(crop_type_osr, mgrs_osr)
    #     #     crop_type_geom.Transform(mgrs_tx)
    #     #     logging.debug('  Extent: {}'.format(crop_type_geom))
    #     #
    #     #     # Spatially filter the MGRS collection to the study area geometry
    #     #     mgrs_lyr.ResetReading()
    #     #     mgrs_lyr.SetSpatialFilter(crop_type_geom)
    #     #
    #     #     # Save the intersecting MGRS feature data
    #     #     for mgrs_ftr in mgrs_lyr:
    #     #         mgrs_id = mgrs_ftr.GetField(mgrs_id_field)
    #     #         if utm_zones and int(mgrs_id[:2]) not in utm_zones:
    #     #             logging.debug(
    #     #                 '  {} - zone not in INI utm_zones, skipping'.format(mgrs_id))
    #     #             continue
    #     #         elif mgrs_tiles and mgrs_id not in mgrs_tiles:
    #     #             logging.debug(
    #     #                 '  {} - tile not in INI mgrs_list, skipping'.format(mgrs_id))
    #     #             continue
    #     #
    #     #         if mgrs_id not in mgrs_info.keys():
    #     #             mgrs_info[mgrs_id] = {
    #     #                 # 'geometry': mgrs_ftr.GetGeometryRef().Clone(),
    #     #                 'properties': {
    #     #                     mgrs_id_field: mgrs_id,
    #     #                     northing_field: mgrs_ftr.GetField(northing_field),
    #     #                     easting_field: mgrs_ftr.GetField(easting_field),
    #     #                 },
    #     #                 'shapefiles': [crop_type_path],
    #     #             }
    #     #         else:
    #     #             mgrs_info[mgrs_id]['shapefiles'].append(crop_type_path)
    #     #
    #     #     logging.info('  MGRS Tiles: {}'.format(
    #     #         ', '.join(sorted(mgrs_info.keys()))))
    #     # mgrs_ds = None
    #     # # pprint.pprint(mgrs_info)
    #     # # input('ENTER')
    #
    #     for year in years:
    #         logging.info('  Year: {}'.format(year))
    #
    #         crop_type_field = 'CDL_{:04d}'.format(year)
    #         logging.debug('  Crop Type Field: {}'.format(year))
    #
    #         cdl_path = os.path.join(
    #             os.path.dirname(output_ws), 'cdl', str(year),
    #             'CDL{}_{}.tif'.format(year, mgrs_tile))
    #         if cdl_fill_flag and not os.path.isfile(cdl_path):
    #             logging.error('  The CDL tile array does not exist, skipping')
    #             continue
    #
    #         image_id = f'{mgrs_tile}_{year}0101'
    #         output_path = os.path.join(output_ws, '{}.tif'.format(image_id))
    #         bucket_path = '{}/{}/{}'.format(
    #             bucket_name, bucket_folder, '{}.tif'.format(image_id))
    #         asset_id = '{}/{}'.format(export_coll_id, image_id)
    #         export_id = 'crop_type_{}'.format(image_id)
    #         logging.debug('  {}'.format(output_path))
    #         logging.debug('  {}'.format(bucket_path))
    #         logging.debug('  {}'.format(asset_id))
    #         logging.debug('  {}'.format(export_id))
    #
    #         # First get rid of the asset if it already exists
    #         if overwrite_flag and asset_id in asset_list:
    #             logging.info('  Removing existing asset')
    #             ee.data.deleteAsset(asset_id)
    #
    #         if overwrite_flag and os.path.isfile(output_path):
    #             logging.info('  Removing existing image')
    #             os.remove(output_path)
    #
    #         # if overwrite_flag:
    #         #     if export_id in tasks.keys():
    #         #         logging.debug('  Task already submitted, cancelling')
    #         #         ee.data.cancelTask(tasks[export_id])
    #         #     # This is intentionally not an "elif" so that a task can be
    #         #     # cancelled and an existing image/file/asset can be removed
    #         #     if asset_id in asset_list:
    #         #         logging.debug('  Asset already exists, removing')
    #         #         ee.data.deleteAsset(asset_id)
    #         #     if os.path.isfile(output_path):
    #         #         logging.debug('  Output image already exists, removing')
    #         #         os.remove(output_path)
    #         # else:
    #         #     if export_id in tasks.keys():
    #         #         logging.debug('  Task already submitted, exiting')
    #         #         continue
    #         #     elif asset_id in asset_list:
    #         #         logging.debug('  Asset already exists, skipping')
    #         #         continue
    #
    #         # utm_zone = int(mgrs_id[:2])
    #         # tile_osr = osr.SpatialReference()
    #         # tile_osr.ImportFromEPSG(int('326{:02d}'.format(utm_zone)))
    #         # logging.debug('  UTM Zone:  {}'.format(utm_zone))
    #         # logging.debug('  {}'.format(tile_osr.ExportToWkt()))
    #         #
    #         # # First build the nominal tile extent
    #         # tile_size = 100 * 1000  # meters
    #         # northing = int(mgrs_ftr['properties'][northing_field][:-2])
    #         # easting = int(mgrs_ftr['properties'][easting_field][:-2])
    #         # tile_extent = [easting, northing, easting + tile_size, northing + tile_size]
    #         # # logging.debug('    Tile Extent: {}'.format(tile_extent))
    #         #
    #         # # Then adjust the extent to the snap points so the tiles don't overlap
    #         # # Trying floor to always shift the tile down and to the left
    #         # #   (so the northing/easting point has data)
    #         # adjust_size = 1 * cell_size
    #         # tile_extent[0] = math.floor((tile_extent[0] - snap_x) / adjust_size) * adjust_size + snap_x
    #         # tile_extent[1] = math.floor((tile_extent[1] - snap_y) / adjust_size) * adjust_size + snap_y
    #         # tile_extent[2] = math.floor((tile_extent[2] - snap_x) / adjust_size) * adjust_size + snap_x
    #         # tile_extent[3] = math.floor((tile_extent[3] - snap_y) / adjust_size) * adjust_size + snap_y
    #         # # # DEADBEEF - using round is causing overlapping tiles
    #         # # tile_extent[0] = round((tile_extent[0] - snap_x) / adjust_size) * adjust_size + snap_x
    #         # # tile_extent[1] = round((tile_extent[1] - snap_y) / adjust_size) * adjust_size + snap_y
    #         # # tile_extent[2] = round((tile_extent[2] - snap_x) / adjust_size) * adjust_size + snap_x
    #         # # tile_extent[3] = round((tile_extent[3] - snap_y) / adjust_size) * adjust_size + snap_y
    #         # # logging.debug('  Adjusted Extent: {}'.format(tile_extent))
    #         # # input('ENTER')
    #
    #         # # GDAL transform format
    #         # tile_geo = [tile_extent[0], cell_size, 0, tile_extent[3], 0, -cell_size]
    #         # tile_shape = [
    #         #     int((tile_extent[2] - tile_extent[0]) / cell_size),
    #         #     int((tile_extent[3] - tile_extent[1]) / cell_size)]
    #         # tile_geom = utils.extent_geom(tile_extent)
    #         # logging.debug('  Extent: {}'.format(tile_extent))
    #         # logging.debug('  Geo:    {}'.format(tile_geo))
    #         # logging.debug('  Shape:  {}'.format(tile_shape))
    #
    #
    #         # if rasterize_flag:
    #         #     if not os.path.isfile(output_path):
    #         #         logging.info('  Writing empty tile')
    #         #         output_driver = gdal.GetDriverByName('GTiff')
    #         #         output_ds = output_driver.Create(
    #         #             output_path, tile_shape[0], tile_shape[1], 1,
    #         #             output_gtype, ['COMPRESS=LZW', 'TILED=YES'])
    #         #         output_ds.SetProjection(tile_osr.ExportToWkt())
    #         #         output_ds.SetGeoTransform(tile_geo)
    #         #         output_band = output_ds.GetRasterBand(1)
    #         #         output_band.Fill(output_nodata)
    #         #         output_band.SetNoDataValue(output_nodata)
    #         #         output_ds = None
    #         #     else:
    #         #         logging.info('  Clearing existing tile')
    #         #         output_ds = gdal.Open(output_path, 1)
    #         #         output_band = output_ds.GetRasterBand(1)
    #         #         output_band.Fill(output_nodata)
    #         #         output_ds = None
    #         #
    #         #     # This could be moved up into the writing empty tile section to
    #         #     #   avoid writing twice.
    #         #     if cdl_fill_flag:
    #         #         logging.debug('    Reading CDL array')
    #         #         cdl_ds = gdal.Open(cdl_path, 1)
    #         #         cdl_band = cdl_ds.GetRasterBand(1)
    #         #         cdl_nodata = cdl_band.GetNoDataValue()
    #         #         cdl_array = cdl_ds.ReadAsArray(
    #         #             0, 0, tile_shape[0], tile_shape[1])
    #         #         cdl_ds = None
    #         #
    #         #         logging.debug('    Writing updated array')
    #         #         output_ds = gdal.Open(output_path, 1)
    #         #         output_band = output_ds.GetRasterBand(1)
    #         #         output_array = output_ds.ReadAsArray(
    #         #             0, 0, tile_shape[0], tile_shape[1])
    #         #         cdl_mask = cdl_array != cdl_nodata
    #         #         output_array[cdl_mask] = cdl_array[cdl_mask]
    #         #         output_band.WriteArray(output_array, 0, 0)
    #         #         output_ds = None
    #         #         del cdl_array, cdl_mask, cdl_ds


    #         # Read each shapefile and write over the crop_type image
    #         logging.debug('  Rasterizing crop_type features')
    #         for crop_type_path in sorted(mgrs_ftr['shapefiles']):
    #             logging.info('  {}'.format(crop_type_path))
    #             crop_type_ds = ogr.Open(crop_type_path, 0)
    #             crop_type_lyr = crop_type_ds.GetLayer()
    #             crop_type_osr = crop_type_lyr.GetSpatialRef()
    #
    #             crop_type_def = crop_type_lyr.GetLayerDefn()
    #             field_list = [
    #                 crop_type_def.GetFieldDefn(n).name
    #                 for n in range(crop_type_def.GetFieldCount())]
    #             if crop_type_field not in field_list:
    #                 logging.info('    The crop_type_field {} is not present, '
    #                              'skipping'.format(crop_type_field))
    #                 continue
    #
    #             tile_tx = osr.CoordinateTransformation(tile_osr, crop_type_osr)
    #             tile_geom_copy = tile_geom.Clone()
    #             tile_geom_copy.Transform(tile_tx)
    #             crop_type_lyr.SetSpatialFilter(tile_geom_copy)
    #             if crop_type_lyr.GetFeatureCount() <= 0:
    #                 logging.info('    No intersecting features, skipping')
    #                 # input('ENTER')
    #                 continue
    #             else:
    #                 logging.debug('    Features: {}'.format(
    #                     crop_type_lyr.GetFeatureCount()))
    #
    #             # # This probably isn't helping that much (or doing anything)
    #             # #   and could probably be skipped
    #             # if clip_utm_zones:
    #             #     logging.info('  Filtering features to UTM Zone')
    #             #     utm_zone_ds = ogr.Open(utm_geojson, 0)
    #             #     utm_zone_lyr = utm_zone_ds.GetLayer()
    #             #     utm_zone_lyr.SetAttributeFilter("{} = '{}'".format(utm_field, utm_zone))
    #             #     for utm_zone_ftr in utm_zone_ds:
    #             #         utm_zone_geom = utm_zone_ftr.GetGeometryRef().Clone()
    #             #     field_lyr.SetSpatialFilter(utm_zone_geom)
    #             #     utm_zone_ds = None
    #
    #             # Convert field shapes to raster
    #             if rasterize_flag:
    #                 logging.debug('    Rasterizing features')
    #                 mem_driver = gdal.GetDriverByName('MEM')
    #                 mem_ds = mem_driver.Create(
    #                     '', tile_shape[0], tile_shape[1], 1, output_gtype)
    #                 mem_ds.SetProjection(tile_osr.ExportToWkt())
    #                 mem_ds.SetGeoTransform(tile_geo)
    #                 mem_band = mem_ds.GetRasterBand(1)
    #                 mem_band.Fill(output_nodata)
    #                 mem_band.SetNoDataValue(output_nodata)
    #                 gdal.RasterizeLayer(
    #                     mem_ds, [1], crop_type_lyr,
    #                     options=["ATTRIBUTE={}".format(crop_type_field)])
    #
    #                 logging.debug('    Reading rasterized array')
    #                 # mem_band = mem_ds.GetRasterBand(1)
    #                 mem_array = mem_band.ReadAsArray(
    #                     0, 0, tile_shape[0], tile_shape[1])
    #                 mem_mask = mem_array != output_nodata
    #                 # print(mem_array[3250:3260, 660:670])
    #                 # print(mem_mask[3250:3260, 660:670])
    #                 mem_ds = None
    #
    #                 logging.debug('    Writing updated array')
    #                 output_ds = gdal.Open(output_path, 1)
    #                 output_band = output_ds.GetRasterBand(1)
    #                 output_array = output_ds.ReadAsArray(
    #                     0, 0, tile_shape[0], tile_shape[1])
    #                 output_array[mem_mask] = mem_array[mem_mask]
    #                 output_band.WriteArray(output_array, 0, 0)
    #                 output_ds = None
    #                 del mem_array, mem_mask, mem_ds
    #
    #
    #             # Keep track of image (with data) that will need to be ingested
    #             if [bucket_path, asset_id] not in ingest_list:
    #                 ingest_list.append([bucket_path, asset_id])
    #
    #
    #         if clip_utm_zones and rasterize_flag:
    #             logging.debug('    Rasterizing UTM zone')
    #             utm_zone_ds = ogr.Open(utm_geojson, 0)
    #             utm_zone_lyr = utm_zone_ds.GetLayer()
    #             # UTM Zones in GeoJSON are integers
    #             utm_zone_lyr.SetAttributeFilter(
    #                 "{} = {}".format(utm_field, int(utm_zone)))
    #             utm_driver = gdal.GetDriverByName('MEM')
    #             utm_nodata = 0
    #             utm_ds = utm_driver.Create(
    #                 '', tile_shape[0], tile_shape[1], 1, gdal.GDT_Byte)
    #             utm_ds.SetProjection(tile_osr.ExportToWkt())
    #             utm_ds.SetGeoTransform(tile_geo)
    #             utm_band = utm_ds.GetRasterBand(1)
    #             utm_band.Fill(utm_nodata)
    #             utm_band.SetNoDataValue(utm_nodata)
    #             gdal.RasterizeLayer(utm_ds, [1], utm_zone_lyr, burn_values=[1])
    #             utm_zone_ds = None
    #
    #             logging.debug('    Reading rasterized UTM array')
    #             utm_band = utm_ds.GetRasterBand(1)
    #             utm_array = utm_band.ReadAsArray(0, 0, tile_shape[0], tile_shape[1])
    #             utm_ds = None
    #
    #             logging.debug('    Clipping UTM zone')
    #             output_ds = gdal.Open(output_path, 1)
    #             output_band = output_ds.GetRasterBand(1)
    #             output_nodata = output_band.GetNoDataValue()
    #             output_array = output_band.ReadAsArray(
    #                 0, 0, tile_shape[0], tile_shape[1])
    #             output_array[utm_array == utm_nodata] = output_nodata
    #             output_band.WriteArray(output_array, 0, 0)
    #             output_ds = None
    #
    #
    #         # CGM: Upload/ingest outside of loop is more efficient the first time
    #         #   The upload to bucket can then use "-m" flag and be faster
    #         if upload_flag:
    #             logging.info('  Uploading to bucket')
    #             args = ['gsutil', 'cp', output_path, bucket_path]
    #             if not logging.getLogger().isEnabledFor(logging.DEBUG):
    #                 args.insert(1, '-q')
    #             try:
    #                 subprocess.run(args, shell=shell_flag, check=True)
    #                 # os.remove(output_path)
    #             except Exception as e:
    #                 logging.exception(
    #                     '    Exception: {}\n    Skipping date'.format(e))
    #                 continue
    #
    #
    #         if ingest_flag:
    #             logging.info('  Ingesting into Earth Engine')
    #             task_id = ee.data.newTaskId()[0]
    #             logging.debug('    {}'.format(task_id))
    #             params = {
    #                 'name': asset_id,
    #                 'bands': [{'id': band_name}],
    #                 'tilesets': [{'sources': [{'uris': bucket_path}]}],
    #                 # 'tilesets': [{'id': 'image', 'sources': [{'uris': bucket_path}]}],
    #                 'pyramidingPolicy': 'MODE',
    #                 'properties': {
    #                     'core_version': openet.core.__version__,
    #                     'date_ingested': '{}'.format(
    #                         datetime.datetime.today().strftime('%Y-%m-%d')),
    #                     'mgrs_tile': mgrs_id.upper(),
    #                     'tool_name': TOOL_NAME,
    #                     'tool_version': TOOL_VERSION},
    #                 'startTime': datetime.datetime(year, 1, 1).isoformat() + '.000000000Z',
    #                 # 'missingData': {'values': [output_nodata]},
    #             }
    #             try:
    #                 ee.data.startIngestion(task_id, params, allow_overwrite=True)
    #             except Exception as e:
    #                 logging.exception('  Exception: {}\n  Skipping'.format(e))
    #                 continue
    #
    #             # # DEADBEEF - For now, assume the file is in the bucket
    #             # args = [
    #             #     'earthengine', '--no-use_cloud_api', 'upload', 'image',
    #             #     '--bands', ','.join([band_name]),
    #             #     '--asset_id', asset_id,
    #             #     '--time_start', '{:04d}-01-01'.format(year),
    #             #     # '--nodata_value', output_nodata,
    #             #     '--property', '(string)core_version={}'.format(
    #             #         openet.core.__version__),
    #             #     '--property', '(string)date_ingested={}'.format(
    #             #         datetime.datetime.today().strftime('%Y-%m-%d')),
    #             #     '--property', '(string)mgrs_tile={}'.format(mgrs_id),
    #             #     '--property', '(string)tool_name={}'.format(TOOL_NAME),
    #             #     '--property', '(string)tool_version={}'.format(TOOL_VERSION),
    #             #     '--pyramiding_policy', 'mode',
    #             #     bucket_path,
    #             # ]
    #             # try:
    #             #     subprocess.run(args, shell=shell_flag, check=True)
    #             # except Exception as e:
    #             #     logging.exception('    Exception: {}'.format(e))
    #             # sleep(0.5)
    #
    # # # CGM - Upload/ingest inside the loop to minimize time assets are missing
    # # if upload_flag:
    # #     logging.info('\nUploading to bucket')
    # #     args = ['gsutil', '-m', 'cp', os.path.join(output_ws, '*'),
    # #             '{}/{}'.format(bucket_name, bucket_folder)]
    # #     if not logging.getLogger().isEnabledFor(logging.DEBUG):
    # #         args.insert(1, '-q')
    # #     try:
    # #         subprocess.run(args, shell=shell_flag, check=True)
    # #         # os.remove(output_path)
    # #     except Exception as e:
    # #         logging.exception('    Exception: {}'.format(e))
    # #
    # #
    # # if ingest_flag:
    # #     logging.info('\nIngesting into Earth Engine')
    # #     # DEADBEEF - For now, assume the file is in the bucket
    # #     for bucket_path, asset_id in ingest_list:
    # #         logging.debug('{}'.format(asset_id))
    # #         args = [
    # #             'earthengine', '--no-use_cloud_api', 'upload', 'image',
    # #             '--bands', ','.join([band_name]),
    # #             '--asset_id', asset_id,
    # #             '--time_start', '{:04d}-01-01'.format(year),
    # #             # '--nodata_value', output_nodata,
    # #             '--property', '(string)DATE_INGESTED={}'.format(
    # #                 datetime.datetime.today().strftime('%Y-%m-%d')),
    # #             '--pyramiding_policy', 'mode',
    # #             bucket_path,
    # #         ]
    # #         logging.debug('  {}'.format(', '.join(args)))
    # #         try:
    # #             subprocess.run(args, shell=shell_flag, check=True)
    # #         except Exception as e:
    # #             logging.exception('    Exception: {}'.format(e))
    # #         sleep(0.5)


def mgrs_export_tiles(
        study_area_coll_id,
        mgrs_coll_id,
        study_area_property=None,
        study_area_features=[],
        mgrs_tiles=[],
        mgrs_skip_list=[],
        utm_zones=[],
        mgrs_property='mgrs',
        utm_property='utm',
        cell_size=30
        ):
    """Select MGRS tiles and metadata that intersect the study area geometry

    Parameters
    ----------
    study_area_coll_id : str
        Study area feature collection asset ID.
    mgrs_coll_id : str
        MGRS feature collection asset ID.
    study_area_property : str, optional
        Property name to use for inList() filter call of study area collection.
        Filter will only be applied if both 'study_area_property' and
        'study_area_features' parameters are both set.
    study_area_features : list, optional
        List of study area feature property values to filter on.
    mgrs_tiles : list, optional
        User defined MGRS tile subset.
    mgrs_skip_list : list, optional
        User defined list MGRS tiles to skip.
    utm_zones : list, optional
        User defined UTM zone subset.
    mgrs_property : str, optional
        MGRS property in the MGRS feature collection (the default is 'mgrs').
    utm_property : str, optional
        UTM zone property in the MGRS feature collection (the default is 'wrs2').
    cell_size : float, optional
        (the default is 30).

    Returns
    ------
    list of dicts: export information

    """
    # Build and filter the study area feature collection
    logging.debug('Building study area collection')
    logging.debug(f'  {study_area_coll_id}')
    study_area_coll = ee.FeatureCollection(study_area_coll_id)
    if (study_area_property == 'STUSPS' and
            'CONUS' in [x.upper() for x in study_area_features]):
        # Exclude AK, HI, AS, GU, PR, MP, VI, (but keep DC)
        study_area_features = [
            'AL', 'AR', 'AZ', 'CA', 'CO', 'CT', 'DC', 'DE', 'FL', 'GA',
            'IA', 'ID', 'IL', 'IN', 'KS', 'KY', 'LA', 'MA', 'MD', 'ME',
            'MI', 'MN', 'MO', 'MS', 'MT', 'NC', 'ND', 'NE', 'NH', 'NJ',
            'NM', 'NV', 'NY', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC', 'SD',
            'TN', 'TX', 'UT', 'VA', 'VT', 'WA', 'WI', 'WV', 'WY',
        ]
    # elif (study_area_property == 'STUSPS' and
    #         'WESTERN11' in [x.upper() for x in study_area_features]):
    #     study_area_features = [
    #         'AZ', 'CA', 'CO', 'ID', 'MT', 'NM', 'NV', 'OR', 'UT', 'WA', 'WY']
    study_area_features = sorted(list(set(study_area_features)))

    if study_area_property and study_area_features:
        logging.debug('  Filtering study area collection')
        logging.debug(f'  Property: {study_area_property}')
        logging.debug(f'  Features: {",".join(study_area_features)}')
        study_area_coll = study_area_coll.filter(
            ee.Filter.inList(study_area_property, study_area_features)
        )

    logging.info('Building MGRS tile list')
    tiles_coll = ee.FeatureCollection(mgrs_coll_id) \
        .filterBounds(study_area_coll.geometry())

    # Filter collection by user defined lists
    if utm_zones:
        logging.debug(f'  Filter User UTM Zones:    {utm_zones}')
        tiles_coll = tiles_coll.filter(ee.Filter.inList(utm_property, utm_zones))
    if mgrs_skip_list:
        logging.debug(f'  Filter MGRS skip list:    {mgrs_skip_list}')
        tiles_coll = tiles_coll.filter(ee.Filter.inList('mgrs', mgrs_skip_list).Not())
    if mgrs_tiles:
        logging.debug(f'  Filter MGRS tiles/zones:  {mgrs_tiles}')
        # Allow MGRS tiles to be subsets of the full tile code
        #   i.e. mgrs_tiles = 10TE, 10TF
        mgrs_filters = [
            ee.Filter.stringStartsWith(mgrs_property, mgrs_id.upper())
            for mgrs_id in mgrs_tiles
        ]
        tiles_coll = tiles_coll.filter(ee.call('Filter.or', mgrs_filters))

    # Drop the MGRS tile geometry to simplify the getInfo call
    def drop_geometry(ftr):
        return ee.Feature(None).copyProperties(ftr)

    logging.debug('  Requesting tile/zone info')
    tiles_info = utils.get_info(tiles_coll.map(drop_geometry))

    tiles_list = []
    for tile_ftr in tiles_info['features']:
        mgrs_id = tile_ftr['properties']['mgrs'].upper()
        tile_extent = [
            int(tile_ftr['properties']['xmin']), int(tile_ftr['properties']['ymin']),
            int(tile_ftr['properties']['xmax']), int(tile_ftr['properties']['ymax'])
        ]
        tile_geo = [cell_size, 0, tile_extent[0], 0, -cell_size, tile_extent[3]]
        tile_shape = [
            int((tile_extent[2] - tile_extent[0]) / cell_size),
            int((tile_extent[3] - tile_extent[1]) / cell_size)
        ]
        tiles_list.append({
            'crs': 'EPSG:{:d}'.format(int(tile_ftr['properties']['epsg'])),
            'extent': tile_extent,
            'geo_str': '[' + ','.join(map(str, tile_geo)) + ']',
            'index': mgrs_id,
            'maxpixels': tile_shape[0] * tile_shape[1] + 1,
            'shape_str': '{0}x{1}'.format(*tile_shape),
            'utm': int(mgrs_id[:2]),
        })

    export_list = [tile for tile in sorted(tiles_list, key=lambda k: k['index'])]

    return export_list


def extent_geom(extent, swap_ul_lr=False):
    """GDAL geometry object of the extent list

    Parameters
    ----------
    extent : list
        GDAL style extent (xmin, ymin, xmax, ymax)
    swap_ul_lsr : boolean
        If True, swap the ymin and xmax values.

    Returns
    -------
    ogr.geometry

    """
    if swap_ul_lr:
        extent = [extent[0], extent[2], extent[1], extent[3]]

    ring = ogr.Geometry(ogr.wkbLinearRing)
    ring.AddPoint(extent[0], extent[3])
    ring.AddPoint(extent[2], extent[3])
    ring.AddPoint(extent[2], extent[1])
    ring.AddPoint(extent[0], extent[1])
    ring.CloseRings()
    polygon = ogr.Geometry(ogr.wkbPolygon)
    polygon.AddGeometry(ring)
    return polygon


def arg_parse():
    """"""
    parser = argparse.ArgumentParser(
        description='Build crop type MGRS tiles from a shapefile',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument(
        '--tiles', default='', nargs='+',
        help='Comma/space separated list of MGRS tiles')
    parser.add_argument(
        '--years', default='', nargs='+',
        help='Comma separated list and/or range of years')
    parser.add_argument(
        '--key', type=utils.arg_valid_file, metavar='FILE',
        help='JSON key file')
    parser.add_argument(
        '--overwrite', default=False, action='store_true',
        help='Force overwrite of existing files')
    parser.add_argument(
        '--debug', default=logging.INFO, const=logging.DEBUG,
        help='Debug level logging', action='store_const', dest='loglevel')
    args = parser.parse_args()

    return args


if __name__ == '__main__':
    args = arg_parse()
    logging.basicConfig(level=args.loglevel, format='%(message)s')
    logging.getLogger('googleapiclient').setLevel(logging.ERROR)

    main(
        years=args.years,
        mgrs_tiles=args.tiles,
        overwrite_flag=args.overwrite,
        gee_key_file=args.key,
    )
