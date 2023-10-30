#--------------------------------
# Name:         crop_type_asset_mgrs_collection.py
# Purpose:      Build crop type MGRS tiles from the crop feature collections
#--------------------------------

import argparse
import datetime
import logging
import os
import pprint

import ee
import pandas as pd

import openet.core
import openet.core.utils as utils

TOOL_NAME = 'crop_type_asset_mgrs_collection'
# TOOL_NAME = os.path.basename(__file__)
TOOL_VERSION = '0.1.1'


def main(years=[], mgrs_tiles=[], overwrite_flag=False, delay=0, gee_key_file=None):
    """Build and ingest crop type MGRS tiles from a feature collection

    Parameters
    ----------
    years : list, optional
    mgrs_tiles : list, optional
    overwrite_flag : bool, optional
        If True, overwrite existing files (the default is False).
    delay : float, optional
        Delay time between each export task (the default is 0).
    gee_key_file : str, None, optional
        Earth Engine service account JSON key file (the default is None).

    Returns
    -------
    None

    """
    logging.info('\nBuild crop type MGRS tiles from the crop feature collections')

    # Hardcoded parameters
    export_coll_id = 'projects/earthengine-legacy/assets/' \
                     'projects/openet/crop_type/v2022a'
    # export_coll_id = 'projects/earthengine-legacy/assets/' \
    #                  'projects/openet/crop_type/annual_provisional'
    # export_coll_id = 'projects/earthengine-legacy/assets/' \
    #                  'projects/openet/crop_type/annual'
    export_band_name = 'crop_type'

    crop_type_folder_id = 'projects/earthengine-legacy/assets/' \
                          'projects/openet/featureCollections/2022-03-15b'
    # crop_type_folder_id = 'projects/earthengine-legacy/assets/' \
    #                       'projects/openet/field_boundaries/provisional'

    mgrs_ftr_coll_id = 'projects/earthengine-legacy/assets/' \
                       'projects/openet/mgrs/conus_gridmet/zones'
    mgrs_mask_coll_id = 'projects/earthengine-legacy/assets/' \
                        'projects/openet/mgrs/conus_gridmet/zone_mask'

    # For now, hardcode the study area as the CONUS
    # Allow the user to set a subset of states also
    # study_area_coll = projects/climate-engine/featureCollections/shp_new/cb_2017_us_state_5m
    study_area_coll_id = 'TIGER/2018/States'
    study_area_property = 'STUSPS'
    study_area_features = 'CONUS'
    # study_area_features = 'AZ, CA, CO, ID, MT, NM, NV, OR, UT, WA, WY'
    # study_area_features = 'NV'

    cdl_coll_id = 'USDA/NASS/CDL'
    landiq_coll_id = 'projects/earthengine-legacy/assets/' \
                     'projects/openet/crop_type/land_iq'

    # year_min = 1997
    year_min = 2000
    year_max = 2023
    if not years:
        years = list(range(year_min, year_max+1))
    else:
        years = sorted(list(set(
            int(year) for year_str in years
            for year in utils.str_ranges_2_list(year_str)
            if ((year <= year_max) and (year >= year_min))
        )))
    logging.info(f'Years:  {", ".join(map(str, years))}')

    # Load the CDL annual crop remap
    # TODO: Get the script path instead (in case it is different than the cwd)
    remap_path = os.path.join(
        os.path.dirname(os.getcwd()), 'crop_type',
        'cdl_annual_crop_remap_table.csv'
    )
    remap_df = pd.read_csv(remap_path, comment='#').sort_values(by='IN')
    cdl_annual_remap = dict(zip(remap_df.IN, remap_df.OUT))
    # Set all unassigned values to remap to themselves
    for cdl_code in set(range(1, 256)) - set(cdl_annual_remap.keys()):
        cdl_annual_remap[cdl_code] = cdl_code
    remap_in, remap_out = map(list, zip(*cdl_annual_remap.items()))

    # mgrs_skip_list = []
    # utm_zones = []

    # Parse user inputs
    mgrs_tiles = sorted([y.strip() for x in mgrs_tiles for y in x.split(',')])
    # mgrs_tiles = sorted([x.strip() for x in mgrs_tiles.split(',')])
    study_area_features = sorted([x.strip() for x in study_area_features.split(',')])
    logging.info(f'Tiles:    {", ".join(mgrs_tiles)}')
    logging.info(f'Features: {", ".join(study_area_features)}')
    # input('ENTER')


    logging.info('\nInitializing Earth Engine')
    if gee_key_file:
        logging.info(f'  Using service account key file: {gee_key_file}')
        # The "EE_ACCOUNT" parameter is not used if the key file is valid
        ee.Initialize(ee.ServiceAccountCredentials('', key_file=gee_key_file))
    else:
        ee.Initialize()
    ee.Number(1).getInfo()


    # Get current running tasks
    tasks = utils.get_ee_tasks()
    if logging.getLogger().getEffectiveLevel() == logging.DEBUG:
        logging.debug(f'  Tasks: {len(tasks)}')
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
    # CGM - Note that "projects/earthengine-legacy/assets/" is not in the ID
    logging.debug('\nGetting GEE asset list')
    asset_list = utils.get_ee_assets(export_coll_id)
    if logging.getLogger().getEffectiveLevel() == logging.DEBUG:
        pprint.pprint(asset_list[:10])


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


    # Get a list of the available state field feature collections
    crop_type_states = [
        asset['id'].split('/')[-1]
        for asset in ee.data.listAssets({'parent': crop_type_folder_id})['assets']
        if asset['type'] == 'TABLE'
    ]
    logging.info('\nStates with field feature collections:'
                 '\n  {}'.format(', '.join(crop_type_states)))


    # Get the last available CDL year
    # TODO: Should probably wrap in a try/except
    cdl_year_max = int(utils.get_info(
        ee.ImageCollection(cdl_coll_id)
        .limit(1, 'system:time_start', False).first()
        .get('system:index')
    ))
    logging.info(f'\nLast available CDL year: {cdl_year_max}')


    # Process each tile separately
    logging.info('\nImage Exports')
    for export_n, export_info in enumerate(export_list):
        mgrs_tile = export_info['index'].upper()
        logging.info('MGRS Tile: {} ({}/{})'.format(
            mgrs_tile, export_n + 1, len(export_list)
        ))
        logging.debug(f'  Shape:      {export_info["shape_str"]}')
        logging.debug(f'  Transform:  {export_info["geo_str"]}')
        logging.debug(f'  Extent:     {export_info["extent"]}')
        logging.debug(f'  MaxPixels:  {export_info["maxpixels"]}')

        mgrs_geom = ee.Geometry.Rectangle(
            export_info['extent'], proj=export_info['crs'], geodesic=False
        )

        mgrs_mask_id = f'{mgrs_mask_coll_id}/{mgrs_tile.lower()}'
        mgrs_mask_img = ee.Image(mgrs_mask_id)
        # logging.debug(f'{mgrs_mask_id}')
        # pprint.pprint(mgrs_mask_img.getInfo())

        # Get a list of states that could intersect the MGRS tile
        # Use this state list to select the field collections
        state_coll = ee.FeatureCollection(study_area_coll_id).filterBounds(mgrs_geom)
        mgrs_states = state_coll.aggregate_array(study_area_property).getInfo()
        logging.debug(f'  States intersecting the MGRS tile/zone: '
                      f'{", ".join(sorted(mgrs_states))}')

        logging.info('  Building crop type feature collection')
        field_coll = ee.FeatureCollection([])
        field_states = sorted(list(set(mgrs_states) | set(crop_type_states)))
        logging.info(f'    States: {", ".join(sorted(mgrs_states))}')
        for state in field_states:
            if state not in crop_type_states:
                continue
            crop_type_coll_id = f'{crop_type_folder_id}/{state.upper()}'
            crop_type_coll = ee.FeatureCollection(crop_type_coll_id)\
                .filterBounds(mgrs_geom)
            field_coll = field_coll.merge(crop_type_coll)

        # CGM - There may be tiles without fields, especially for the eastern
        #   states, but we still want to build an image.  For now, just pause
        #   and force the user to acknowledge that the field count is 0.
        # Need to check if the rasterize will work if there are no fields
        # field_count = field_coll.size().getInfo()
        # logging.info(f'  Features: {field_count}')
        # if field_count <= 0:
        #     input('ENTER')
        #     # logging.info('  No intersecting features - skipping')
        #     # continue

        for year in years:
            image_id = f'{mgrs_tile}_{year}0101'
            asset_id = f'{export_coll_id}/{image_id}'
            asset_short_id = asset_id.replace(
                'projects/earthengine-legacy/assets/', '')
            export_id = f'crop_type_{image_id}'
            crop_type_field = f'CROP_{year}'
            logging.info(f'{asset_id}')
            logging.debug(f'  {asset_short_id}')
            logging.debug(f'  {export_id}')

            if overwrite_flag:
                if export_id in tasks.keys():
                    logging.info('  Task already submitted, cancelling')
                    ee.data.cancelTask(tasks[export_id])
                # This is intentionally not an "elif" so that a task can be
                # cancelled and an existing image/file/asset can be removed
                if asset_id in asset_list or asset_short_id in asset_list:
                    logging.info('  Asset already exists, removing')
                    ee.data.deleteAsset(asset_id)
            else:
                if export_id in tasks.keys():
                    logging.info('  Task already submitted, exiting')
                    continue
                elif asset_id in asset_list or asset_short_id in asset_list:
                    logging.info('  Asset already exists, skipping')
                    continue

            properties = {
                'system:time_start': ee.Date.fromYMD(year, 1, 1).millis(),
                'core_version': openet.core.__version__,
                'crop_type_folder': crop_type_folder_id,
                'crop_type_states': ','.join(mgrs_states),
                'date_ingested': datetime.datetime.today().strftime('%Y-%m-%d'),
                'mgrs_tile': mgrs_tile,
                'tool_name': TOOL_NAME,
                'tool_version': TOOL_VERSION,
            }


            # Start with the MGRS mask set to 0
            output_img = mgrs_mask_img.updateMask(0)

            # Rasterize the fields
            # Added the uint8 since image was coming back as a double
            field_img = field_coll\
                .filterMetadata(crop_type_field, 'greater_than', 0)\
                .reduceToImage([crop_type_field], ee.Reducer.first())\
                .uint8()
            output_img = output_img.addBands(field_img.rename(['fields']))
            properties['field_states'] = ','.join(field_states)


            # If a California tile, mosaic in the LandIQ image for the UTM zone
            if mgrs_tile in ['10S', '10T', '11S']:
                if year in [2014, 2016, 2018]:
                    landiq_img_id = f'{landiq_coll_id}/{year}'
                    landiq_img = ee.Image(landiq_img_id)
                elif year < 2009:
                    # Don't include LandIQ before 2009
                    landiq_img = None
                elif year in [2009, 2010, 2011, 2012, 2013]:
                    # Use a 2014 remapped annual crop image for pre-2014 years
                    # Remove the urban and managed wetland polygons
                    landiq_img_id = f'{landiq_coll_id}/2014'
                    landiq_img = ee.Image(landiq_img_id)\
                        .remap(remap_in, remap_out)\
                        .updateMask(ee.Image(landiq_img_id).neq(82)
                                    .And(ee.Image(landiq_img_id).neq(87)))
                elif year == 2015:
                    # Should we use the zone specific images?
                    landiq_img_id = f'{landiq_coll_id}/2014'
                    # landiq_img_id = f'{landiq_coll_id}/2014_zone{mgrs_tiles[:2]}'
                    landiq_img = ee.Image(landiq_img_id).remap(remap_in, remap_out)
                elif year == 2017:
                    landiq_img_id = f'{landiq_coll_id}/2016'
                    landiq_img = ee.Image(landiq_img_id).remap(remap_in, remap_out)
                elif year > 2018:
                    landiq_img_id = f'{landiq_coll_id}/2018'
                    landiq_img = ee.Image(landiq_img_id).remap(remap_in, remap_out)
                else:
                    logging.warning(f'unexpected year: {year}')
                    input('ENTER')

                if landiq_img:
                    output_img = output_img.addBands(landiq_img.rename(['landiq']))
                    properties['landiq_img_id'] = landiq_img_id


            # For California, always use the annual remapped CDL
            # Use a 2008 remapped annual crop image for all pre-2008 years
            # For all years after the last available CDL year,
            #   use a annual crop remapped version of the last CDL year
            # Never use the CA 2007 image
            if mgrs_tile in ['10S', '10T', '11S']:
                if year < 2008:
                    cdl_img_id = f'{cdl_coll_id}/2008'
                elif year > cdl_year_max:
                    cdl_img_id = f'{cdl_coll_id}/{cdl_year_max}'
                else:
                    cdl_img_id = f'{cdl_coll_id}/{year}'
                # The clip could be changed to a mask using the CIMIS mask?
                ca_ftr = ee.FeatureCollection('TIGER/2018/States')\
                    .filterMetadata('STUSPS', 'equals', 'CA').first()
                cdl_ca_img = ee.Image(cdl_img_id).select(['cropland'])\
                    .remap(remap_in, remap_out)\
                    .clip(ca_ftr.geometry())
                output_img = output_img.addBands(cdl_ca_img.rename(['cdl_ca_img']))
                properties['cdl_ca_img_id'] = cdl_ca_img


            # Attempt to use the year specific CDL images for pre2008 years
            if year < 2008:
                if year == 2005 and mgrs_tiles not in ['11T', '12T']:
                    # Don't use the 2005a image for Idaho tiles
                    # First add the Mississippi 2005b image
                    output_img.addBands(ee.Image(f'{cdl_coll_id}/2005b')
                        .select(['cropland'], ['cdl_ms_img'])
                    )
                    # Then bring in the other 2005 image
                    cdl_img_id = f'{cdl_coll_id}/2005a'
                elif year == 2007:
                    # Never use the California 2007b image
                    cdl_img_id = f'{cdl_coll_id}/2007a'
                else:
                    cdl_img_id = f'{cdl_coll_id}/{year}'
                cdl_state_img = ee.Image(cdl_img_id)\
                    .select(['cropland'], ['cdl_state_img'])
                output_img = output_img.addBands(cdl_state_img)
                # CGM - Not sure what to set the property to here
                # An MGRS tile could be a combination of a pre2008 state image
                #   and a 2008 image
                properties['cdl_state_img_id'] = cdl_img_id


            # For any years after the last available CDL year
            #   use the annual crop remapped version of the last year image
            # For pre-2008 years, use the annual crop remapped 2008 images
            if year > cdl_year_max:
                cdl_img_id = f'{cdl_coll_id}/{cdl_year_max}'
                cdl_img = ee.Image(cdl_img_id).select(['cropland'])\
                    .remap(remap_in, remap_out)
                # crop_source = f'{cdl_img_id} - remapped annual crops'
            elif year < 2008:
                cdl_img_id = f'{cdl_coll_id}/2008'
                cdl_img = ee.Image(cdl_img_id).select(['cropland'])\
                    .remap(remap_in, remap_out)
                # crop_source = f'{cdl_img_id} - remapped annual crops'
            else:
                cdl_img_id = f'{cdl_coll_id}/{year}'
                cdl_img = ee.Image(cdl_img_id).select(['cropland'])
                # crop_source = f'{cdl_img_id}'
            output_img = output_img.addBands(cdl_img.rename(['cdl_conus_img']))
            properties['cdl_img_id'] = cdl_img_id

            # pprint.pprint(output_img.getInfo())
            # input('ENTER')


            # TODO: Figure out if firstNonNull will return first or last band first?
            output_img = output_img\
                .reduce(ee.Reducer.firstNonNull()) \
                .updateMask(mgrs_mask_img)\
                .rename(['cropland'])\
                .set(properties)
            # output_img = ee.ImageCollection([crop_type_img, field_img])\
            #     .mosaic()\
            #     .updateMask(mgrs_mask_img)\
            #     .set(properties)

            # Build export tasks
            task = ee.batch.Export.image.toAsset(
                output_img,
                description=export_id,
                assetId=asset_id,
                dimensions=export_info['shape_str'],
                crs=export_info['crs'],
                crsTransform=export_info['geo_str'],
                maxPixels=export_info['maxpixels']*2,
                pyramidingPolicy={'cropland': 'mode'},
            )

            logging.info('  Starting export task')
            utils.ee_task_start(task)

            # Pause before starting the next export task
            utils.delay_task(delay)

            logging.debug('')


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
            # 'geo': tile_geo,
            'geo_str': '[' + ','.join(map(str, tile_geo)) + ']',
            'index': mgrs_id,
            'maxpixels': tile_shape[0] * tile_shape[1],
            # 'shape': tile_shape,
            'shape_str': '{0}x{1}'.format(*tile_shape),
            'utm': int(mgrs_id[:2]),
        })

    export_list = [tile for tile in sorted(tiles_list, key=lambda k: k['index'])]

    return export_list


def arg_parse():
    """"""
    parser = argparse.ArgumentParser(
        description='Build crop type MGRS tiles from a feature collection',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument(
        '--tiles', default='', nargs='+',
        help='Comma/space separated list of MGRS tiles')
    parser.add_argument(
        '--years', default='', nargs='+',
        help='Comma separated list and/or range of years')
    parser.add_argument(
        '--delay', default=0, type=float,
        help='Delay (in seconds) between each export tasks')
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
        delay=args.delay,
        gee_key_file=args.key,
    )
