import argparse
from builtins import input
import configparser
import datetime
import json
import logging
import os
import pprint
import re
import time

import ee
from google.cloud import datastore

import openet.sharpen
import openet.sharpen.thermal
import openet.core
import openet.core.utils as utils
# CGM - Using SIMS for building image ID list (for now)
import openet.sims as model
# import openet.disalexi as model

TOOL_NAME = 'tir_image_wrs2_export'
TOOL_VERSION = '0.1.6'


def main(ini_path=None, overwrite_flag=False, delay_time=0, gee_key_file=None,
         max_ready=-1, reverse_flag=False, tiles=None, update_flag=False,
         log_tasks=True, recent_days=0, start_dt=None, end_dt=None):
    """Export Landsat sharpened thermal images

    Parameters
    ----------
    ini_path : str
        Input file path.
    overwrite_flag : bool, optional
        If True, overwrite existing files (the default is False).
    delay_time : float, optional
        Delay time in seconds between starting export tasks (or checking the
        number of queued tasks, see "max_ready" parameter).  The default is 0.
    gee_key_file : str, None, optional
        Earth Engine service account JSON key file (the default is None).
    max_ready: int, optional
        Maximum number of queued "READY" tasks.  The default is -1 which is
        implies no limit to the number of tasks that will be submitted.
    reverse_flag : bool, optional
        If True, process WRS2 tiles in reverse order (the default is False).
    tiles : str, None, optional
        List of MGRS tiles to process (the default is None).
    update_flag : bool, optional
        If True, only overwrite scenes with an older model version.
    log_tasks : bool, optional
        If True, log task information to the datastore (the default is True).
    recent_days : int, optional
        Limit start/end date range to this many days before the current date
        (the default is 0 which is equivalent to not setting the parameter and
         will use the INI start/end date directly).
    start_dt : datetime, optional
        Override the start date in the INI file
        (the default is None which will use the INI start date).
    end_dt : datetime, optional
        Override the (inclusive) end date in the INI file
        (the default is None which will use the INI end date).

    Returns
    -------
    None

    """
    logging.info('\nExport Landsat sharpened thermal images')

    # CGM - Which format should we use for the WRS2 tile?
    wrs2_tile_fmt = 'p{:03d}r{:03d}'
    # wrs2_tile_fmt = '{:03d}{:03d}'
    wrs2_tile_re = re.compile('p?(\d{1,3})r?(\d{1,3})')

    # List of path/rows to skip
    wrs2_skip_list = [
        'p049r026',  # Vancouver Island, Canada
        # 'p047r031', # North California coast
        'p042r037',  # San Nicholas Island, California
        # 'p041r037', # South California coast
        'p040r038', 'p039r038', 'p038r038', # Mexico (by California)
        'p037r039', 'p036r039', 'p035r039', # Mexico (by Arizona)
        'p034r039', 'p033r039', # Mexico (by New Mexico)
        'p032r040',  # Mexico (West Texas)
        'p029r041', 'p028r042', 'p027r043', 'p026r043',  # Mexico (South Texas)
        'p019r040', # West Florida coast
        'p016r043', 'p015r043', # South Florida coast
        'p014r041', 'p014r042', 'p014r043', # East Florida coast
        'p013r035', 'p013r036', # North Carolina Outer Banks
        'p013r026', 'p012r026', # Canada (by Maine)
        'p011r032', # Rhode Island coast
    ]
    wrs2_path_skip_list = [9, 49]
    wrs2_row_skip_list = [25, 24, 43]

    mgrs_skip_list = []

    export_id_fmt = '{model}_{index}'

    # TODO: Move to INI file
    clip_ocean_flag = True

    # Read config file
    ini = configparser.ConfigParser(interpolation=None)
    ini.read_file(open(ini_path, 'r'))

    # # Force conversion of unicode to strings
    # for section in ini.sections():
    #     ini[str(section)] = {}
    #     for k, v in ini[section].items():
    #         ini[str(section)][str(k)] = v

    # TODO: Move to INI parsing function or module
    try:
        model_name = str(ini['INPUTS']['et_model']).upper()
    except KeyError:
        raise ValueError('"et_model" parameter was not set in INI')
    except Exception as e:
        raise e
    logging.info('  ET Model: {}'.format(model_name))

    try:
        study_area_coll_id = str(ini['INPUTS']['study_area_coll'])
    except KeyError:
        raise ValueError('"study_area_coll" parameter was not set in INI')
    except Exception as e:
        raise e

    try:
        start_date = str(ini['INPUTS']['start_date'])
    except KeyError:
        raise ValueError('"start_date" parameter was not set in INI')
    except Exception as e:
        raise e

    try:
        end_date = str(ini['INPUTS']['end_date'])
    except KeyError:
        raise ValueError('"end_date" parameter was not set in INI')
    except Exception as e:
        raise e

    try:
        collections = str(ini['INPUTS']['collections'])
        collections = sorted([x.strip() for x in collections.split(',')])
    except KeyError:
        raise ValueError('"collections" parameter was not set in INI')
    except Exception as e:
        raise e

    try:
        mgrs_ftr_coll_id = str(ini['EXPORT']['mgrs_ftr_coll'])
    except KeyError:
        raise ValueError('"mgrs_ftr_coll" parameter was not set in INI')
    except Exception as e:
        raise e

    # Optional parameters
    try:
        study_area_property = str(ini['INPUTS']['study_area_property'])
    except KeyError:
        study_area_property = None
        logging.debug('  study_area_property: not set in INI, defaulting to None')
    except Exception as e:
        raise e

    try:
        study_area_features = str(ini['INPUTS']['study_area_features'])
        study_area_features = sorted([
            x.strip() for x in study_area_features.split(',')])
    except KeyError:
        study_area_features = []
        logging.debug('  study_area_features: not set in INI, defaulting to []')
    except Exception as e:
        raise e

    try:
        scene_coll_id = str(ini['EXPORT']['scene_coll'])
    except KeyError:
        scene_coll_id = 'projects/earthengine-legacy/assets/' \
                        'projects/openet/{}/landsat/scene'.format(model_name.lower())
        # scene_coll_id = 'projects/openet/assets/{}/landsat/scene'.format(
        #     model_name.lower())
        logging.debug('  scene_coll: not set in INI, '
                      'defaulting to {}'.format(scene_coll_id))
    except Exception as e:
        raise e

    try:
        wrs2_tiles = str(ini['INPUTS']['wrs2_tiles'])
        wrs2_tiles = sorted([x.strip() for x in wrs2_tiles.split(',')])
    except KeyError:
        wrs2_tiles = []
        logging.debug('  wrs2_tiles: not set in INI, defaulting to []')
    except Exception as e:
        raise e

    try:
        mgrs_tiles = str(ini['EXPORT']['mgrs_tiles'])
        mgrs_tiles = sorted([x.strip() for x in mgrs_tiles.split(',')])
        # CGM - Remove empty strings caused by trailing or extra commas
        mgrs_tiles = [x.upper() for x in mgrs_tiles if x]
        logging.debug('  mgrs_tiles: {}'.format(mgrs_tiles))
    except KeyError:
        mgrs_tiles = []
        logging.debug('  mgrs_tiles: not set in INI, defaulting to []')
    except Exception as e:
        raise e

    try:
        utm_zones = str(ini['EXPORT']['utm_zones'])
        utm_zones = sorted([int(x.strip()) for x in utm_zones.split(',')])
        logging.debug('  utm_zones: {}'.format(utm_zones))
    except KeyError:
        utm_zones = []
        logging.debug('  utm_zones: not set in INI, defaulting to []')
    except Exception as e:
        raise e

    try:
        output_type = str(ini['EXPORT']['output_type'])
    except KeyError:
        output_type = 'float'
        # output_type = 'int16'
        logging.debug('  output_type: not set in INI, '
                      'defaulting to {}'.format(output_type))
    except Exception as e:
        raise e

    try:
        scale_factor = int(ini['EXPORT']['scale_factor'])
    except KeyError:
        scale_factor = 1
        # scale_factor = 10000
        logging.debug('  scale_factor: not set in INI, '
                      'defaulting to {}'.format(scale_factor))
    except Exception as e:
        raise e

    try:
        export_id_name = '_' + str(ini['EXPORT']['export_id_name'])
    except KeyError:
        export_id_name = ''
        logging.debug('  export_id_name: not set in INI, defaulting to ""')
    except Exception as e:
        raise e

    # If the user set the tiles argument, use these instead of the INI values
    if tiles:
        logging.info('\nOverriding INI mgrs_tiles and utm_zones parameters')
        logging.info('  user tiles: {}'.format(tiles))
        mgrs_tiles = sorted([y.strip() for x in tiles for y in x.split(',')])
        mgrs_tiles = [x.upper() for x in mgrs_tiles if x]
        logging.info('  mgrs_tiles: {}'.format(', '.join(mgrs_tiles)))
        utm_zones = sorted(list(set([int(x[:2]) for x in mgrs_tiles])))
        logging.info('  utm_zones:  {}'.format(', '.join(map(str, utm_zones))))

    today_dt = datetime.datetime.now()
    today_dt = today_dt.replace(hour=0, minute=0, second=0, microsecond=0)
    if recent_days:
        logging.info('\nOverriding INI "start_date" and "end_date" parameters')
        logging.info('  Recent days: {}'.format(recent_days))
        end_dt = today_dt - datetime.timedelta(days=1)
        start_dt = today_dt - datetime.timedelta(days=recent_days)
        start_date = start_dt.strftime('%Y-%m-%d')
        end_date = end_dt.strftime('%Y-%m-%d')
    elif start_dt and end_dt:
        # Attempt to use the function start/end dates
        logging.info('\nOverriding INI "start_date" and "end_date" parameters')
        logging.info('  Custom date range')
        start_date = start_dt.strftime('%Y-%m-%d')
        end_date = end_dt.strftime('%Y-%m-%d')
    else:
        # Parse the INI start/end dates
        logging.info('\nINI date range')
        try:
            start_dt = datetime.datetime.strptime(start_date, '%Y-%m-%d')
            end_dt = datetime.datetime.strptime(end_date, '%Y-%m-%d')
        except Exception as e:
            raise e
    logging.info('  Start: {}'.format(start_date))
    logging.info('  End:   {}'.format(end_date))

    # TODO: Add a few more checks on the dates
    if end_dt < start_dt:
        raise ValueError('end date can not be before start date')

    logging.info('\nInterpolation date range')
    iter_start_dt = start_dt
    iter_end_dt = end_dt + datetime.timedelta(days=1)
    # iter_start_dt = start_dt - datetime.timedelta(days=interp_days)
    # iter_end_dt = end_dt + datetime.timedelta(days=interp_days+1)
    logging.info('  Start: {}'.format(iter_start_dt.strftime('%Y-%m-%d')))
    logging.info('  End:   {}'.format(iter_end_dt.strftime('%Y-%m-%d')))


    logging.info('\nInitializing Earth Engine')
    if gee_key_file:
        logging.info('  Using service account key file: {}'.format(gee_key_file))
        # The "EE_ACCOUNT"  doesn't seem to be used if the key file is valid
        ee.Initialize(ee.ServiceAccountCredentials('test', key_file=gee_key_file))
    else:
        ee.Initialize()
    utils.get_info(ee.Number(1))


    # TODO: set datastore key file as a parameter?
    datastore_key_file = 'openet-dri-datastore.json'
    if log_tasks and not os.path.isfile(datastore_key_file):
        logging.info('\nTask logging disabled, datastore key does not exist')
        log_tasks = False
        # input('ENTER')
    if log_tasks:
        logging.info('\nInitializing task datastore client')
        try:
            datastore_client = datastore.Client.from_service_account_json(
                datastore_key_file)
        except Exception as e:
            logging.error('{}'.format(e))
            return False


    # Get current running tasks
    tasks = utils.get_ee_tasks()
    if logging.getLogger().getEffectiveLevel() == logging.DEBUG:
        utils.print_ee_tasks()
        input('ENTER')


    # Build output collection and folder if necessary
    logging.debug('\nExport Collection: {}'.format(scene_coll_id))
    if not ee.data.getInfo(scene_coll_id.rsplit('/', 1)[0]):
        logging.debug('\nFolder does not exist and will be built'
                      '\n  {}'.format(scene_coll_id.rsplit('/', 1)[0]))
        input('Press ENTER to continue')
        ee.data.createAsset({'type': 'FOLDER'}, scene_coll_id.rsplit('/', 1)[0])
    if not ee.data.getInfo(scene_coll_id):
        logging.info('\nExport collection does not exist and will be built'
                     '\n  {}'.format(scene_coll_id))
        input('Press ENTER to continue')
        ee.data.createAsset({'type': 'IMAGE_COLLECTION'}, scene_coll_id)


# Get list of MGRS tiles that intersect the study area
    logging.debug('\nMGRS Tiles/Zones')
    export_list = mgrs_export_tiles(
        study_area_coll_id=study_area_coll_id,
        mgrs_coll_id=mgrs_ftr_coll_id,
        study_area_property=study_area_property,
        study_area_features=study_area_features,
        mgrs_tiles=mgrs_tiles,
        mgrs_skip_list=mgrs_skip_list,
        utm_zones=utm_zones,
        wrs2_tiles=wrs2_tiles,
    )
    if not export_list:
        logging.error('\nEmpty export list, exiting')
        return False
    # pprint.pprint(export_list)
    # input('ENTER')


    # Process each WRS2 tile separately
    logging.info('\nImage Exports')
    wrs2_tiles = []
    for export_info in sorted(export_list, key=lambda i: i['index'],
                              reverse=reverse_flag):
        logging.info('{}'.format(export_info['index']))
        # logging.info('  {} - {}'.format(
        #     export_info['index'], ', '.join(export_info['wrs2_tiles'])))
        tile_count = len(export_info['wrs2_tiles'])
        tile_list = sorted(export_info['wrs2_tiles'], reverse=not(reverse_flag))

        for export_n, wrs2_tile in enumerate(tile_list):
            path, row = map(int, wrs2_tile_re.findall(wrs2_tile)[0])
            if wrs2_tile in wrs2_tiles:
                logging.info('{} {} ({}/{}) - already processed'.format(
                    export_info['index'], wrs2_tile, export_n + 1, tile_count))
                continue
            elif wrs2_skip_list and wrs2_tile in wrs2_skip_list:
                logging.info('{} {} ({}/{}) - in wrs2 skip list'.format(
                    export_info['index'], wrs2_tile, export_n + 1, tile_count))
                continue
            elif wrs2_row_skip_list and row in wrs2_row_skip_list:
                logging.info('{} {} ({}/{}) - in wrs2 row skip list'.format(
                    export_info['index'], wrs2_tile, export_n + 1, tile_count))
                continue
            elif wrs2_path_skip_list and path in wrs2_path_skip_list:
                logging.info('{} {} ({}/{}) - in wrs2 path skip list'.format(
                    export_info['index'], wrs2_tile, export_n + 1, tile_count))
                continue
            else:
                logging.info('{} {} ({}/{})'.format(
                    export_info['index'], wrs2_tile, export_n + 1, tile_count))
            wrs2_tiles.append(wrs2_tile)

            # path, row = map(int, wrs2_tile_re.findall(export_info['index'])[0])
            # logging.info('WRS2 tile: {}  ({}/{})'.format(
            #     export_info['index'], export_n + 1, len(export_list)))
            #
            # logging.debug('  Shape:     {}'.format(export_info['shape']))
            # logging.debug('  Transform: {}'.format(export_info['geo_str']))
            # logging.debug('  Extent:    {}'.format(export_info['extent']))
            # logging.debug('  MaxPixels: {}'.format(export_info['maxpixels']))

            filter_args = {}
            for coll_id in collections:
                filter_args[coll_id] = [
                    {'type': 'equals', 'leftField': 'WRS_PATH', 'rightValue': path},
                    {'type': 'equals', 'leftField': 'WRS_ROW', 'rightValue': row}]
            # logging.debug('  Filter Args: {}'.format(filter_args))

            # Get the full Landsat collection
            # Collection end date is exclusive
            model_obj = model.Collection(
                collections=collections,
                cloud_cover_max=float(ini['INPUTS']['cloud_cover']),
                start_date=iter_start_dt.strftime('%Y-%m-%d'),
                end_date=iter_end_dt.strftime('%Y-%m-%d'),
                geometry=ee.Geometry.Point(openet.core.wrs2.centroids[wrs2_tile]),
                # model_args={},
                filter_args=filter_args,
            )

            logging.debug('  Getting image IDs from EarthEngine')
            image_id_list = utils.get_info(ee.List(model_obj.overpass(
                variables=['ndvi']).aggregate_array('image_id')))

            if not image_id_list:
                logging.info('  No Landsat images in date range, skipping tile')
                continue

            # Get list of existing images for the target tile
            logging.debug('  Getting GEE asset list')
            asset_coll = ee.ImageCollection(scene_coll_id) \
                .filterDate(iter_start_dt.strftime('%Y-%m-%d'),
                            iter_end_dt.strftime('%Y-%m-%d')) \
                .filterMetadata('wrs2_tile', 'equals',
                                wrs2_tile_fmt.format(path, row))
            asset_props = {f'{scene_coll_id}/{x["properties"]["system:index"]}':
                               x['properties']
                           for x in utils.get_info(asset_coll)['features']}
            # asset_props = {x['id']: x['properties']
            #                for x in assets_info['features']}

            # # Get list of band types for checking to see if any bands are floats
            # asset_types = {
            #     f['id']: {b['id']: b['data_type']['precision'] for b in
            #               f['bands']}
            #     for f in assets_info['features']}

            # Sort image ID list by date
            image_id_list = sorted(
                image_id_list, key=lambda k: k.split('/')[-1].split('_')[-1],
                reverse=reverse_flag)
            # pprint.pprint(image_id_list)
            # input('ENTER')

            # Process each image in the collection by date
            # image_id is the full Earth Engine ID to the asset
            for image_id in image_id_list:
                logging.info('  {}'.format(image_id))
                coll_id, scene_id = image_id.rsplit('/', 1)
                l, p, r, year, month, day = parse_landsat_id(scene_id)
                image_dt = datetime.datetime.strptime(
                    '{:04d}{:02d}{:02d}'.format(year, month, day), '%Y%m%d')
                image_date = image_dt.strftime('%Y-%m-%d')
                next_date = (image_dt + datetime.timedelta(days=1)).strftime('%Y-%m-%d')
                logging.debug('    Date: {}'.format(image_date))
                # logging.debug('    DOY: {}'.format(doy))

                export_id = export_id_fmt.format(
                    model=ini['INPUTS']['et_model'].lower(),
                    index=image_id.lower().replace('/', '_'))
                export_id = export_id.replace('-', '')
                export_id += export_id_name
                logging.debug('    Export ID:  {}'.format(export_id))

                asset_id = '{}/{}'.format(scene_coll_id, scene_id.lower())
                logging.debug('    Collection: {}'.format(
                    os.path.dirname(asset_id)))
                logging.debug('    Image ID:   {}'.format(
                    os.path.basename(asset_id)))

                if update_flag:
                    def version_number(version_str):
                        return list(map(int, version_str.split('.')))

                    if export_id in tasks.keys():
                        logging.info('    Task already submitted, skipping')
                        continue
                    # In update mode only overwrite if the version is old
                    if asset_props and asset_id in asset_props.keys():
                        model_ver = version_number(openet.sharpen.__version__)
                        asset_ver = version_number(
                            asset_props[asset_id]['model_version'])
                        # asset_flt = [
                        #     t == 'float' for b, t in asset_types.items()
                        #     if b in ['et', 'et_reference']]

                        if asset_ver < model_ver:
                            logging.info('    Existing asset model version is old, '
                                         'removing')
                            logging.debug(f'    asset: {asset_ver}\n'
                                          f'    model: {model_ver}')
                            try:
                                ee.data.deleteAsset(asset_id)
                            except:
                                logging.info('    Error removing asset, skipping')
                                continue
                        elif ((('T1_RT_TOA' in asset_props[asset_id]['coll_id']) and
                               ('T1_RT_TOA' not in image_id)) or
                              (('T1_RT' in asset_props[asset_id]['coll_id']) and
                               ('T1_RT' not in image_id))):
                            logging.info(
                                '    Existing asset is from realtime Landsat '
                                'collection, removing')
                            try:
                                ee.data.deleteAsset(asset_id)
                            except:
                                logging.info('    Error removing asset, skipping')
                                continue
                        # elif (version_number(asset_props[asset_id]['tool_version']) <
                        #       version_number(TOOL_VERSION)):
                        #     logging.info('    Asset tool version is old, removing')
                        #     try:
                        #         ee.data.deleteAsset(asset_id)
                        #     except:
                        #         logging.info('    Error removing asset, skipping')
                        #         continue
                        # elif any(asset_flt):
                        #     logging.info(
                        #         '    Asset ET types are float, removing')
                        #     ee.data.deleteAsset(asset_id)
                        # elif 'tool_version' not in asset_props[asset_id].keys():
                        #     logging.info('    TOOL_VERSION property was not set, removing')
                        #     ee.data.deleteAsset(asset_id)

                        # elif asset_props[asset_id]['images'] == '':
                        #     logging.info('    Images property was not set, removing')
                        #     input('ENTER')
                        #     ee.data.deleteAsset(asset_id)
                        else:
                            logging.info('    Asset is up to date, skipping')
                            continue
                elif overwrite_flag:
                    if export_id in tasks.keys():
                        logging.info('    Task already submitted, cancelling')
                        ee.data.cancelTask(tasks[export_id]['id'])
                        # ee.data.cancelOperation(tasks[export_id]['id'])
                    # This is intentionally not an "elif" so that a task can be
                    # cancelled and an existing image/file/asset can be removed
                    if asset_props and asset_id in asset_props.keys():
                        logging.info('    Asset already exists, removing')
                        ee.data.deleteAsset(asset_id)
                else:
                    if export_id in tasks.keys():
                        logging.info('    Task already submitted, skipping')
                        continue
                    if asset_props and asset_id in asset_props.keys():
                        logging.info('    Asset already exists, skipping')
                        continue

                # CGM: We could pre-compute (or compute once and then save)
                #   the crs, transform, and shape since they should (will?) be
                #   the same for each wrs2 tile
                output_info = utils.get_info(ee.Image(image_id).select(['B2']))
                transform = '[{}]'.format(
                    ','.join(map(str, output_info['bands'][0]['crs_transform'])))



                # TODO: Module should handle the band renaming and scaling
                # Copied from PTJPL Image.from_landsat_c1_sr()
                landsat_img = ee.Image(image_id)
                input_bands = ee.Dictionary({
                    'LANDSAT_5': ['B1', 'B2', 'B3', 'B4', 'B5', 'B7', 'B6', 'pixel_qa'],
                    'LANDSAT_7': ['B1', 'B2', 'B3', 'B4', 'B5', 'B7', 'B6', 'pixel_qa'],
                    'LANDSAT_8': ['B2', 'B3', 'B4', 'B5', 'B6', 'B7', 'B10', 'pixel_qa']})
                output_bands = ['blue', 'green', 'red', 'nir', 'swir1', 'swir2', 'tir',
                                'pixel_qa']
                spacecraft_id = ee.String(landsat_img.get('SATELLITE'))
                prep_img = landsat_img \
                    .select(input_bands.get(spacecraft_id), output_bands) \
                    .multiply([0.0001, 0.0001, 0.0001, 0.0001, 0.0001, 0.0001, 0.1, 1]) \
                    .set({'system:index': landsat_img.get('system:index'),
                          'system:time_start': landsat_img.get('system:time_start'),
                          'system:id': landsat_img.get('system:id'),
                          'SATELLITE': spacecraft_id,
                         })

                # Compute the sharpened thermal image
                output_img = openet.sharpen.thermal.landsat(prep_img) \
                    .select(['tir_sharpened'], ['tir'])




                # CGM - We will need to think this through a little bit more
                #   Should any scale factor be allowed?  Do we need the clamping?
                #   Should/could we support separate scale factors for each band?
                if scale_factor > 1:
                    if output_type == 'int16':
                        output_img = output_img.multiply(scale_factor).round()\
                            .clamp(-32768, 32767).int16()
                    elif output_type == 'uint16':
                        output_img = output_img.multiply(scale_factor).round()\
                            .clamp(0, 65536).uint16()
                    elif output_type == 'int8':
                        output_img = output_img.multiply(scale_factor).round()\
                            .clamp(-128, 127).int8()
                    elif output_type == 'uint8':
                        output_img = output_img.multiply(scale_factor).round()\
                            .clamp(0, 255).uint8()
                    else:
                        output_img = output_img.multiply(scale_factor)

                if clip_ocean_flag:
                    output_img = output_img\
                        .updateMask(ee.Image('projects/openet/ocean_mask'))

                properties = {
                    # Custom properties
                    'coll_id': coll_id,
                    'core_version': openet.core.__version__,
                    'date_ingested': datetime.datetime.today().strftime('%Y-%m-%d'),
                    'image_id': image_id,
                    'model_name': model_name,
                    'model_version': openet.sharpen.__version__,
                    'scale_factor': 1.0 / scale_factor,
                    'scene_id': scene_id,
                    # CGM - Is this separate property still needed?
                    # Having it named separately from model_version might make
                    #   it easier to pass through to other models/calculations
                    'sharpen_version': openet.sharpen.__version__,
                    # CGM - Tracking the SIMS version since it was used to build
                    #   the image collection
                    'sims_version': model.__version__,
                    'tool_name': TOOL_NAME,
                    'tool_version': TOOL_VERSION,
                    'wrs2_path': p,
                    'wrs2_row': r,
                    'wrs2_tile': wrs2_tile_fmt.format(p, r),
                    # Source properties
                    'CLOUD_COVER': output_info['properties']['CLOUD_COVER'],
                    'CLOUD_COVER_LAND': output_info['properties']['CLOUD_COVER_LAND'],
                    'system:time_start': output_info['properties']['system:time_start'],
                }
                output_img = output_img.set(properties)

                # Copy metadata properties from original output_img
                # Explicit casting since copyProperties returns an element obj
                # output_img = ee.Image(output_img.copyProperties(landsat_img))

                # CGM - Why am I not using the utils.ee_task_start()?
                # Build export tasks
                max_retries = 4
                task = None
                for i in range(1, max_retries):
                    try:
                        task = ee.batch.Export.image.toAsset(
                            output_img,
                            description=export_id,
                            assetId=asset_id,
                            dimensions=output_info['bands'][0]['dimensions'],
                            crs=output_info['bands'][0]['crs'],
                            crsTransform=transform,
                            maxPixels=int(1E10),
                            # pyramidingPolicy='mean',
                        )
                    # except ee.ee_exception.EEException as e:
                    except Exception as e:
                        if ('Earth Engine memory capacity exceeded' in str(e) or
                                'Earth Engine capacity exceeded' in str(e)):
                            logging.info('    Rebuilding task ({}/{})'.format(
                                i, max_retries))
                            logging.debug('    {}'.format(e))
                            time.sleep(i ** 2)
                        else:
                            logging.warning('Unhandled exception\n{}'.format(e))
                            break
                            raise e

                if not task:
                    logging.warning('    Export task was not built, skipping')
                    continue

                logging.info('    Starting export task')
                for i in range(1, max_retries):
                    try:
                        task.start()
                        break
                    except Exception as e:
                        logging.info('    Resending query ({}/{})'.format(
                            i, max_retries))
                        logging.debug('    {}'.format(e))
                        time.sleep(i ** 2)
                # # Not using ee_task_start since it doesn't return the task object
                # utils.ee_task_start(task)

                # Write the export task info the openet-dri project datastore
                if log_tasks:
                    logging.debug('    Writing datastore entity')
                    try:
                        task_obj = datastore.Entity(key=datastore_client.key(
                            'Task', task.status()['id']),
                            exclude_from_indexes=['properties'])
                        for k, v in task.status().items():
                            task_obj[k] = v
                        # task_obj['date'] = datetime.datetime.today() \
                        #     .strftime('%Y-%m-%d')
                        task_obj['index'] = properties.pop('wrs2_tile')
                        # task_obj['wrs2_tile'] = properties.pop('wrs2_tile')
                        task_obj['model_name'] = properties.pop('model_name')
                        # task_obj['model_version'] = properties.pop('model_version')
                        task_obj['runtime'] = 0
                        task_obj['start_timestamp_ms'] = 0
                        task_obj['tool_name'] = properties.pop('tool_name')
                        task_obj['properties'] = json.dumps(properties)
                        datastore_client.put(task_obj)
                    except Exception as e:
                        # CGM - The message/handling will probably need to be updated
                        #   We may want separate try/excepts on the create and the put
                        logging.warning('\nDatastore entity was not written')
                        logging.warning('{}\n'.format(e))

                # Pause before starting the next export task
                utils.delay_task(delay_time, max_ready)

                logging.debug('')


def mgrs_export_tiles(study_area_coll_id, mgrs_coll_id,
                      study_area_property=None, study_area_features=[],
                      mgrs_tiles=[], mgrs_skip_list=[],
                      utm_zones=[], wrs2_tiles=[],
                      mgrs_property='mgrs', utm_property='utm',
                      wrs2_property='wrs2'):
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
    wrs2_tiles : list, optional
        User defined WRS2 tile subset.
    mgrs_property : str, optional
        MGRS property in the MGRS feature collection (the default is 'mgrs').
    utm_property : str, optional
        UTM zone property in the MGRS feature collection (the default is 'wrs2').
    wrs2_property : str, optional
        WRS2 property in the MGRS feature collection (the default is 'wrs2').

    Returns
    ------
    list of dicts: export information

    """
    # Build and filter the study area feature collection
    logging.debug('Building study area collection')
    logging.debug('  {}'.format(study_area_coll_id))
    study_area_coll = ee.FeatureCollection(study_area_coll_id)
    if (study_area_property == 'STUSPS' and
            'CONUS' in [x.upper() for x in study_area_features]):
        # Exclude AK, HI, AS, GU, PR, MP, VI, (but keep DC)
        study_area_features = [
            'AL', 'AR', 'AZ', 'CA', 'CO', 'CT', 'DC', 'DE', 'FL', 'GA',
            'IA', 'ID', 'IL', 'IN', 'KS', 'KY', 'LA', 'MA', 'MD', 'ME',
            'MI', 'MN', 'MO', 'MS', 'MT', 'NC', 'ND', 'NE', 'NH', 'NJ',
            'NM', 'NV', 'NY', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC', 'SD',
            'TN', 'TX', 'UT', 'VA', 'VT', 'WA', 'WI', 'WV', 'WY']
    # elif (study_area_property == 'STUSPS' and
    #         'WESTERN11' in [x.upper() for x in study_area_features]):
    #     study_area_features = [
    #         'AZ', 'CA', 'CO', 'ID', 'MT', 'NM', 'NV', 'OR', 'UT', 'WA', 'WY']
    study_area_features = sorted(list(set(study_area_features)))

    if study_area_property and study_area_features:
        logging.debug('  Filtering study area collection')
        logging.debug('  Property: {}'.format(study_area_property))
        logging.debug('  Features: {}'.format(','.join(study_area_features)))
        study_area_coll = study_area_coll.filter(
            ee.Filter.inList(study_area_property, study_area_features))

    logging.debug('Building MGRS tile list')
    tiles_coll = ee.FeatureCollection(mgrs_coll_id) \
        .filterBounds(study_area_coll.geometry())

    # Filter collection by user defined lists
    if utm_zones:
        logging.debug('  Filter user UTM Zones:    {}'.format(utm_zones))
        tiles_coll = tiles_coll.filter(ee.Filter.inList(utm_property, utm_zones))
    if mgrs_skip_list:
        logging.debug('  Filter MGRS skip list:    {}'.format(mgrs_skip_list))
        tiles_coll = tiles_coll.filter(
            ee.Filter.inList(mgrs_property, mgrs_skip_list).Not())
    if mgrs_tiles:
        logging.debug('  Filter MGRS tiles/zones:  {}'.format(mgrs_tiles))
        # Allow MGRS tiles to be subsets of the full tile code
        #   i.e. mgrs_tiles = 10TE, 10TF
        mgrs_filters = [
            ee.Filter.stringStartsWith(mgrs_property, mgrs_id.upper())
            for mgrs_id in mgrs_tiles]
        tiles_coll = tiles_coll.filter(ee.call('Filter.or', mgrs_filters))

    def drop_geometry(ftr):
        return ee.Feature(None).copyProperties(ftr)

    logging.debug('  Requesting tile/zone info')
    tiles_info = utils.get_info(tiles_coll.map(drop_geometry))

    # Constructed as a list of dicts to mimic other interpolation/export tools
    tiles_list = []
    for tile_ftr in tiles_info['features']:
        tiles_list.append({
            'index': tile_ftr['properties']['mgrs'].upper(),
            'wrs2_tiles': sorted(utils.wrs2_str_2_set(
                tile_ftr['properties'][wrs2_property])),
        })

    # Apply the user defined WRS2 tile list
    if wrs2_tiles:
        logging.debug('  Filter WRS2 tiles: {}'.format(wrs2_tiles))
        for tile in tiles_list:
            tile['wrs2_tiles'] = sorted(list(
                set(tile['wrs2_tiles']) & set(wrs2_tiles)))

    # Only return export tiles that have intersecting WRS2 tiles
    export_list = [
        tile for tile in sorted(tiles_list, key=lambda k: k['index'])
        if tile['wrs2_tiles']]

    return export_list


def parse_landsat_id(system_index):
    """Return the components of an EE Landsat Collection 1 system:index

    Parameters
    ----------
    system_index : str

    Notes
    -----
    LT05_PPPRRR_YYYYMMDD

    """
    sensor = system_index[0:4]
    path = int(system_index[5:8])
    row = int(system_index[8:11])
    year = int(system_index[12:16])
    month = int(system_index[16:18])
    day = int(system_index[18:20])
    return sensor, path, row, year, month, day


def arg_parse():
    """"""
    parser = argparse.ArgumentParser(
        description='Export Landsat sharpened thermal images',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument(
        '-i', '--ini', type=utils.arg_valid_file,
        help='Input file', metavar='FILE')
    parser.add_argument(
        '--debug', default=logging.INFO, const=logging.DEBUG,
        help='Debug level logging', action='store_const', dest='loglevel')
    parser.add_argument(
        '--delay', default=0, type=float,
        help='Delay (in seconds) between each export tasks')
    parser.add_argument(
        '--key', type=utils.arg_valid_file, metavar='FILE',
        help='Earth Engine service account JSON key file')
    parser.add_argument(
        '--overwrite', default=False, action='store_true',
        help='Force overwrite of existing files')
    parser.add_argument(
        '--ready', default=-1, type=int,
        help='Maximum number of queued READY tasks')
    parser.add_argument(
        '--recent', default=0, type=int,
        help='Number of days to process before current date '
             '(ignore INI start_date and end_date')
    parser.add_argument(
        '--reverse', default=False, action='store_true',
        help='Process WRS2 tiles in reverse order')
    parser.add_argument(
        '--tiles', default='', nargs='+',
        help='Comma/space separated list of tiles to process')
    parser.add_argument(
        '--update', default=False, action='store_true',
        help='Update images with older model version numbers')
    parser.add_argument(
        '--start', type=utils.arg_valid_date, metavar='DATE', default=None,
        help='Start date (format YYYY-MM-DD)')
    parser.add_argument(
        '--end', type=utils.arg_valid_date, metavar='DATE', default=None,
        help='End date (format YYYY-MM-DD)')
    args = parser.parse_args()

    return args


if __name__ == '__main__':
    args = arg_parse()
    logging.basicConfig(level=args.loglevel, format='%(message)s')
    logging.getLogger('googleapiclient').setLevel(logging.ERROR)

    main(ini_path=args.ini, overwrite_flag=args.overwrite,
         delay_time=args.delay, gee_key_file=args.key, max_ready=args.ready,
         reverse_flag=args.reverse, tiles=args.tiles, update_flag=args.update,
         recent_days=args.recent, start_dt=args.start, end_dt=args.end,
    )
