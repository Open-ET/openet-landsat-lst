import argparse
from builtins import input
from collections import defaultdict
import configparser
import datetime
import json
import logging
import os
import pprint
import re
import time

import ee

import openet.sharpen
import openet.core
import openet.core.utils as utils
# CGM - Using SSEBop for building image ID list (for now)
import openet.ssebop as model
# import openet.disalexi as model

# try:
#     from importlib import metadata
# except ImportError:  # for Python<3.8
#     import importlib_metadata as metadata

TOOL_NAME = 'tir_image_wrs2_export'
# TOOL_NAME = os.path.basename(__file__)
TOOL_VERSION = '0.3.0'

logging.getLogger("earthengine-api").setLevel(logging.INFO)
logging.getLogger("googleapiclient").setLevel(logging.INFO)
logging.getLogger('requests').setLevel(logging.INFO)
logging.getLogger("urllib3").setLevel(logging.INFO)


def main(ini_path=None, overwrite_flag=False,
         delay_time=0, ready_task_max=-1, gee_key_file=None,
         reverse_flag=False, tiles=None, update_flag=False,
         recent_days=None, start_dt=None, end_dt=None,
         log_tasks=False,
         ):
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
    ready_task_max: int, optional
        Maximum number of queued "READY" tasks.  The default is -1 which is
        implies no limit to the number of tasks that will be submitted.
    reverse_flag : bool, optional
        If True, process WRS2 tiles in reverse order (the default is False).
    tiles : str, optional
        Comma separated UTM zones or MGRS tiles to process (the default is None).
    update_flag : bool, optional
        If True, only overwrite scenes with an older model version.
    recent_days : int, str, optional
        Limit start/end date range to this many days before the current date.
        The default is None which will use the INI start/end date directly.
    start_dt : datetime, optional
        Override the start date in the INI file
        (the default is None which will use the INI start date).
    end_dt : datetime, optional
        Override the (inclusive) end date in the INI file
        (the default is None which will use the INI end date).
    log_tasks : bool, optional
        If True, log task information to the datastore (the default is True).

    Returns
    -------
    None

    """
    logging.info('\nExport Landsat sharpened thermal images')

    wrs2_tile_fmt = 'p{:03d}r{:03d}'
    wrs2_tile_re = re.compile('p?(\d{1,3})r?(\d{1,3})')

    # List of path/rows to skip
    wrs2_skip_list = [
        'p049r026',  # Vancouver Island, Canada
        # 'p047r031',  # North California coast
        'p042r037',  # San Nicholas Island, California
        # 'p041r037',  # South California coast
        'p040r038', 'p039r038', 'p038r038',  # Mexico (by California)
        'p037r039', 'p036r039', 'p035r039',  # Mexico (by Arizona)
        'p034r039', 'p033r039',  # Mexico (by New Mexico)
        'p032r040',  # Mexico (West Texas)
        'p029r041', 'p028r042', 'p027r043', 'p026r043',  # Mexico (South Texas)
        'p019r040', 'p018r040',  # West Florida coast
        'p016r043', 'p015r043',  # South Florida coast
        'p014r041', 'p014r042', 'p014r043',  # East Florida coast
        'p013r035', 'p013r036',  # North Carolina Outer Banks
        'p013r026', 'p012r026',  # Canada (by Maine)
        'p011r032', # Rhode Island coast
    ]
    wrs2_path_skip_list = [9, 49]
    wrs2_row_skip_list = [25, 24, 43]
    mgrs_skip_list = []

    export_id_fmt = '{model}_{index}'

    # TODO: Move to INI file
    clip_ocean_flag = True

    # Read config file
    logging.info(f'  {os.path.basename(ini_path)}')
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
    logging.info(f'  ET Model: {model_name}')

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
        scene_coll_id = f'projects/earthengine-legacy/assets/' \
                        f'projects/openet/{model_name.lower()}/landsat/scene'
        # scene_coll_id = f'projects/openet/assets/{model_name.lower()}/landsat/scene'
        logging.debug(f'  scene_coll: not set in INI, defaulting to {scene_coll_id}')
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
        logging.debug(f'  mgrs_tiles: {mgrs_tiles}')
    except KeyError:
        mgrs_tiles = []
        logging.debug('  mgrs_tiles: not set in INI, defaulting to []')
    except Exception as e:
        raise e

    try:
        utm_zones = str(ini['EXPORT']['utm_zones'])
        utm_zones = sorted([int(x.strip()) for x in utm_zones.split(',')])
        logging.debug(f'  utm_zones: {utm_zones}')
    except KeyError:
        utm_zones = []
        logging.debug('  utm_zones: not set in INI, defaulting to []')
    except Exception as e:
        raise e

    try:
        if 'data_type' in ini['EXPORT'].keys():
            output_dtype = str(ini['EXPORT']['data_type'])
        elif 'output_type' in ini['EXPORT'].keys():
            output_dtype = str(ini['EXPORT']['output_type'])
        else:
            raise KeyError
    except KeyError:
        output_dtype = 'float'
        # output_dtype = 'int16'
        logging.debug(f'  data_type: not set in INI, defaulting to {output_dtype}')
    except Exception as e:
        raise e

    try:
        scale_factor = int(ini['EXPORT']['scale_factor'])
    except KeyError:
        scale_factor = 1
        # scale_factor = 10000
        logging.debug(f'  scale_factor: not set in INI, defaulting to {scale_factor}')
    except Exception as e:
        raise e

    try:
        export_id_name = '_' + str(ini['EXPORT']['export_id_name'])
    except KeyError:
        export_id_name = ''
        logging.debug('  export_id_name: not set in INI, defaulting to ""')
    except Exception as e:
        raise e

    try:
        destination = str(ini['EXPORT']['destination']).upper()
    except KeyError:
        destination = 'ASSET'
        # destination = 'BUCKET'
        logging.debug(f'  destination: not set in INI, defaulting to {destination}')
    except Exception as e:
        raise e

    if destination == 'ASSET':
        properties_json_flag = False
        bucket_project_id = None
        bucket_name = None
    elif destination == 'BUCKET':
        try:
            # TODO: Write code to parse other boolean values
            #   but for now if the flag is not exactly "True", set to False
            properties_json_flag = ini['EXPORT']['properties_json_flag'].strip().capitalize()
            properties_json_flag = True if properties_json_flag in ['True'] else False
        except KeyError:
            properties_json_flag = True
            logging.debug(f'  properties_json_flag: not set in INI, defaulting to '
                          f'{properties_json_flag}')
        except Exception as e:
            raise e

        try:
            bucket_project_id = str(ini['EXPORT']['bucket_project_id'])
        except KeyError:
            raise ValueError('"project_id" parameter must be set for COG exports')
        except Exception as e:
            raise e

        try:
            bucket_name = str(ini['EXPORT']['bucket_name'])
        except KeyError:
            raise ValueError('"bucket_name" parameter must be set for COG exports')
        except Exception as e:
            raise e

        # try:
        #     export_bucket_name = str(ini['EXPORT']['export_bucket'])
        # except KeyError:
        #     raise ValueError('"export_bucket" parameter must be set for COG exports')
        # except Exception as e:
        #     raise e
        #
        # try:
        #     asset_bucket_name = str(ini['EXPORT']['assets_bucket'])
        # except KeyError:
        #     raise ValueError('"assets_bucket" parameter must be set for COG exports')
        # except Exception as e:
        #     raise e
    # elif destination == 'GDRIVE':
    else:
        raise ValueError(f'Unsupported EXPORT "destination" parameter: {destination}')


    # If the user set the tiles argument, use these instead of the INI values
    if tiles:
        logging.info('\nOverriding INI mgrs_tiles and utm_zones parameters')
        logging.info(f'  user tiles: {tiles}')
        mgrs_tiles = sorted([x.strip() for x in tiles.split(',')])
        # mgrs_tiles = sorted([y.strip() for x in tiles for y in x.split(',')])
        mgrs_tiles = [x.upper() for x in mgrs_tiles if x]
        logging.info(f'  mgrs_tiles: {", ".join(mgrs_tiles)}')
        utm_zones = sorted(list(set([int(x[:2]) for x in mgrs_tiles])))
        logging.info(f'  utm_zones:  {", ".join(map(str, utm_zones))}')

    today_dt = datetime.datetime.now()
    today_dt = today_dt.replace(hour=0, minute=0, second=0, microsecond=0)
    if start_dt and end_dt:
        # Attempt to use the function start/end dates
        logging.info('\nOverriding INI "start_date" and "end_date" parameters')
        logging.info('  Custom date range')
        start_date = start_dt.strftime('%Y-%m-%d')
        end_date = end_dt.strftime('%Y-%m-%d')
    elif recent_days:
        logging.debug('\nOverriding INI "start_date" and "end_date" parameters')
        logging.debug(f'  Recent days: {recent_days}')
        recent_days = list(sorted(utils.parse_int_set(recent_days)))
        # Assume that a single day value should actually be a range?
        if len(recent_days) == 1:
            recent_days = list(range(1, recent_days[0]))
        end_dt = today_dt - datetime.timedelta(days=recent_days[0])
        start_dt = today_dt - datetime.timedelta(days=recent_days[-1])
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
    logging.info(f'  Start: {start_date}')
    logging.info(f'  End:   {end_date}')

    # TODO: Add a few more checks on the dates
    if end_dt < start_dt:
        raise ValueError('end date can not be before start date')

    logging.debug('\nFilter date range')
    filter_end_dt = end_dt + datetime.timedelta(days=1)
    filter_end_date = filter_end_dt.strftime("%Y-%m-%d")
    logging.debug(f'  Start: {start_date}')
    logging.debug(f'  End:   {filter_end_date}')


    # Setup datastore task logging
    if log_tasks:
        # Assume function is being run deployed as a cloud function
        #   and use the defult credentials (should be the SA credentials)
        logging.debug('\nInitializing task datastore client')
        try:
            from google.cloud import datastore
            datastore_client = datastore.Client(project='openet-dri')
        except Exception as e:
            logging.info('  Task logging disabled, error setting up datastore client')
            log_tasks = False


    # Initialize Earth Engine
    if gee_key_file:
        logging.info(f'\nInitializing GEE using user key file: {gee_key_file}')
        try:
            ee.Initialize(ee.ServiceAccountCredentials('_', key_file=gee_key_file))
        except ee.ee_exception.EEException:
            logging.warning('Unable to initialize GEE using user key file')
            return False
    elif 'FUNCTION_REGION' in os.environ:
        # Assume code is deployed to a cloud function
        logging.debug(f'\nInitializing GEE using application default credentials')
        import google.auth
        credentials, project_id = google.auth.default(
            default_scopes=['https://www.googleapis.com/auth/earthengine']
        )
        ee.Initialize(credentials)
    # elif 'GOOGLE_APPLICATION_CREDENTIALS' in os.environ:
    #     logging.info(f'\nInitializing GEE using GOOGLE_APPLICATION_CREDENTIALS key')
    #     try:
    #         ee.Initialize(ee.ServiceAccountCredentials(
    #             "_", key_file=os.environ.get('GOOGLE_APPLICATION_CREDENTIALS')))
    #     except Exception as e:
    #         logging.warning('Unable to initialize GEE using '
    #                         'GOOGLE_APPLICATION_CREDENTIALS key file')
    #         return False
    else:
        logging.info('\nInitializing Earth Engine using user credentials')
        ee.Initialize()


    # Build output collection and folder if necessary
    logging.debug(f'\nExport Collection: {scene_coll_id}')
    if not ee.data.getInfo(scene_coll_id.rsplit('/', 1)[0]):
        logging.info(f'\nFolder does not exist and will be built'
                      f'\n  {scene_coll_id.rsplit("/", 1)[0]}')
        input('Press ENTER to continue')
        ee.data.createAsset({'type': 'FOLDER'},
                            scene_coll_id.rsplit('/', 1)[0])
    if not ee.data.getInfo(scene_coll_id):
        logging.info(f'\nExport collection does not exist and will be built'
                     f'\n  {scene_coll_id}')
        input('Press ENTER to continue')
        ee.data.createAsset({'type': 'IMAGE_COLLECTION'}, scene_coll_id)


    # Get current running tasks
    if ready_task_max == -9999:
        # CGM - Getting the task list can take awhile so set ready tasks to
        #   -9999 to skip requesting it.  Only run this if you are going to
        #   manually avoid running existing tasks.
        # TODO: Check if this should disable delay_task() or set the
        #   ready_task_max to a large value
        tasks = {}
        ready_task_count = 0
    else:
        tasks = utils.get_ee_tasks()
        if logging.getLogger().getEffectiveLevel() == logging.DEBUG:
            utils.print_ee_tasks(tasks)
            input('ENTER')
        running_task_count = sum([1 for v in tasks.values() if v['state'] == 'RUNNING'])
        ready_task_count = sum([1 for v in tasks.values() if v['state'] == 'READY'])
        logging.info(f'  Running Tasks: {running_task_count}')
        logging.info(f'  Ready Tasks:   {ready_task_count}')

        # Hold the job here if the ready task count is already over the max
        ready_task_count = delay_task(
            delay_time=0, task_max=ready_task_max, task_count=ready_task_count
        )


    # Check the storage bucket
    # CGM - We don't really need to connect to the bucket here
    #   but it maybe useful for checking that the bucket exists
    bucket = None
    bucket_folder = None
    if destination == 'BUCKET':
        logging.info(f'\nChecking bucket files')
        from google.cloud import storage
        storage_client = storage.Client(bucket_project_id)
        # CGM - Is there a difference between .bucket() and .get_bucket()?
        bucket = storage_client.get_bucket(bucket_name)
        # bucket = storage_client.bucket(bucket_name)
        # TODO: Add a check to make sure the bucket folder looks like
        #   model/landsat/c02, or maybe just starts with the model?
        #   May need to support numbers & hyphens in the project name
        #   projects/earthengine-legacy/assets/projects/openet/geesebal/landsat/c02
        #   projects/openet/geesebal/landsat/c02
        #   projects/openet/assets/geesebal/landsat/c02
        bucket_folder = re.sub(
            'projects/[a-zA-Z_]+/(assets/)?', '',
            scene_coll_id.replace('projects/earthengine-legacy/assets/', '')
        )
        logging.debug(f'  {bucket_folder}')
        # # CGM - Not checking bucket files any more
        # bucket_files = {x.name for x in bucket.list_blobs(prefix=bucket_folder)}


    # Get list of MGRS tiles/zones that intersect the study area
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
    processed_image_ids = set()
    # processed_wrs2_tiles = []
    for export_info in sorted(export_list, key=lambda i: i['index'],
                              reverse=reverse_flag):
        logging.info(f'{export_info["index"]}')
        # logging.debug('  {} - {}'.format(
        #     export_info['index'], ', '.join(export_info['wrs2_tiles'])))
        tile_count = len(export_info['wrs2_tiles'])
        tile_list = sorted(export_info['wrs2_tiles'], reverse=not(reverse_flag))
        tile_geom = ee.Geometry.Rectangle(
            export_info['extent'], export_info['crs'], False)

        # Get the available image ID list for the zone
        # Get list of existing image assets and their properties for the zone
        # Process date range by years to avoid requesting more than 3000 images
        logging.debug('  Getting list of available model images and existing assets')
        export_image_id_list = []
        asset_props = {}
        for year_start_dt, year_end_dt in date_range_by_year(
                start_dt, end_dt, exclusive_end_dates=True):
            year_start_date = year_start_dt.strftime("%Y-%m-%d")
            year_end_date = year_end_dt.strftime("%Y-%m-%d")
            logging.debug(f'  {year_start_date} {year_end_date}')

            # Buffer the tile geometry a bit, but filter to the WRS2 tile list below
            # This is needed because the Landsat 7 and 8 footprints are different
            # For tile p027r031, L7 is being included but not L8 in zone 14T,
            # Adding a buffer helps prevent that tile but causes other ones to
            #   have the same problem
            logging.debug('  Getting list of available model images')
            model_obj = model.Collection(
                collections=collections,
                cloud_cover_max=float(ini['INPUTS']['cloud_cover']),
                start_date=year_start_date,
                end_date=year_end_date,
                geometry=tile_geom.buffer(1000),
                # model_args={},
                # filter_args=filter_args,
            )

            year_image_id_list = []
            for i in range(1, 4):
                try:
                    year_image_id_list = model_obj.get_image_ids()
                    break
                except Exception as e:
                    logging.info('  Error requesting image IDs from collection, retrying')
                    time.sleep(i ** 3)
                    continue
                    # # Get the image ID list from an NDVI collection if get_image_ids()
                    # #   doesn't work
                    # logging.info('  Could not get image IDs from collection method')
                    # year_image_id_list = utils.get_info(
                    #     ee.List(model_obj.overpass(variables=['ndvi'])
                    #             .aggregate_array('image_id')),
                    #     max_retries=10
                    # )
            if not year_image_id_list:
                logging.info('  Empty year image ID list, skipping')
                continue

            # Filter to the wrs2_tile list
            # The WRS2 tile filtering should be done in the Collection call above,
            #   but not all of the models support this
            year_image_id_list = [
                x for x in year_image_id_list
                if 'p{}r{}'.format(*re.findall('_(\d{3})(\d{3})_', x)[0]) in tile_list
            ]

            # Filter image_ids that have already been processed as part of a
            #   different MGRS tile (might be faster with sets)
            year_image_id_list = [x for x in year_image_id_list
                                  if x not in processed_image_ids]
            # Keep track of all the image_ids that have been processed
            processed_image_ids.update(year_image_id_list)

            export_image_id_list.extend(year_image_id_list)

             # Get list of existing image assets and their properties
            logging.debug('  Getting list of existing model assets')
            asset_coll = ee.ImageCollection(scene_coll_id) \
                .filterDate(year_start_date, year_end_date) \
                .filter(ee.Filter.inList('wrs2_tile', tile_list)) \
                .filterBounds(tile_geom)
            year_asset_props = {
                f'{scene_coll_id}/{x["properties"]["system:index"]}': x['properties']
                for x in utils.get_info(asset_coll)['features']
            }
            asset_props.update(year_asset_props)

        if not export_image_id_list:
            logging.info('  No Landsat images in date range, skipping zone')
            continue
        export_image_id_list = sorted(
            export_image_id_list, key=lambda k: k.split('/')[-1].split('_')[-1],
            reverse=reverse_flag
        )

        # Group images by wrs2 tile
        image_id_lists = defaultdict(list)
        for image_id in export_image_id_list:
            wrs2_tile = 'p{}r{}'.format(
                *wrs2_tile_re.findall(image_id.split('/')[-1].split('_')[1])[0]
            )
            if wrs2_tile not in tile_list:
                continue
            image_id_lists[wrs2_tile].append(image_id)


        for export_n, wrs2_tile in enumerate(tile_list):
            path, row = map(int, wrs2_tile_re.findall(wrs2_tile)[0])
            # DEADBEEF Tracking processed image_ids instead of processed wrs2_tiles
            # The L7 and L8 footprints are slightly different so processing the
            #   wrs2 tile does not mean that all of the images were processed
            # if wrs2_tile in wrs2_tiles:
            #     logging.debug('{} {} ({}/{}) - already processed'.format(
            #         export_info['index'], wrs2_tile, export_n + 1, tile_count))
            #     continue
            # processed_wrs2_tiles.append(wrs2_tile)
            if wrs2_skip_list and wrs2_tile in wrs2_skip_list:
                logging.debug('{} {} ({}/{}) - in wrs2 skip list'.format(
                    export_info['index'], wrs2_tile, export_n + 1, tile_count))
                continue
            elif wrs2_row_skip_list and row in wrs2_row_skip_list:
                logging.debug('{} {} ({}/{}) - in wrs2 row skip list'.format(
                    export_info['index'], wrs2_tile, export_n + 1, tile_count))
                continue
            elif wrs2_path_skip_list and path in wrs2_path_skip_list:
                logging.debug('{} {} ({}/{}) - in wrs2 path skip list'.format(
                    export_info['index'], wrs2_tile, export_n + 1, tile_count))
                continue
            else:
                logging.debug('{} {} ({}/{})'.format(
                    export_info['index'], wrs2_tile, export_n + 1, tile_count))

            # path, row = map(int, wrs2_tile_re.findall(export_info['index'])[0])
            # logging.info('WRS2 tile: {}  ({}/{})'.format(
            #     export_info['index'], export_n + 1, len(export_list)))
            #
            # logging.debug(f'  Shape:     {export_info["shape"]}')
            # logging.debug(f'  Transform: {export_info["geo_str"]}')
            # logging.debug(f'  Extent:    {export_info["extent"]}')
            # logging.debug(f'  MaxPixels: {export_info["maxpixels"]}')


            # Subset the image ID list to the WRS2 tile
            try:
                image_id_list = image_id_lists[wrs2_tile]
            except KeyError:
                image_id_list = []
            if not image_id_list:
                logging.debug('  No Landsat images in date range, skipping tile')
                continue


            # DEADBEEF - Checking the available images and assets once per
            #   export tile instead of separately per WRS2 tile
            # filter_args = {}
            # for coll_id in collections:
            #     filter_args[coll_id] = [
            #         {'type': 'equals', 'leftField': 'WRS_PATH', 'rightValue': path},
            #         {'type': 'equals', 'leftField': 'WRS_ROW', 'rightValue': row}]
            # # logging.debug(f'  Filter Args: {filter_args}')
            #
            # # Get the full Landsat collection
            # # Collection end date is exclusive
            # model_obj = model.Collection(
            #     collections=collections,
            #     cloud_cover_max=float(ini['INPUTS']['cloud_cover']),
            #     start_date=start_date,
            #     end_date=filter_end_date,
            #     geometry=ee.Geometry.Point(openet.core.wrs2.centroids[wrs2_tile]),
            #     # model_args={},
            #     filter_args=filter_args,
            # )
            #
            # logging.debug('  Getting image IDs from EarthEngine')
            # image_id_list = utils.get_info(ee.List(model_obj.overpass(
            #     variables=['ndvi']).aggregate_array('image_id')))
            #
            # if not image_id_list:
            #     logging.info('  No Landsat images in date range, skipping tile')
            #     continue
            #
            # # Get list of existing images for the target tile
            # logging.debug('  Getting GEE asset list')
            # asset_coll = ee.ImageCollection(scene_coll_id) \
            #     .filterDate(start_date, filter_end_date) \
            #     .filterMetadata('wrs2_tile', 'equals',
            #                     wrs2_tile_fmt.format(path, row))
            # asset_props = {f'{scene_coll_id}/{x["properties"]["system:index"]}':
            #                    x['properties']
            #                for x in utils.get_info(asset_coll)['features']}
            # # asset_props = {x['id']: x['properties']
            # #                for x in assets_info['features']}
            #
            # # # Get list of band types for checking to see if any bands are floats
            # # asset_types = {
            # #     f['id']: {b['id']: b['data_type']['precision'] for b in
            # #               f['bands']}
            # #     for f in assets_info['features']}
            #
            # # Sort image ID list by date
            # image_id_list = sorted(
            #     image_id_list, key=lambda k: k.split('/')[-1].split('_')[-1],
            #     reverse=reverse_flag)
            # # pprint.pprint(image_id_list)
            # # input('ENTER')


            # # CGM - Commenting out since checking the bucket files isn't really
            # #   necessary now that the nodata value can be set correctly
            # # Get list of existing bucket files
            # bucket_files = set()
            # if destination == 'BUCKET':
            #     logging.debug(f'  Checking bucket files')
            #     # CGM - This is a quick hack to try and break up the filtering a little
            #     #   I thought about just iterating on collections but I wasn't sure that
            #     #   would work if we ever added other sensors or the realtime collections
            #     # This works for now since I know that this tool operates on scenes and
            #     #   there are only images with this format in the model/landsat/c02 folders
            #     for landsat in ['lt05', 'le07', 'lc08', 'lc09']:
            #         if landsat.upper() not in ','.join(collections):
            #             continue
            #         bucket_prefix = f'{bucket_folder}/{landsat}_{path:03d}{row:03d}'
            #         logging.debug(f'    {bucket_prefix}')
            #         landsat_files = {x.name for x in bucket.list_blobs(prefix=bucket_prefix)}
            #         bucket_files.update(landsat_files)


            # Process each image in the collection by date
            # image_id is the full Earth Engine ID to the asset
            for image_id in image_id_list:
                coll_id, scene_id = image_id.rsplit('/', 1)
                l, p, r, year, month, day = parse_landsat_id(scene_id)
                image_dt = datetime.datetime.strptime(
                    '{:04d}{:02d}{:02d}'.format(year, month, day), '%Y%m%d')
                image_date = image_dt.strftime('%Y-%m-%d')
                # next_date = (image_dt + datetime.timedelta(days=1)).strftime('%Y-%m-%d')

                export_id = export_id_fmt.format(
                    model=ini['INPUTS']['et_model'].lower(),
                    index=image_id.lower().replace('/', '_')
                )
                export_id = export_id.replace('-', '')
                export_id += export_id_name
                if destination == 'BUCKET':
                    export_id += '_cog'

                asset_id = f'{scene_coll_id}/{scene_id.lower()}'

                bucket_img = f'{bucket_folder}/{scene_id.lower()}.tif'
                bucket_json = f'{bucket_folder}/{scene_id.lower()}_properties.json'
                # logging.debug(f'  Bucket path:   {bucket_img}')
                # logging.debug(f'  Properties JSON: {bucket_json}')

                if update_flag:
                    if export_id in tasks.keys():
                        logging.debug(f'  {scene_id} - Task already submitted, skipping')
                        continue
                    elif asset_props and asset_id in asset_props.keys():
                        # In update mode only overwrite if the version is old
                        model_ver = version_number(openet.sharpen.__version__)
                        asset_ver = version_number(asset_props[asset_id]['model_version'])
                        # asset_flt = [
                        #     t == 'float' for b, t in asset_types.items()
                        #     if b in ['et', 'et_reference']]

                        if asset_ver < model_ver:
                            logging.info(f'  {scene_id} - Existing asset model version is old, removing')
                            logging.debug(f'  asset: {asset_ver}\n'
                                          f'  model: {model_ver}')
                            try:
                                # CGM - For all of these we are assuming that COG backed assets
                                #   will only be written to collections of COG backed assets
                                #   and native assets to native asset image collections
                                if destination == 'BUCKET':
                                    delete_cog_asset(asset_id, bucket, bucket_img, bucket_json)
                                else:
                                    ee.data.deleteAsset(asset_id)
                            except:
                                logging.info(f'  {scene_id} - Error removing asset, skipping')
                                continue
                        elif ((('T1_RT_TOA' in asset_props[asset_id]['coll_id']) and
                               ('T1_RT_TOA' not in image_id)) or
                              (('T1_RT' in asset_props[asset_id]['coll_id']) and
                               ('T1_RT' not in image_id))):
                            logging.info(
                                '  Existing asset is from realtime Landsat '
                                'collection, removing')
                            try:
                                if destination == 'BUCKET':
                                    delete_cog_asset(asset_id, bucket, bucket_img, bucket_json)
                                else:
                                    ee.data.deleteAsset(asset_id)
                            except:
                                logging.info(f'  {scene_id} - Error removing asset, skipping')
                                continue
                        # elif (version_number(asset_props[asset_id]['tool_version']) <
                        #       version_number(TOOL_VERSION)):
                        #     logging.info('  Asset tool version is old, removing')
                        #     try:
                        #         ee.data.deleteAsset(asset_id)
                        #     except:
                        #         logging.info('  Error removing asset, skipping')
                        #         continue
                        # elif any(asset_flt):
                        #     logging.info(
                        #         '  Asset ET types are float, removing')
                        #     ee.data.deleteAsset(asset_id)
                        # elif 'tool_version' not in asset_props[asset_id].keys():
                        #     logging.info('  TOOL_VERSION property was not set, removing')
                        #     ee.data.deleteAsset(asset_id)
                        # elif asset_props[asset_id]['images'] == '':
                        #     logging.info('  Images property was not set, removing')
                        #     input('ENTER')
                        #     ee.data.deleteAsset(asset_id)
                        else:
                            logging.debug(f'  {scene_id} - Asset is up to date, skipping')
                            continue
                elif overwrite_flag:
                    if export_id in tasks.keys():
                        logging.info(f'  {scene_id} - Task already submitted, cancelling')
                        ee.data.cancelTask(tasks[export_id]['id'])
                        # ee.data.cancelOperation(tasks[export_id]['id'])
                    # This is intentionally not an "elif" so that a task can be
                    # cancelled and an existing image/file/asset can be removed
                    if asset_props and asset_id in asset_props.keys():
                        logging.info(f'  {scene_id} - Asset already exists, removing')
                        if destination == 'BUCKET':
                            delete_cog_asset(asset_id, bucket, bucket_img, bucket_json)
                        else:
                            ee.data.deleteAsset(asset_id)
                else:
                    if export_id in tasks.keys():
                        logging.debug(f'  {scene_id} - Task already submitted, skipping')
                        continue
                    elif asset_props and asset_id in asset_props.keys():
                        logging.debug(f'  {scene_id} - Asset already exists, skipping')
                        continue
                    # # CGM - Commenting out since checking the bucket files isn't really
                    # #   necessary now that the nodata value can be set correctly
                    # # CGM - We probably don't need the extra check on bucket_files
                    # elif destination == 'BUCKET' and bucket_files and bucket_img in bucket_files:
                    #     # if overwrite_flag:
                    #     logging.debug(f'  {scene_id} - Image is already in bucket, skipping')
                    #     continue

                logging.debug(f'  Source: {image_id}')
                # logging.debug(f'  Date: {image_date}')
                # logging.debug(f'  DOY:  {doy}')
                logging.debug(f'  Export ID:  {export_id}')
                logging.debug(f'  Collection: {os.path.dirname(asset_id)}')
                # logging.debug(f'  Image ID:   {os.path.basename(asset_id)}')

                # CGM: We could pre-compute (or compute once and then save)
                #   the crs, transform, and shape since they should (will?) be
                #   the same for each wrs2 tile
                output_info = utils.get_info(ee.Image(image_id).select([2]))
                transform = '[{}]'.format(
                    ','.join(map(str, output_info['bands'][0]['crs_transform']))
                )

                # Generate sharpened thermal image
                output_img = openet.sharpen.Landsat(image_id).thermal()\
                    .select(['tir_sharpened'], ['tir'])


                # # TODO: Check if model_img_with_metadata is needed?
                # #   This was in the scene export tool
                # # Keep a reference to the original image for copying properties
                # model_img_with_metadata = output_img
                # if destination == 'BUCKET' and properties_json_flag:
                #     # Need to get the source image properties, but calling getInfo
                #     #   on the output image might be really slow
                #     # TODO: Check if this is working for the more complex models
                #     src_info = utils.get_info(model_img_with_metadata)
                #     if not src_info:
                #         logging.info(f'  {scene_id} - Could not get image properties, skipping')
                #         continue


                # CGM - We will need to think this through a little bit more
                #   Should any scale factor be allowed?  Do we need the clamping?
                #   Should/could we support separate scale factors for each band?
                nodata = -9999
                if scale_factor > 1:
                    if output_dtype == 'int16':
                        output_img = output_img.multiply(scale_factor).round()\
                            .clamp(-32768, 32767).int16()
                    elif output_dtype == 'uint16':
                        output_img = output_img.multiply(scale_factor).round()\
                            .clamp(0, 65534).uint16()
                        nodata = 65535
                    # elif output_type == 'int8':
                    #     output_img = output_img.multiply(scale_factor).round()\
                    #         .clamp(-128, 127).int8()
                    # elif output_type == 'uint8':
                    #     output_img = output_img.multiply(scale_factor).round()\
                    #         .clamp(0, 255).uint8()
                    else:
                        output_img = output_img.multiply(scale_factor)

                if clip_ocean_flag:
                    output_img = output_img.updateMask(ee.Image('projects/openet/ocean_mask'))

                properties = {
                    # Custom properties
                    'coll_id': coll_id,
                    'core_version': openet.core.__version__,
                    'date_ingested': datetime.datetime.today().strftime('%Y-%m-%d'),
                    'image_id': image_id,
                    'model_name': model_name,
                    'model_version': openet.sharpen.__version__,
                    # CGM - We will need something like this for models where the version
                    #   number is set in the pyproject.toml and not in the init (i.e. SIMS)
                    # 'model_name': metadata.metadata(model)['Name'],
                    # 'model_version': metadata.metadata(model)['Version'],
                    'scale_factor': 1.0 / scale_factor,
                    'scene_id': scene_id,
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

                # CGM - Is this separate property still needed?
                # Having it named separately from model_version might make
                #   it easier to pass through to other models/calculations
                properties['sharpen_version'] = openet.sharpen.__version__

                # CGM - Tracking the OpenET MODEL version used to build the scene lists
                properties[f'{model.MODEL_NAME.lower()}_version'] = model.__version__
                # properties['sims_version'] = model.__version__

                output_img = output_img.set(properties)

                # Copy metadata properties from original output_img
                # Explicit casting since copyProperties returns an element obj
                # output_img = ee.Image(output_img.copyProperties(landsat_img))


                if destination == 'BUCKET' and 'int' in output_dtype.lower():
                    # CGM - This is needed to get around the bug with integer exports
                    #   where masked pixels are set to 0
                    # Unmasking the image (commented out below) can sometimes create
                    #   a border around the image of 0 pixels even though it works
                    #   correctly when exporting a single Landsat image band
                    nodata_mask = output_img.mask().lte(0)
                    output_img = nodata_mask.multiply(nodata) \
                        .where(nodata_mask.eq(0), output_img) \
                        .rename(output_img.bandNames())
                    # output_img = output_img.unmask(nodata)


                # CGM - Why am I not using the utils.ee_task_start()?
                # Build export tasks
                max_retries = 4
                task = None
                for i in range(1, max_retries+1):
                    try:
                        if destination == 'ASSET':
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
                        elif destination == 'BUCKET':
                            task = ee.batch.Export.image.toCloudStorage(
                                image=output_img,
                                description=export_id,
                                bucket=bucket_name,
                                fileNamePrefix=bucket_img.replace('.tif', ''),
                                dimensions=output_info['bands'][0]['dimensions'],
                                crs=output_info['bands'][0]['crs'],
                                crsTransform=transform,
                                maxPixels=int(1E10),
                                fileFormat='GeoTIFF',
                                formatOptions={'cloudOptimized': True, 'noData': nodata},
                                # pyramidingPolicy='mean',
                            )
                        break
                    # except ee.ee_exception.EEException as e:
                    except Exception as e:
                        if ('Earth Engine memory capacity exceeded' in str(e) or
                                'Earth Engine capacity exceeded' in str(e)):
                            logging.info(f'  Rebuilding task ({i}/{max_retries})')
                            logging.debug(f'  {e}')
                            time.sleep(i ** 3)
                        else:
                            logging.warning(f'Unhandled exception\n{e}')
                            break
                            # raise e

                if not task:
                    logging.warning(f'  {scene_id} - Export task was not built, skipping')
                    continue

                logging.info(f'  {scene_id} - Starting export task')
                for i in range(1, max_retries+1):
                    try:
                        task.start()
                        break
                    except Exception as e:
                        logging.info(f'  Resending query ({i}/{max_retries})')
                        logging.debug(f'  {e}')
                        time.sleep(i ** 3)
                # # Not using ee_task_start since it doesn't return the task object
                # utils.ee_task_start(task)


                # Don't write the properties JSON until after the task has started
                if destination == 'BUCKET' and properties_json_flag:
                    # CGM - Should we check if the json is already in the bucket?
                    # We already checked if the asset exists so at this point it seems
                    #   better to always overwrite the json properties file
                    # if bucket_json not in bucket_files:

                    logging.debug(f'  {scene_id} - Writing properties JSON to bucket')
                    blob = bucket.blob(bucket_json)

                    # # First remove the system index and time_start from the source properties
                    # for k in ['system:index', 'system:time_start']:
                    #     if k in src_info['properties']:
                    #         del src_info['properties'][k]
                    #
                    # # Then add any missing source properties to the properties dictionary
                    # for k, v in src_info['properties'].items():
                    #     if k not in properties:
                    #         properties[k] = v

                    max_retries = 4
                    for i in range(1, max_retries+1):
                        try:
                            blob.upload_from_string(json.dumps(properties))
                            break
                        except Exception as e:
                            logging.info(f'  JSON properties file not written ({i}/{max_retries})')
                            logging.debug(f'  {e}')
                            time.sleep(i ** 3)


                # Write the export task info the openet-dri project datastore
                if log_tasks:
                    # logging.debug('  Writing datastore entity')
                    try:
                        task_obj = datastore.Entity(
                            key=datastore_client.key('Task', task.status()['id']),
                            exclude_from_indexes=['properties']
                        )
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
                        logging.warning(f'\nDatastore entity was not written\n{e}\n')

                # Pause before starting the next export task
                ready_task_count += 1
                ready_task_count = delay_task(
                    delay_time=delay_time, task_max=ready_task_max,
                    task_count=ready_task_count
                )

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
            'TN', 'TX', 'UT', 'VA', 'VT', 'WA', 'WI', 'WV', 'WY']
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
            ee.Filter.inList(study_area_property, study_area_features))

    logging.debug('Building MGRS tile list')
    tiles_coll = ee.FeatureCollection(mgrs_coll_id) \
        .filterBounds(study_area_coll.geometry())

    # Filter collection by user defined lists
    if utm_zones:
        logging.debug(f'  Filter user UTM Zones:    {utm_zones}')
        tiles_coll = tiles_coll.filter(ee.Filter.inList(utm_property, utm_zones))
    if mgrs_skip_list:
        logging.debug(f'  Filter MGRS skip list:    {mgrs_skip_list}')
        tiles_coll = tiles_coll.filter(
            ee.Filter.inList(mgrs_property, mgrs_skip_list).Not())
    if mgrs_tiles:
        logging.debug(f'  Filter MGRS tiles/zones:  {mgrs_tiles}')
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
            'crs': 'EPSG:{:d}'.format(int(tile_ftr['properties']['epsg'])),
            'extent': [int(tile_ftr['properties']['xmin']),
                       int(tile_ftr['properties']['ymin']),
                       int(tile_ftr['properties']['xmax']),
                       int(tile_ftr['properties']['ymax'])],
            'index': tile_ftr['properties']['mgrs'].upper(),
            'wrs2_tiles': sorted(utils.wrs2_str_2_set(
                tile_ftr['properties'][wrs2_property])),
        })

    # Apply the user defined WRS2 tile list
    if wrs2_tiles:
        logging.debug(f'  Filter WRS2 tiles: {wrs2_tiles}')
        for tile in tiles_list:
            tile['wrs2_tiles'] = sorted(list(
                set(tile['wrs2_tiles']) & set(wrs2_tiles)))

    # Only return export tiles that have intersecting WRS2 tiles
    export_list = [
        tile for tile in sorted(tiles_list, key=lambda k: k['index'])
        if tile['wrs2_tiles']
    ]

    return export_list


def date_range_by_year(start_dt, end_dt, exclusive_end_dates=False):
    """

    Parameters
    ----------
    start_dt : datetime
    end_dt : datetime
    exclusive_end_dates : bool, optional
        If True, set the end dates for each iteration range to be exclusive.

    Returns
    -------
    list of start and end datetimes split by year

    """
    if (end_dt - start_dt).days > 366:
        for year in range(start_dt.year, end_dt.year+1):
            year_start_dt = max(datetime.datetime(year, 1, 1), start_dt)
            year_end_dt = datetime.datetime(year+1, 1, 1) - datetime.timedelta(days=1)
            year_end_dt = min(year_end_dt, end_dt)
            if exclusive_end_dates:
                year_end_dt = year_end_dt + datetime.timedelta(days=1)
            yield year_start_dt, year_end_dt
    else:
        if exclusive_end_dates:
            year_end_dt = end_dt + datetime.timedelta(days=1)
        yield start_dt, year_end_dt


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


# CGM - This is a modified copy of openet.utils.delay_task()
#   It was changed to take and return the number of ready tasks
#   This change may eventually be pushed to openet.utils.delay_task()
def delay_task(delay_time=0, task_max=-1, task_count=0):
    """Delay script execution based on number of READY tasks

    Parameters
    ----------
    delay_time : float, int
        Delay time in seconds between starting export tasks or checking the
        number of queued tasks if "ready_task_max" is > 0.  The default is 0.
        The delay time will be set to a minimum of 10 seconds if
        ready_task_max > 0.
    task_max : int, optional
        Maximum number of queued "READY" tasks.
    task_count : int
        The current/previous/assumed number of ready tasks.
        Value will only be updated if greater than or equal to ready_task_max.

    Returns
    -------
    int : ready_task_count

    """
    if task_max > 3000:
        raise ValueError('The maximum number of queued tasks must be less than 3000')

    # Force delay time to be a positive value since the parameter used to
    #   support negative values
    if delay_time < 0:
        delay_time = abs(delay_time)

    if (task_max is None or task_max <= 0) and (delay_time >= 0):
        # Assume task_max was not set and just wait the delay time
        logging.debug(f'  Pausing {delay_time} seconds, not checking task list')
        time.sleep(delay_time)
        return 0
    elif task_max and (task_count < task_max):
        # Skip waiting or checking tasks if a maximum number of tasks was set
        #   and the current task count is below the max
        logging.debug(f'  Ready tasks: {task_count}')
        return task_count

    # If checking tasks, force delay_time to be at least 10 seconds if
    #   ready_task_max is set to avoid excessive EE calls
    delay_time = max(delay_time, 10)

    # Make an initial pause before checking tasks lists to allow
    #   for previous export to start up
    # CGM - I'm not sure what a good default first pause time should be,
    #   but capping it at 30 seconds is probably fine for now
    logging.debug(f'  Pausing {min(delay_time, 30)} seconds for tasks to start')
    time.sleep(delay_time)

    # If checking tasks, don't continue to the next export until the number
    #   of READY tasks is greater than or equal to "ready_task_max"
    while True:
        ready_task_count = len(utils.get_ee_tasks(states=['READY']).keys())
        logging.debug(f'  Ready tasks: {ready_task_count}')
        if ready_task_count >= task_max:
            logging.debug(f'  Pausing {delay_time} seconds')
            time.sleep(delay_time)
        else:
            logging.debug(f'  {task_max - ready_task_count} open task '
                          f'slots, continuing processing')
            break

    return ready_task_count


# TODO: Move to openet-core utils.py
def version_number(version_str):
    return list(map(int, version_str.split('.')))


# TODO: Move to openet-core utils.py
# TODO: Should we pass in the bucket file list instead of checking .exists()
#   or maybe only check .exists() if the bucket_file list is not set?
def delete_cog_asset(asset_id, bucket, bucket_img, bucket_json=None):
    # Always remove the EE asset before deleting the bucket files
    ee.data.deleteAsset(asset_id)

    img_blob = bucket.blob(bucket_img)
    if img_blob.exists():
        img_blob.delete()

    if bucket_json is None:
        bucket_json = bucket_img.replace('.tif', '_properties.json')
    json_blob = bucket.blob(bucket_json)
    if json_blob.exists():
        json_blob.delete()


def arg_parse():
    """"""
    parser = argparse.ArgumentParser(
        description='Export Landsat sharpened thermal images',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument(
        '-i', '--ini', type=utils.arg_valid_file,
        help='Input file', metavar='FILE')
    parser.add_argument(
        '--key', type=utils.arg_valid_file, metavar='FILE',
        help='Earth Engine service account JSON key file')
    parser.add_argument(
        '--recent', default='',
        help='Day range (or number of days) to process before current date '
             '(ignore INI start_date and end_date')
    parser.add_argument(
        '--start', type=utils.arg_valid_date, metavar='DATE', default=None,
        help='Start date (format YYYY-MM-DD)')
    parser.add_argument(
        '--end', type=utils.arg_valid_date, metavar='DATE', default=None,
        help='End date (format YYYY-MM-DD)')
    parser.add_argument(
        '--tiles', default='',
        help='Comma separated list of UTM zones or MGRS tiles to process')
    parser.add_argument(
        '--delay', default=0, type=float,
        help='Delay (in seconds) between each export tasks')
    parser.add_argument(
        '--ready', default=-1, type=int,
        help='Maximum number of queued READY tasks')
    parser.add_argument(
        '--log_tasks', default=False, action='store_true',
        help='Log tasks to the datastore')
    parser.add_argument(
        '--overwrite', default=False, action='store_true',
        help='Force overwrite of existing files')
    parser.add_argument(
        '--reverse', default=False, action='store_true',
        help='Process WRS2 tiles in reverse order')
    parser.add_argument(
        '--update', default=False, action='store_true',
        help='Update images with older model version numbers')
    parser.add_argument(
        '--debug', default=logging.INFO, const=logging.DEBUG,
        help='Debug level logging', action='store_const', dest='loglevel')
    args = parser.parse_args()

    return args


if __name__ == '__main__':
    args = arg_parse()
    logging.basicConfig(level=args.loglevel, format='%(message)s')

    main(ini_path=args.ini, gee_key_file=args.key, recent_days=args.recent,
         start_dt=args.start, end_dt=args.end, tiles=args.tiles,
         delay_time=args.delay, ready_task_max=args.ready,
         log_tasks=args.log_tasks, reverse_flag=args.reverse,
         overwrite_flag=args.overwrite, update_flag=args.update,
    )
