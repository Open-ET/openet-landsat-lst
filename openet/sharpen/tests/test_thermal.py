# import datetime
import logging
import pprint

import ee
# import pytest

import openet.sharpen

logging.basicConfig(level=logging.DEBUG, format='%(message)s')

# Low Almond site
TEST_POINT = [-120.10237, 36.946608]
# # Low almond Landsat cell center coordinates in EPSG:32611
# TEST_POINT = [223740.0, 4093440.0]
# # Test point used in some of the other OpenET modules
# TEST_POINT = [-121.5265, 38.7399]


def test_ee_init():
    assert ee.Number(1).getInfo() == 1


def test_sharpen_thermal_landsat_sr():
    input_img = ee.Image('LANDSAT/LC08/C01/T1_SR/LC08_042034_20180705')
    # input_img = ee.Image('LANDSAT/LC08/C01/T1_SR/LC08_044033_20170716')

    # Copied from PTJPL Image.from_landsat_c1_sr()
    input_bands = ee.Dictionary({
        'LANDSAT_5': ['B1', 'B2', 'B3', 'B4', 'B5', 'B7', 'B6', 'pixel_qa'],
        'LANDSAT_7': ['B1', 'B2', 'B3', 'B4', 'B5', 'B7', 'B6', 'pixel_qa'],
        'LANDSAT_8': ['B2', 'B3', 'B4', 'B5', 'B6', 'B7', 'B10', 'pixel_qa']})
    output_bands = ['blue', 'green', 'red', 'nir', 'swir1', 'swir2', 'tir',
                    'pixel_qa']
    spacecraft_id = ee.String(input_img.get('SATELLITE'))
    prep_image = input_img \
        .select(input_bands.get(spacecraft_id), output_bands) \
        .set({'SATELLITE': spacecraft_id})
    # CGM - Don't unscale SR images to reflectance yet
    #     .multiply([0.0001, 0.0001, 0.0001, 0.0001, 0.0001, 0.0001, 0.1, 1]) \

    output_img = openet.sharpen.thermal.thermalSharpening(prep_image) \
        .select(['tir_sharpened'])

    assert openet.sharpen.utils.point_image_value(
        output_img, TEST_POINT, scale=30)['tir_sharpened'] == 3035


# def test_sharpen_thermal_landsat_toa():
#     input_img = ee.Image('LANDSAT/LC08/C01/T1_TOA/LC08_042034_20180705')
#     # input_img = ee.Image('LANDSAT/LC08/C01/T1_TOA/LC08_044033_20170716')
#
#     # Copied from PTJPL Image.from_landsat_c1_sr()
#     input_bands = ee.Dictionary({
#         'LANDSAT_5': ['B1', 'B2', 'B3', 'B4', 'B5', 'B7', 'B6', 'BQA'],
#         'LANDSAT_7': ['B1', 'B2', 'B3', 'B4', 'B5', 'B7', 'B6', 'BQA'],
#         'LANDSAT_8': ['B2', 'B3', 'B4', 'B5', 'B6', 'B7', 'B10', 'BQA']})
#     output_bands = ['blue', 'green', 'red', 'nir', 'swir1', 'swir2', 'tir',
#                     'BQA']
#     spacecraft_id = ee.String(input_img.get('SPACECRAFT_ID'))
#     prep_image = input_img \
#         .select(input_bands.get(spacecraft_id), output_bands) \
#         .set({'SATELLITE': spacecraft_id})
#
#     output_img = openet.sharpen.thermal.thermalSharpening(prep_image) \
#         .select(['tir_sharpened'])
#
#     assert openet.sharpen.utils.point_image_value(
#         output_img, TEST_POINT, scale=30)['tir_sharpened'] == 3035
