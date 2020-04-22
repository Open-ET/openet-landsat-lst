import logging
import pprint

import ee
# import pytest

import openet.sharpen.thermal
import openet.core.utils as utils

logging.basicConfig(level=logging.DEBUG, format='%(message)s')

# Low Almond site
# TEST_POINT = [-120.10237, 36.946608]
# # Low almond Landsat cell center coordinates in EPSG:32611
# TEST_POINT = [223740.0, 4093440.0]
# # Test point used in some of the other OpenET modules
# TEST_POINT = [-121.5265, 38.7399]



def test_ee_init():
    assert ee.Number(1).getInfo() == 1


def test_sharpen_thermal_landsat_sr(tol=0.01):
    TEST_POINT = [-120.10237, 36.946608]
    expected = 303.566
    input_img = ee.Image('LANDSAT/LC08/C01/T1_SR/LC08_042034_20180705')
    # input_img = ee.Image('LANDSAT/LC08/C01/T1_SR/LC08_044033_20170716')
    spacecraft_id = ee.String(input_img.get('SATELLITE'))
    landsat_sr_bands = ee.Dictionary({
        'LANDSAT_5': ['B1', 'B2', 'B3', 'B4', 'B5', 'B7', 'B6', 'pixel_qa'],
        'LANDSAT_7': ['B1', 'B2', 'B3', 'B4', 'B5', 'B7', 'B6', 'pixel_qa'],
        'LANDSAT_8': ['B2', 'B3', 'B4', 'B5', 'B6', 'B7', 'B10', 'pixel_qa']})
    prep_sr_bands = ['blue', 'green', 'red', 'nir', 'swir1', 'swir2', 'tir', 'pixel_qa']
    prep_image = input_img \
        .select(landsat_sr_bands.get(spacecraft_id), prep_sr_bands) \
        .multiply([0.0001, 0.0001, 0.0001, 0.0001, 0.0001, 0.0001, 0.1, 1]) \
        .set({'SATELLITE': spacecraft_id})
    output_img = openet.sharpen.thermal.landsat(prep_image) \
        .select(['tir_sharpened'])
    output = utils.point_image_value(
        output_img, TEST_POINT, scale=30)['tir_sharpened']
    assert abs(output - expected) < tol


def test_sharpen_thermal_landsat_sr_no_scaling(tol=0.01):
    """Check that an unscaled Landsat SR image can be sharpened"""
    TEST_POINT = [-120.10237, 36.946608]
    expected = 3035.56
    input_img = ee.Image('LANDSAT/LC08/C01/T1_SR/LC08_042034_20180705')
    # input_img = ee.Image('LANDSAT/LC08/C01/T1_SR/LC08_044033_20170716')
    spacecraft_id = ee.String(input_img.get('SATELLITE'))
    landsat_sr_bands = ee.Dictionary({
        'LANDSAT_5': ['B1', 'B2', 'B3', 'B4', 'B5', 'B7', 'B6', 'pixel_qa'],
        'LANDSAT_7': ['B1', 'B2', 'B3', 'B4', 'B5', 'B7', 'B6', 'pixel_qa'],
        'LANDSAT_8': ['B2', 'B3', 'B4', 'B5', 'B6', 'B7', 'B10', 'pixel_qa']})
    prep_sr_bands = ['blue', 'green', 'red', 'nir', 'swir1', 'swir2', 'tir', 'pixel_qa']
    prep_image = input_img \
        .select(landsat_sr_bands.get(spacecraft_id), prep_sr_bands) \
        .set({'SATELLITE': spacecraft_id})
    output_img = openet.sharpen.thermal.landsat(prep_image) \
        .select(['tir_sharpened'])
    output = utils.point_image_value(
        output_img, TEST_POINT, scale=30)['tir_sharpened']
    assert abs(output - expected) < tol


def test_sharpen_thermal_landsat_toa(tol=0.01):
    TEST_POINT = [-120.10237, 36.946608]
    expected = 303.465
    input_img = ee.Image('LANDSAT/LC08/C01/T1_TOA/LC08_042034_20180705')
    # input_img = ee.Image('LANDSAT/LC08/C01/T1_TOA/LC08_044033_20170716')
    spacecraft_id = ee.String(input_img.get('SPACECRAFT_ID'))
    landsat_toa_bands = ee.Dictionary({
        'LANDSAT_5': ['B1', 'B2', 'B3', 'B4', 'B5', 'B7', 'B6', 'BQA'],
        'LANDSAT_7': ['B1', 'B2', 'B3', 'B4', 'B5', 'B7', 'B6', 'BQA'],
        'LANDSAT_8': ['B2', 'B3', 'B4', 'B5', 'B6', 'B7', 'B10', 'BQA']})
    prep_toa_bands = ['blue', 'green', 'red', 'nir', 'swir1', 'swir2', 'tir', 'BQA']
    prep_image = input_img \
        .select(landsat_toa_bands.get(spacecraft_id), prep_toa_bands) \
        .set({'SATELLITE': spacecraft_id})
    output_img = openet.sharpen.thermal.landsat(prep_image) \
        .select(['tir_sharpened'])
    output = utils.point_image_value(
        output_img, TEST_POINT, scale=30)['tir_sharpened']
    assert abs(output - expected) < tol
