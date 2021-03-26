import logging
import pprint

import ee
import pytest

import openet.sharpen
import openet.core.utils as utils

logging.basicConfig(level=logging.DEBUG, format='%(message)s')

TEST_IMAGE_ID = 'LANDSAT/LC08/C01/T1_SR/LC08_044033_20170716'
DEFAULT_BANDS = ['blue', 'green', 'red', 'nir', 'swir1', 'swir2', 'tir', 'pixel_qa']
DEFAULT_VALUES = [0.1, 0.1, 0.1, 0.3, 0.1, 0.1, 300, 1]


def test_ee_init():
    assert ee.Number(1).getInfo() == 1


def test_Model_init():
    input_img = ee.Image.constant(DEFAULT_VALUES).rename(DEFAULT_BANDS)
    image_obj = openet.sharpen.Model(image=input_img)
    assert set(image_obj.image.bandNames().getInfo()) == set(DEFAULT_BANDS)


def test_Model_thermal_band_name():
    input_img = openet.sharpen.Landsat(TEST_IMAGE_ID).image
    output = openet.sharpen.Model(input_img).thermal().bandNames().getInfo()
    assert output == ['tir_sharpened']


@pytest.mark.parametrize(
    "image_id, xy, expected",
    [
        ['LANDSAT/LC08/C01/T1_SR/LC08_042034_20180705',
         [-120.10237, 36.946608], 304.13262575660264],
        ['LANDSAT/LC08/C01/T1_TOA/LC08_042034_20180705',
         [-120.10237, 36.946608], 303.75037059315696],
    ]
)
def test_Model_thermal_point_values(image_id, xy, expected, tol=0.01):
    input_img = openet.sharpen.Landsat(image_id).image
    output_img = openet.sharpen.Model(input_img).thermal()
    output = utils.point_image_value(output_img, xy, scale=30)
    assert abs(output['tir_sharpened'] - expected) < tol