import logging

import ee
import pytest

import openet.lst
import openet.core.utils as utils

logging.basicConfig(level=logging.DEBUG, format='%(message)s')

TEST_IMAGE_ID = 'LANDSAT/LC08/C02/T1_L2/LC08_044033_20170716'
DEFAULT_BANDS = ['blue', 'green', 'red', 'nir', 'swir1', 'swir2', 'lst', 'qa']
DEFAULT_VALUES = [0.1, 0.1, 0.1, 0.3, 0.1, 0.1, 300, 1]


def test_ee_init():
    assert ee.Number(1).getInfo() == 1


def test_Model_init():
    input_img = ee.Image.constant(DEFAULT_VALUES).rename(DEFAULT_BANDS)
    image_obj = openet.lst.Model(image=input_img)
    assert set(image_obj.image.bandNames().getInfo()) == set(DEFAULT_BANDS)


def test_Model_thermal_band_name():
    input_img = openet.lst.Landsat(TEST_IMAGE_ID, c2_lst_correct=False).image
    output = openet.lst.Model(input_img).sharpen().bandNames().getInfo()
    assert output == ['lst_sharpened']


@pytest.mark.parametrize(
    "image_id, xy, expected",
    [
        # Arbitrary test image and locations
        [
            'LANDSAT/LC08/C02/T1_L2/LC08_042034_20180705',
            [-120.10237, 36.946608], 308.6364
        ],
        [
            'LANDSAT/LC09/C02/T1_L2/LC09_044033_20220127',
            [-121.5265, 38.7399], 282.9366
        ],
    ]
)
def test_Model_thermal_point_values(image_id, xy, expected, tol=0.001):
    input_img = openet.lst.Landsat(image_id, c2_lst_correct=False).image
    output_img = openet.lst.Model(input_img).sharpen()
    output = utils.point_image_value(output_img, xy, scale=30)
    assert abs(output['lst_sharpened'] - expected) < tol
