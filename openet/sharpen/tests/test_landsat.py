import logging
import pprint

import ee
import pytest

import openet.sharpen
import openet.core.utils as utils

logging.basicConfig(level=logging.DEBUG, format='%(message)s')

DEFAULT_BANDS = ['blue', 'green', 'red', 'nir', 'swir1', 'swir2', 'tir', 'pixel_qa']



def test_ee_init():
    assert ee.Number(1).getInfo() == 1


@pytest.mark.parametrize(
    'image_id',
    [
        'LANDSAT/LT04/C02/T1_L2/LT04_044033_19830812',
        'LANDSAT/LT05/C02/T1_L2/LT05_044033_20110716',
        'LANDSAT/LE07/C02/T1_L2/LE07_044033_20170708',
        'LANDSAT/LC08/C02/T1_L2/LC08_044033_20170716',
        'LANDSAT/LC09/C02/T1_L2/LC09_044033_20220127',
    ]
)
def test_Landsat_C02_L2_band_names(image_id):
    output = openet.sharpen.Landsat_C02_L2(image_id).image.bandNames().getInfo()
    assert set(output) == set(DEFAULT_BANDS)


def test_Landsat_C02_L2_properties():
    image_id = 'LANDSAT/LC08/C02/T1_L2/LC08_044033_20170716'
    output = openet.sharpen.Landsat_C02_L2(image_id=image_id).image.getInfo()
    assert output['properties']['SATELLITE'] == 'LANDSAT_8'


def test_Landsat_C02_L2_scaling():
    image_id = 'LANDSAT/LC08/C02/T1_L2/LC08_044033_20170716'
    output = utils.point_image_value(
        openet.sharpen.Landsat_C02_L2(image_id).image, xy=(-121.5265, 38.7399))
    assert abs(output['nir'] - 0.26) <= 0.01
    assert abs(output['tir'] - 306) <= 1


@pytest.mark.parametrize(
    'image_id',
    [
        'LANDSAT/LC08/C02/T1_L2/LC08_044033_20170716',
        'LANDSAT/LC09/C02/T1_L2/LC09_044033_20220127',
    ]
)
def test_Landsat_band_names(image_id):
    output = openet.sharpen.Landsat(image_id).image.bandNames().getInfo()
    assert set(output) == set(DEFAULT_BANDS)
