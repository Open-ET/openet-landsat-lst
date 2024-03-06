import logging
import pprint

import ee
import pytest

import openet.lst
import openet.core.utils as utils

logging.basicConfig(level=logging.DEBUG, format='%(message)s')

DEFAULT_BANDS = ['blue', 'green', 'red', 'nir', 'swir1', 'swir2', 'lst', 'qa']



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
    output = openet.lst.Landsat_C02_L2(image_id).image.bandNames().getInfo()
    assert set(output) == set(DEFAULT_BANDS)


def test_Landsat_C02_L2_properties():
    image_id = 'LANDSAT/LC08/C02/T1_L2/LC08_044033_20170716'
    output = openet.lst.Landsat_C02_L2(image_id=image_id).image.getInfo()
    assert output['properties']['SPACECRAFT_ID'] == 'LANDSAT_8'


def test_Landsat_C02_L2_scaling():
    image_id = 'LANDSAT/LC08/C02/T1_L2/LC08_044033_20170716'
    output = utils.point_image_value(
        openet.lst.Landsat_C02_L2(image_id).image, xy=(-121.5265, 38.7399))
    assert abs(output['nir'] - 0.26) <= 0.01
    assert abs(output['lst'] - 306) <= 1


@pytest.mark.parametrize(
    'image_id',
    [
        'LANDSAT/LC08/C02/T1_L2/LC08_044033_20170716',
        'LANDSAT/LC09/C02/T1_L2/LC09_044033_20220127',
    ]
)
def test_Landsat_band_names(image_id):
    output = openet.lst.Landsat(image_id).image.bandNames().getInfo()
    assert set(output) == set(DEFAULT_BANDS)


@pytest.mark.parametrize(
    "image_id, xy, expected",
    [
        # First two points are in the same field but the second one is in an
        # area of bad emissivity data and needs correction
        ['LANDSAT/LC08/C02/T1_L2/LC08_030036_20210725', [-102.266679, 34.368470], 310],
        ['LANDSAT/LC08/C02/T1_L2/LC08_030036_20210725', [-102.266754, 34.367682], 310],
        # This point is just outside the field and should stay the same
        ['LANDSAT/LC08/C02/T1_L2/LC08_030036_20210725', [-102.269769, 34.366115], 318],
        # These two points are in the ASTER GED hole and have no L2 temperature
        # The first is a high NDVI field, the second is a low NDVI field
        ['LANDSAT/LC08/C02/T1_L2/LC08_031034_20160702', [-102.08284, 37.81728], 307],
        ['LANDSAT/LC08/C02/T1_L2/LC08_031034_20160702', [-102.04696, 37.81796], 298],
    ]
)
def test_Landsat_c2_lst_correction_flag(image_id, xy, expected, tol=1):
    output_img = openet.lst.Landsat(image_id, c2_lst_correct=True).image
    corrected = utils.point_image_value(output_img, xy, scale=30)
    assert abs(corrected['lst'] - expected) <= tol
