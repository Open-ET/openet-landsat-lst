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
def test_Landsat_C02_SR_band_names(image_id):
    output = openet.sharpen.Landsat_C02_SR(image_id).image.bandNames().getInfo()
    assert set(output) == set(DEFAULT_BANDS)


# CGM - Eventually we should switch and use SPACECRAFT_ID instead of SATELLITE
def test_Landsat_C02_SR_properties():
    image_id = 'LANDSAT/LC08/C02/T1_L2/LC08_044033_20170716'
    output = openet.sharpen.Landsat_C02_SR(image_id=image_id).image.getInfo()
    assert output['properties']['SATELLITE'] == 'LANDSAT_8'


def test_Landsat_C02_SR_scaling():
    image_id = 'LANDSAT/LC08/C02/T1_L2/LC08_044033_20170716'
    output = utils.point_image_value(
        openet.sharpen.Landsat_C02_SR(image_id).image, xy=(-121.5265, 38.7399))
    assert abs(output['nir'] - 0.26) <= 0.01
    assert abs(output['tir'] - 306) <= 1


@pytest.mark.parametrize(
    'image_id',
    [
        'LANDSAT/LT04/C01/T1_SR/LT04_044033_19830812',
        'LANDSAT/LT05/C01/T1_SR/LT05_044033_20110716',
        'LANDSAT/LE07/C01/T1_SR/LE07_044033_20170724',
        'LANDSAT/LC08/C01/T1_SR/LC08_044033_20170716',
    ]
)
def test_Landsat_C01_SR_band_names(image_id):
    output = openet.sharpen.Landsat_C01_SR(image_id).image.bandNames().getInfo()
    assert set(output) == set(DEFAULT_BANDS)


# CGM - Eventually we should switch and use SPACECRAFT_ID instead of SATELLITE
def test_Landsat_C01_SR_properties():
    image_id = 'LANDSAT/LC08/C01/T1_SR/LC08_044033_20170716'
    output = openet.sharpen.Landsat_C01_SR(image_id=image_id).image.getInfo()
    assert output['properties']['SATELLITE'] == 'LANDSAT_8'


def test_Landsat_C01_SR_scaling():
    image_id = 'LANDSAT/LC08/C01/T1_SR/LC08_044033_20170716'
    output = utils.point_image_value(
        openet.sharpen.Landsat_C01_SR(image_id).image, xy=(-121.5265, 38.7399))
    assert abs(output['nir'] - 0.29) <= 0.01
    assert abs(output['tir'] - 301.5) <= 1


@pytest.mark.parametrize(
    'image_id',
    [
        'LANDSAT/LT04/C01/T1_TOA/LT04_044033_19830812',
        'LANDSAT/LT05/C01/T1_TOA/LT05_044033_20110716',
        'LANDSAT/LE07/C01/T1_TOA/LE07_044033_20170724',
        'LANDSAT/LC08/C01/T1_TOA/LC08_044033_20170716',
    ]
)
def test_Landsat_C01_TOA_band_names(image_id):
    output = openet.sharpen.Landsat_C01_TOA(image_id).image.bandNames().getInfo()
    assert set(output) == set(DEFAULT_BANDS)


# CGM - Eventually we should switch and use SPACECRAFT_ID instead of SATELLITE
def test_Landsat_C01_TOA_properties():
    image_id = 'LANDSAT/LC08/C01/T1_TOA/LC08_044033_20170716'
    output = openet.sharpen.Landsat_C01_TOA(image_id=image_id).image.getInfo()
    assert output['properties']['SATELLITE'] == 'LANDSAT_8'


def test_Landsat_C01_TOA_scaling():
    image_id = 'LANDSAT/LC08/C01/T1_TOA/LC08_044033_20170716'
    output = utils.point_image_value(
        openet.sharpen.Landsat_C01_TOA(image_id).image, xy=(-121.5265, 38.7399))
    assert abs(output['nir'] - 0.29) <= 0.01
    assert abs(output['tir'] - 301.5) <= 1


@pytest.mark.parametrize(
    'image_id',
    [
        'LANDSAT/LC08/C01/T1_SR/LC08_044033_20170716',
        'LANDSAT/LC08/C01/T1_TOA/LC08_044033_20170716',
        'LANDSAT/LC08/C02/T1_L2/LC08_044033_20170716',
    ]
)
def test_Landsat_band_names(image_id):
    output = openet.sharpen.Landsat(image_id).image.bandNames().getInfo()
    assert set(output) == set(DEFAULT_BANDS)
