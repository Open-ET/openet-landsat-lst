import re

import ee

from .model import Model


class Landsat(object):
    # CGM - Using the __new__ to return is discouraged and is probably not
    #   great Python but it was the only way I could find to make the general
    #   Landsat class directly callable like the collection specific ones
    # def __init__(self):
    #     """"""
    #     pass

    def __new__(cls, image_id):
        if type(image_id) is not str:
            raise ValueError('unsupported input type')
        elif re.match('LANDSAT/L[TEC]0[4578]/C02/T1_L2', image_id):
            return Landsat_C02_SR(image_id)
        elif re.match('LANDSAT/L[TEC]0[4578]/C01/T1_SR', image_id):
            return Landsat_C01_SR(image_id)
        elif re.match('LANDSAT/L[TEC]0[4578]/C01/T1_TOA', image_id):
            return Landsat_C01_TOA(image_id)
        else:
            raise ValueError('unsupported image_id')


class Landsat_C02_SR(Model):
    def __init__(self, image_id):
        """"""
        # TODO: Support input being an ee.Image
        # For now assume input is always an image ID
        if type(image_id) is not str:
            raise ValueError('unsupported input type')
        elif (image_id.startswith('LANDSAT/') and
                not re.match('LANDSAT/L[TEC]0[4578]/C02/T1_L2', image_id)):
            raise ValueError('unsupported collection ID')
        raw_image = ee.Image(image_id)

        # CGM - Testing out not setting any self. parameters and passing inputs
        #   to the super().__init__() call instead

        spacecraft_id = ee.String(raw_image.get('SPACECRAFT_ID'))

        # CGM - The QA_PIXEL band could probably be dropped
        input_bands = ee.Dictionary({
            'LANDSAT_4': ['SR_B1', 'SR_B2', 'SR_B3', 'SR_B4', 'SR_B5', 'SR_B7',
                          'ST_B6', 'QA_PIXEL'],
            'LANDSAT_5': ['SR_B1', 'SR_B2', 'SR_B3', 'SR_B4', 'SR_B5', 'SR_B7',
                          'ST_B6', 'QA_PIXEL'],
            'LANDSAT_7': ['SR_B1', 'SR_B2', 'SR_B3', 'SR_B4', 'SR_B5', 'SR_B7',
                          'ST_B6', 'QA_PIXEL'],
            'LANDSAT_8': ['SR_B2', 'SR_B3', 'SR_B4', 'SR_B5', 'SR_B6', 'SR_B7',
                          'ST_B10', 'QA_PIXEL'],
        })
        output_bands = [
            'blue', 'green', 'red', 'nir', 'swir1', 'swir2', 'tir', 'pixel_qa']

        # # Cloud mask function must be passed with raw/unnamed image
        # cloud_mask = openet.core.common.landsat_c2_sr_cloud_mask(
        #     raw_image, **cloudmask_args)

        input_image = raw_image \
            .select(input_bands.get(spacecraft_id), output_bands)\
            .multiply([0.0000275, 0.0000275, 0.0000275, 0.0000275,
                       0.0000275, 0.0000275, 0.00341802, 1])\
            .add([-0.2, -0.2, -0.2, -0.2, -0.2, -0.2, 149.0, 1])\
            .set({'system:time_start': raw_image.get('system:time_start'),
                  'system:index': raw_image.get('system:index'),
                  'SATELLITE': spacecraft_id,
                  })

        # CGM - super could be called without the init if we set input_image and
        #   spacecraft_id as properties of self
        super().__init__(input_image)
        # super()


class Landsat_C01_SR(Model):
    def __init__(self, image_id):
        """"""
        if type(image_id) is not str:
            raise ValueError('unsupported input type')
        elif (image_id.startswith('LANDSAT/') and
                not re.match('LANDSAT/L[TEC]0[4578]/C01/T1_SR', image_id)):
            raise ValueError('unsupported collection ID')
        raw_image = ee.Image(image_id)

        # The SATELLITE property in this collection is equivalent to SPACECRAFT_ID
        spacecraft_id = ee.String(raw_image.get('SATELLITE'))

        input_bands = ee.Dictionary({
            'LANDSAT_4': ['B1', 'B2', 'B3', 'B4', 'B5', 'B7', 'B6', 'pixel_qa'],
            'LANDSAT_5': ['B1', 'B2', 'B3', 'B4', 'B5', 'B7', 'B6', 'pixel_qa'],
            'LANDSAT_7': ['B1', 'B2', 'B3', 'B4', 'B5', 'B7', 'B6', 'pixel_qa'],
            'LANDSAT_8': ['B2', 'B3', 'B4', 'B5', 'B6', 'B7', 'B10', 'pixel_qa']})
        output_bands = [
            'blue', 'green', 'red', 'nir', 'swir1', 'swir2', 'tir', 'pixel_qa']

        # # Cloud mask function must be passed with raw/unnamed image
        # cloud_mask = openet.core.common.landsat_c1_sr_cloud_mask(
        #     raw_image, **cloudmask_args)

        input_image = raw_image \
            .select(input_bands.get(spacecraft_id), output_bands)\
            .multiply([0.0001, 0.0001, 0.0001, 0.0001, 0.0001, 0.0001, 0.1, 1])\
            .set({'system:time_start': raw_image.get('system:time_start'),
                  'system:index': raw_image.get('system:index'),
                  'SATELLITE': spacecraft_id,
                  })
        #     .updateMask(cloud_mask)

        super().__init__(input_image)


class Landsat_C01_TOA(Model):
    def __init__(self, image_id):
        """"""
        # TODO: Support input being an ee.Image
        # For now assume input is always an image ID
        if type(image_id) is not str:
            raise ValueError('unsupported input type')
        elif (image_id.startswith('LANDSAT/') and
                not re.match('LANDSAT/L[TEC]0[4578]/C01/T1_TOA', image_id)):
            raise ValueError('unsupported collection ID')
        raw_image = ee.Image(image_id)

        # Use the SPACECRAFT_ID property identify each Landsat type
        spacecraft_id = ee.String(raw_image.get('SPACECRAFT_ID'))

        input_bands = ee.Dictionary({
            'LANDSAT_4': ['B1', 'B2', 'B3', 'B4', 'B5', 'B7', 'B6', 'BQA'],
            'LANDSAT_5': ['B1', 'B2', 'B3', 'B4', 'B5', 'B7', 'B6', 'BQA'],
            'LANDSAT_7': ['B1', 'B2', 'B3', 'B4', 'B5', 'B7', 'B6_VCID_1', 'BQA'],
            'LANDSAT_8': ['B2', 'B3', 'B4', 'B5', 'B6', 'B7', 'B10', 'BQA']})
        output_bands = [
            'blue', 'green', 'red', 'nir', 'swir1', 'swir2', 'tir', 'pixel_qa']

        # # Cloud mask function must be passed with raw/unnamed image
        # cloud_mask = openet.core.common.landsat_c1_toa_cloud_mask(
        #     raw_image, **cloudmask_args)

        input_image = raw_image \
            .select(input_bands.get(spacecraft_id), output_bands)\
            .set({'system:time_start': raw_image.get('system:time_start'),
                  'system:index': raw_image.get('system:index'),
                  'SATELLITE': spacecraft_id,
                  })

        super().__init__(input_image)